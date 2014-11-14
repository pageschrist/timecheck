/**************************************************************************\
*    Filename: timecheck.c
*      Author:
* Description: Time synchronisation test utility.
*   Copyright: (C) 2011 Solarflare Communications Inc.
*
* This program is free software; you can redistribute it and/or modify it
* under the terms of the GNU General Public License version 2 as published
* by the Free Software Foundation, incorporated herein by reference.
\**************************************************************************/

/*
 *  Most errors are considered fatal and the program will exit at once.
 */

/*
 * @todo:
 *  Add functions to post process results.
 */

/*----------------------------------------------------------------------*/
/* Header files                                                         */
/*----------------------------------------------------------------------*/

#include <assert.h>
#include <getopt.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/socket.h>
#include <net/if.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>
#include <time.h>
#include <stdarg.h>
#include <pthread.h>
#include <errno.h>

/*----------------------------------------------------------------------*/
/* Defines and structure definitions                                    */
/*----------------------------------------------------------------------*/

#define MAX_CLIENTS             (10)

#define TIMECHECK_SEND_PORT     (41234)
#define TIMECHECK_RECEIVE_PORT  (41235)
#define TIMECHECK_ADDRESS       0xe1010203  /* 225.1.2.3 */

#define LOOP_TIME           (2)
#define NUMBER_ITERATIONS   (30)

/* How accurate the system clock should be */
#define DESIRED_CLOCK_RESOLUTION    (200)

#define NSEC_IN_SEC                 (1000000000UL)

/**
 *  Data as seen on the wire (all elements four bytes long)
 */
#define PACKET_FLAGS_OFFSET                 (0)
#define PACKET_SEQUENCE_OFFSET              (PACKET_FLAGS_OFFSET + 4)
#define PACKET_SECONDS_OFFSET               (PACKET_SEQUENCE_OFFSET + 4)
#define PACKET_NANOSECONDS_OFFSET           (PACKET_SECONDS_OFFSET + 4)
#define PACKET_CONTROL_IP_OFFSET            (PACKET_NANOSECONDS_OFFSET + 4)
#define PACKET_CONTROL_PORT_OFFSET          (PACKET_CONTROL_IP_OFFSET + 4)
#define PACKET_SLAVE_IP_OFFSET              (PACKET_CONTROL_PORT_OFFSET + 4)
#define PACKET_SLAVE_SECONDS_OFFSET         (PACKET_SLAVE_IP_OFFSET + 4)
#define PACKET_SLAVE_NANOSECONDS_OFFSET     (PACKET_SLAVE_SECONDS_OFFSET + 4)

#define PACKET_LENGTH                       (PACKET_SLAVE_NANOSECONDS_OFFSET + 4)

#define PACKET_FLAG_DONE    (1UL << 0)

struct data_packet_s;

/* Results */
typedef struct data_packet_s
{
    struct data_packet_s    *next;
    uint32_t    flags;              /**< Control */
    uint32_t    sequence;           /**< Sequence number */
    uint32_t    seconds;            /**< Time of sending */
    uint32_t    nanoseconds;
    in_addr_t   control_ip;         /**< Address of multicast transmitter */
    in_port_t   control_port;       /**< Port to which to reply */

    in_addr_t   slave_ip;           /**< Address of receiver */
    uint32_t    slave_seconds;      /**< Time seen by receiver */
    uint32_t    slave_nanoseconds;
    uint32_t    recv_seconds;      /**< Time seen at the arrival */
    uint32_t    recv_nanoseconds;
} data_packet_t;

typedef struct client_s
{
    int             fd;         /* Reader socket, -1 if the client stopped */
    in_addr_t       address;    /* Client's address */
    data_packet_t   *head;      /* First packet received */
    data_packet_t   *tail;      /* Last packet received */
} client_t;

/*----------------------------------------------------------------------*/
/* Forward declarations of functions in this module                     */
/*----------------------------------------------------------------------*/

static void parse_options (int argc, char *argv []);
static void check_clock_resolution (void);
static void do_slave (void);
static void do_control (void);
static int configure_control (void);
static int configure_slave (void);
static int open_control_reply (in_port_t port);
static void *control_reply_reader (void *arg);
static void add_client (int listen_fd);
static void process_client (int client_id);
static void close_client (int client_id, char *message);
static int open_slave_reply (in_addr_t address, in_port_t port);
static void write_uint32 (uint8_t *buf, unsigned offset, uint32_t data);
static uint32_t read_uint32 (uint8_t *buf, unsigned offset);
static int read_packet (data_packet_t *packet, uint8_t *buf, unsigned length);
static int process_packet (data_packet_t *packet, bool short_form);
static void runtime_error (char *text);
static void fatal_error (const char *fmt, ...);
static void info (const char *fmt, ...);
static char *ipaddr_str (in_addr_t address);
static void add_pending_packet (data_packet_t *packet);
static void display_pending_packets (void);
static int compare_packet (const void *first, const void *second);

/*----------------------------------------------------------------------*/
/* Variables                                                            */
/*----------------------------------------------------------------------*/

/** Running as control if true else slave */
static bool control = false;

/* Communication addresses */
static in_port_t listen_port = TIMECHECK_SEND_PORT;
static in_port_t reply_port = TIMECHECK_RECEIVE_PORT;
static in_addr_t multicast_address = TIMECHECK_ADDRESS;
static in_addr_t interface_address;

static unsigned int cycle_delay = LOOP_TIME;
static unsigned int number_loops = NUMBER_ITERATIONS;
static bool control_running = true;
static bool unsorted = false;
static bool oneline = false;

/**
 *  The number of clients seen during the run: some clients may
 *  stop during the run.
 */
static int      num_clients = 0;
static client_t clients [MAX_CLIENTS];

/**
 *
 */
static data_packet_t    *pending_packets [MAX_CLIENTS];
static int              pending_index = 0;

static bool verbose = false;

/** Options available to the program */
static struct option long_options [] =
{
    {"control",     0,  0,  'c'},
    {"slave",       0,  0,  's'},
    {"port",        1,  0,  'p'},
    {"address",     1,  0,  'a'},
    {"loops",       1,  0,  'l'},
    {"delay",       1,  0,  'd'},
    {"replyport",   1,  0,  'r'},
    {"unsorted",    0,  0,  'u'},
    {"oneline",     0,  0,  'o'},
    {"verbose",     0,  0,  'v'},
    {0,             0,  0,  0}
};

static char usage [] =
{
    "Usage: timecheck -c -s -p port -a address -l loops -d delay -u -r replyport -o {interface_ip}\n"
    "-c             [--control] control\n"
    "-s             [--slave] slave\n"
    "-p port        [--port] network port for control send\n"
    "-a address     [--address] network address must be multicast\n"
    "-l loops       [--loops] number of iterations (control)\n"
    "-d delay       [--delay] delay between iterations (control, seconds)\n"
    "-r replyport   [--replyport] network port for control receive\n"
    "-u             [--unsorted] don't sort output by IP address\n"
    "-o             [--oneline] one client per line (only applicable for sorted output)\n"
    "interface_ip   IP address over which multicast to be transferred\n"
};

/*----------------------------------------------------------------------*/
/* Implementation                                                       */
/*----------------------------------------------------------------------*/

/**
 *  Main entry point
 */
int main (int argc, char *argv [])
{
    parse_options (argc, argv);
    check_clock_resolution ();

    if (control)
    {
        info ("Starting control\n");
        do_control ();
    }
    else
    {
        info ("Starting slave\n");
        do_slave ();
    }

    return  0;

}

/**
 *  Process any supplied options
 *
 */
static void parse_options (int argc, char *argv [])
{
    struct in_addr  address;
    int             c = 0;

    while (c >= 0)
    {
        int option_index = 0;

        c = getopt_long (argc, argv, "csp:a:l:d:r:ouv", long_options, &option_index);
        switch (c)
        {
        case -1:
            break;
        case 'c':
            control = true;
            break;
        case 's':
            control = false;
            break;
        case 'p':
            listen_port = atoi (optarg);
            if (0 == listen_port)
            {
                fatal_error ("Bad send port '%s'\n", optarg);
            }
            break;
        case 'r':
            reply_port = atoi (optarg);
            if (0 == reply_port)
            {
                fatal_error ("Bad receive port '%s'\n", optarg);
            }
            break;
        case 'a':
            if (inet_aton (optarg, &address))
            {
                multicast_address = ntohl(address.s_addr);
                if (!IN_MULTICAST (multicast_address))
                {
                    fatal_error ("%s is not a multicast address\n", ipaddr_str(multicast_address));
                }
            }
            else
            {
                fatal_error ("Bad multicast IP address '%s'\n", optarg);
            }
            break;
        case 'l':
            number_loops = atoi (optarg);
            if (0 == number_loops)
            {
                fatal_error ("Bad loop count'%s'\n", optarg);
            }
            break;
        case 'd':
            cycle_delay = atoi (optarg);
            if (0 == cycle_delay)
            {
                fatal_error ("Bad iteration delay '%s'\n", optarg);
            }
            break;
        case 'u':
            unsorted = true;
            break;
        case 'o':
            oneline = true;
            break;
        case 'v':
            verbose = true;
            break;
        case '?':
        default:
            fatal_error ("%s", usage);
            break;
        }
    }
    if ((optind + 1) != argc)
    {
        fatal_error ("No interface address\n\n%s", usage);
    }
    else if (0 == inet_aton (argv [optind], &address))
    {
        fatal_error ("Bad interface IP address '%s'\n", argv [optind]);
    }
    else
    {
        interface_address = ntohl(address.s_addr);
    }

    if (unsorted)
    {
        oneline = true;
    }
}

/**
 *  Check that the clock resolution available is appropriate
 */
static void check_clock_resolution (void)
{
      struct timespec  t1;

      clock_getres (CLOCK_REALTIME, &t1);
      if (t1.tv_nsec > DESIRED_CLOCK_RESOLUTION)
      {
          printf ("Warning: Clock resolution is only %ldns (wanting %ldns)\n", t1.tv_nsec, DESIRED_CLOCK_RESOLUTION);
      }
}

/**
 *  Run slave side operations
 *
 */
static void do_slave (void)
{
    int fd = configure_slave ();

    setbuf (stdout, NULL);

    if (fd >= 0)
    {
        int replyfd = -1;
        int running = true;

        /* Wait for a packet from the control device */
        do
        {
            struct timespec     now;
            uint8_t             buf [PACKET_LENGTH];
            data_packet_t       packet;
            int                 rc;

            rc = recv (fd, buf, sizeof (buf), 0);
            if (rc < 0)
            {
                runtime_error ("Receive failed");
            }

            if (clock_gettime (CLOCK_REALTIME, &now) < 0)
            {
                runtime_error ("Couldn't read time");
            }

            info ("Packet read\n");
            if (0 == read_packet (&packet, buf, rc))
            {
                /* Complete reply details */
                write_uint32 (buf, PACKET_SLAVE_IP_OFFSET, interface_address);
                write_uint32 (buf, PACKET_SLAVE_SECONDS_OFFSET, now.tv_sec);
                write_uint32 (buf, PACKET_SLAVE_NANOSECONDS_OFFSET, now.tv_nsec);

                /* Open link back to control if required */
                if (-1 == replyfd)
                {
                    replyfd = open_slave_reply (packet.control_ip, packet.control_port);
                }

                if (packet.flags & PACKET_FLAG_DONE)
                {
                    running = false;
                }
                else if (sizeof (buf) != send (replyfd, buf, sizeof (buf), 0))
                {
                    runtime_error ("Can't send reply to control");
                }
                else
                {
                    uint32_t seq = read_uint32 (buf, PACKET_SEQUENCE_OFFSET);

                    printf ("\b\b\b\b\b%5d", seq);
                }
            }
            else
            {
                /* Unexpected packet length */
                fatal_error ("Short packet\n");
            }
        } while (running);
        printf ("\n");

        close (replyfd);
        close (fd);
    }
}

/**
 *  Run control side operations
 */
static void do_control (void)
{
    int                 fd = configure_control ();
    int                 reply_fd = open_control_reply (reply_port);
    uint8_t             packet [PACKET_LENGTH];
    unsigned            sequence = 0;
    pthread_t           threadid;
    struct sockaddr_in  addr;

    info ("Configured control\n");

    /* Start reply reader */
    if (pthread_create (&threadid, 0, control_reply_reader, &reply_fd))
    {
        runtime_error ("Can't start reader thread");
    }

    info ("Started control reader\n");

    /* Set static data in packet */
    memset (packet, 0, sizeof (packet));
    write_uint32(packet, PACKET_CONTROL_IP_OFFSET, interface_address);
    write_uint32(packet, PACKET_CONTROL_PORT_OFFSET, reply_port);

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl (multicast_address);
    addr.sin_port = htons (listen_port);

    /* Send data */
    while (number_loops-- > 0)
    {
        struct timespec now;
        int             rc;

        info ("Send %d\n", sequence);
        write_uint32 (packet, PACKET_SEQUENCE_OFFSET, sequence++);
        clock_gettime (CLOCK_REALTIME, &now);
        write_uint32 (packet, PACKET_SECONDS_OFFSET, now.tv_sec);
        write_uint32 (packet, PACKET_NANOSECONDS_OFFSET, now.tv_nsec);
        rc = sendto (fd, packet, sizeof (packet), 0, (struct sockaddr *)&addr, sizeof(addr));
        if (rc < 0)
        {
            runtime_error ("Send failed");
        }
        else if (rc != sizeof (packet))
        {
            fatal_error ("Send failed: not enough sent (%d/%d)\n", rc, sizeof (packet));
        }
        sleep (cycle_delay);
    }

    info ("Closing down slaves\n");
    /* Send complete to slaves: invalidate sequence number */
    write_uint32 (packet, PACKET_SEQUENCE_OFFSET, ~0);
    write_uint32(packet, PACKET_FLAGS_OFFSET, PACKET_FLAG_DONE);
    sendto (fd, packet, sizeof (packet), 0, (struct sockaddr *)&addr, sizeof(addr));
    sendto (fd, packet, sizeof (packet), 0, (struct sockaddr *)&addr, sizeof(addr));
    close (fd);

    info ("Closing down reader\n");
    /* Wait for reader exit */
    control_running = false;
    pthread_join (threadid, NULL);
}

/**
 *  Process replies from slaves.
 */
static void *control_reply_reader (void *arg)
{
    int listen_fd = * (int *) arg;
    int i;

    assert (listen_fd >= 0);

    while (control_running)
    {
        fd_set  readfds;
        fd_set  exceptfds;
        struct timeval  timeout;
        int     nfds;
        int     rc;

        timeout.tv_sec = 0;
        timeout.tv_usec = 10000; /* 10ms */

        /* Only interesting in reading and exceptions */
        FD_ZERO (&readfds);
        FD_ZERO (&exceptfds);
        FD_SET (listen_fd, &readfds);
        nfds = listen_fd;
        for (i = 0; i < num_clients; i++)
        {
            if (-1 != clients [i].fd)
            {
                FD_SET (clients [i].fd, &readfds);
                FD_SET (clients [i].fd, &exceptfds);
                if (clients [i].fd > nfds)
                {
                    nfds = clients [i].fd;
                }
            }
        }
        /* Limit is highest file descriptor plus one */
        nfds++;
        rc = select(nfds, &readfds, NULL, &exceptfds, &timeout);
        if (rc < 0)
        {
            /* Ignore signals */
            if (EINTR != rc)
            {
                runtime_error ("Select failed in reader");
            }
        }
        else if (rc > 0)
        {
            /* Check for new clients */
            if (FD_ISSET (listen_fd, &readfds))
            {
                add_client (listen_fd);
            }
            for (i = 0; i < num_clients; i++)
            {
                /* Process existing clients */
                if (-1 != clients [i].fd)
                {
                    if (FD_ISSET (clients [i].fd, &exceptfds))
                    {
                        close_client (i, "%s died\n");
                    }
                    else if (FD_ISSET (clients [i].fd, &readfds))
                    {
                        process_client (i);
                    }
                }
            }
        }
    }

    if (!unsorted)
    {
        display_pending_packets ();
    }

    /* Close any client sockets */
    for (i = 0; i < num_clients; i++)
    {
        if (-1 != clients [i].fd)
        {
            close (clients [i].fd);
        }
    }
    close (listen_fd);

    return  0;
}

/**
 *  Add a new client
 */
static void add_client (int listen_fd)
{
    struct sockaddr_in  addr;
    socklen_t           addr_len = sizeof (addr);

    int fd = accept (listen_fd, (struct sockaddr *) &addr, &addr_len);
    if (fd < 0)
    {
        perror ("Couldn't accept new client");
    }
    else if (num_clients == MAX_CLIENTS)
    {
        fatal_error ("Too many clients\n");
    }
    else
    {
        clients [num_clients].fd = fd;
        clients [num_clients].address = ntohl (addr.sin_addr.s_addr);
        printf ("%s added\n", ipaddr_str (clients [num_clients].address));
        num_clients++;
    }
}

/**
 *  Process the results from one client
 */
static void process_client (int client_id)
{
    uint8_t         buffer [PACKET_LENGTH];
    data_packet_t   packet;
    int             rc;

    rc = recv (clients [client_id].fd, buffer, sizeof (buffer), 0);
    if (rc < 0)
    {
        close_client (client_id, "%s error - closed\n");
    }
    else if (0 == rc)
    {
        close_client (client_id, "%s has stopped\n");
    }
    else if (0 == read_packet (&packet, buffer, rc))
    {
        data_packet_t   *new_packet;

        /* Add copy of packet to client's details */
        new_packet = malloc (sizeof (packet));
        if (NULL == new_packet)
        {
            fatal_error ("Out of memory\n");
        }
        memcpy (new_packet, &packet, sizeof (packet));
        new_packet->next = clients [client_id].tail;
        clients [client_id].tail = new_packet;
        if (NULL == clients [client_id].head)
        {
            clients [client_id].head = new_packet;
        }

        if (unsorted)
        {
            process_packet (new_packet, false);
        }
        else
        {
            add_pending_packet (new_packet);
        }
    }
    else
    {
        printf ("%s bad message\n", ipaddr_str ((clients [client_id].address)));
    }
}

/**
 *  Mark a client as closed (but don't delete it)
 */
static void close_client (int client_id, char *message)
{
    close (clients [client_id].fd);
    clients [client_id].fd = -1;
    printf (message, ipaddr_str (clients [client_id].address));
}

/**
 *  Configure slave for multicast operation
 */
static int configure_slave (void)
{
    int                 fd;
    struct sockaddr_in  addr;
    struct ip_mreq      imr;
    int                 opt;

    fd = socket (PF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (fd < 0)
    {
        runtime_error ("Can't open (UDP) socket");
    }

    /* Allow address reuse */
    opt = 1;
    if (setsockopt (fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof (opt)) < 0)
    {
        runtime_error ("Can't reuse address");
    }

    /* Allow reception of multicast */
    addr.sin_family = AF_INET;
    addr.sin_port = htons (listen_port);
    addr.sin_addr.s_addr = htonl (INADDR_ANY);
    if (bind (fd, (struct sockaddr *) &addr, sizeof (addr)) < 0)
    {
        runtime_error ("Can't bind to multicast address");
    }

    memset (&imr, 0, sizeof (imr));
    imr.imr_multiaddr.s_addr = htonl (multicast_address);
    imr.imr_interface.s_addr = htonl (INADDR_ANY);
    if (setsockopt (fd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &imr, sizeof (imr)) < 0)
    {
        runtime_error ("Can't join multicast group");
    }

    return  fd;

}

/**
 *  Configure slave for multicast operation
 */
static int configure_control (void)
{
    int                 fd;
    struct sockaddr_in  addr;
    int                 opt;

    fd = socket (PF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (fd < 0)
    {
        runtime_error ("Can't open (UDP) socket");
    }

    /*  Limit TTL */
    opt = 1;
    if (setsockopt (fd, IPPROTO_IP, IP_MULTICAST_TTL, &opt, sizeof (opt)) < 0)
    {
        runtime_error ("Can't set TTL");
    }

    /* Allow address reuse */
    opt = 1;
    if (setsockopt (fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof (opt)) < 0)
    {
        runtime_error ("Can't reuse address");
    }

    return  fd;

}

/**
 *  Write a uint32_t into a buffer in network order
 */
static void write_uint32 (uint8_t *buf, unsigned offset, uint32_t data)
{
    assert ((offset & 3) == 0);
    *((uint32_t *) &buf [offset]) = htonl (data);
}

/**
 *  Read a uint32_t from a buffer in host order
 */
static uint32_t read_uint32 (uint8_t *buf, unsigned offset)
{
    uint32_t    data;

    assert ((offset & 3) == 0);
    data = *((uint32_t *) &buf [offset]);

    return  ntohl (data);
}

/**
 *  Read the contents of a network packet
 */
static int read_packet (data_packet_t *packet, uint8_t *buf, unsigned length)
{
    int rc = -1;
    struct timespec now;

    if (length == PACKET_LENGTH)
    {
        /* Leave 'next' alone: caller's responsibility */
        packet->flags = read_uint32 (buf, PACKET_FLAGS_OFFSET);
        packet->sequence = read_uint32 (buf, PACKET_SEQUENCE_OFFSET);
        packet->seconds = read_uint32 (buf, PACKET_SECONDS_OFFSET);
        packet->nanoseconds = read_uint32 (buf, PACKET_NANOSECONDS_OFFSET);
        packet->control_ip = read_uint32 (buf, PACKET_CONTROL_IP_OFFSET);
        packet->control_port = read_uint32 (buf, PACKET_CONTROL_PORT_OFFSET);
        packet->slave_ip = read_uint32 (buf, PACKET_SLAVE_IP_OFFSET);
        packet->slave_seconds = read_uint32 (buf, PACKET_SLAVE_SECONDS_OFFSET);
        packet->slave_nanoseconds = read_uint32 (buf, PACKET_SLAVE_NANOSECONDS_OFFSET);
        clock_gettime (CLOCK_REALTIME, &now)
        packet->recv_seconds = now.tv_sec;
        packet->recv_nanoseconds = now.tv_nsec;

        rc = 0;
    }

    return  rc;
}

/**
 *  Process the contents of a packet
 */
static int process_packet (data_packet_t *packet, bool short_form)
{
    assert (NULL != packet);
    struct timespec     control;
    struct timespec     slave;
    struct timespec     recvt;
    struct timespec     diff;
    struct timespec     difftot;
    char   scratch      [80];
    char   rscratch     [80];

    control.tv_sec = (time_t) packet->seconds;
    control.tv_nsec = packet->nanoseconds;
    slave.tv_sec = (time_t) packet->slave_seconds;
    slave.tv_nsec = packet->slave_nanoseconds;
    recvt.tv_sec = (time_t) packet->recv_seconds;
    recvt.tv_nsec = packet->recv_nanoseconds;
    /* Allow for wrap */
    if (slave.tv_nsec > control.tv_nsec)
    {
        diff.tv_nsec = NSEC_IN_SEC + control.tv_nsec - slave.tv_nsec;
        diff.tv_sec = control.tv_sec - slave.tv_sec - 1;
    }
    else
    {
        diff.tv_nsec = control.tv_nsec - slave.tv_nsec;
        diff.tv_sec = control.tv_sec - slave.tv_sec;
    }
    if (recvt.tv_nsec > control.tv_nsec)
    {
        difftot.tv_nsec = NSEC_IN_SEC + control.tv_nsec - recvt.tv_nsec;
        difftot.tv_sec = control.tv_sec - recvt.tv_sec - 1;
    }
    else
    {
        difftot.tv_nsec = control.tv_nsec - recvt.tv_nsec;
        difftot.tv_sec = control.tv_sec - recvt.tv_sec;
    }

    if (short_form)
    {
        /* Pad to keep alignment */
        printf ("%15s ", ipaddr_str (packet->slave_ip));
    }
    else
    {
        printf ("%s %4d C %ld.%09ld S %ld.%09ld R %ld.%09ld delta ",
            ipaddr_str (packet->slave_ip),
            packet->sequence,
            packet->seconds,
            packet->nanoseconds,
            packet->slave_seconds,
            packet->slave_nanoseconds,
            packet->recv_seconds,
            packet->recv_nanoseconds);
    }

    if (diff.tv_sec >= 0)
    {
        sprintf (scratch, " %d.%09ld", diff.tv_sec, diff.tv_nsec);
    }
    else
    {
        sprintf (scratch, "-%d.%09ld", ~diff.tv_sec, NSEC_IN_SEC - diff.tv_nsec);
    }
    printf ("%16s", scratch);
    if (difftot.tv_sec >= 0)
    {
        sprintf (rscratch, " %d.%09ld", difftot.tv_sec, difftot.tv_nsec);
    }
    else
    {
        sprintf (rscratch, "-%d.%09ld", ~difftot.tv_sec, NSEC_IN_SEC - difftot.tv_nsec);
    }
    printf ("%16s", rscratch);

    if (!short_form)
    {
        printf ("\n");
    }
}

/**
 *  Control: open a TCP socket to process slave replies
 */
static int open_control_reply (in_port_t port)
{
    int                 fd;
    struct sockaddr_in  addr;
    int                 opt;

    fd = socket (PF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (fd < 0)
    {
        runtime_error ("Can't open reply socket");
    }

    /* Allow address reuse */
    opt = 1;
    if (setsockopt (fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof (opt)) < 0)
    {
        runtime_error ("Can't reuse address");
    }

    memset (&addr, 0, sizeof (addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons (port);
    addr.sin_addr.s_addr = htonl (INADDR_ANY);
    if (bind (fd, (struct sockaddr *) &addr, sizeof (addr)))
    {
        runtime_error ("Can't bind to reply address");
    }

    if (listen (fd, MAX_CLIENTS))
    {
        runtime_error ("Can't listen on reply address");
    }

    return  fd;
}

/**
 *  Slave: open a TCP socket to send replies to the control
 */
static int open_slave_reply (in_addr_t address, in_port_t port)
{
    int                 fd;
    struct sockaddr_in  addr;
    int                 opt;

    fd = socket (PF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (fd < 0)
    {
        runtime_error ("Can't open reply socket");
    }
    info ("Opening connection to: %s/%d\n", ipaddr_str (address), port);
    memset (&addr, 0, sizeof (addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons (port);
    addr.sin_addr.s_addr = htonl (address);
    if (connect (fd, (struct sockaddr *) &addr, sizeof (addr)))
    {
        runtime_error ("Can't connect to control's reply address");
    }

    /* Want to send results as soon as possible */
    opt = 1;
    if (0 != setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof (opt)))
    {
        runtime_error ("Can't set NODELAY option on socket");
    }

    return  fd;
}

/**
 *  Report a runtime (as defined by errno) error and exit.
 */
static void runtime_error (char *text)
{
    perror (text);
    exit (-1);
}

/**
 *  Report an error and exit.
 */
static void fatal_error (const char *fmt, ...)
{
    va_list ap;

    va_start (ap, fmt);
    vfprintf (stderr, fmt, ap);
    va_end (ap);
    exit (1);
}

/**
 *  Print an information message, depending on whether verbose is set.
 */
static void info (const char *fmt, ...)
{
    if (verbose)
    {
        va_list ap;

        va_start (ap, fmt);
        vfprintf (stdout, fmt, ap);
        va_end (ap);
    }
}

/**
 *  Print an IP address in dotted form.
 *
 *  inet_ntoa is supposed to return a string but, on 64bit Linux,
 *  its use causes a crash.
 */
static char *ipaddr_str (in_addr_t address)
{
    static char buf [64];

    sprintf (buf, "%d.%d.%d.%d",
        (address >> 24) & 0xff,
        (address >> 16) & 0xff,
        (address >>  8) & 0xff,
        (address >>  0) & 0xff);

    return buf;
}


/**
 *  Add a packet to the queue awaiting display.  If the queue is full or
 *  the sequence number has changed then display the queue.
 *
 *  A full queue implies an (external) error
 */
static void add_pending_packet (data_packet_t *packet)
{
    assert (0 != packet);

    if ((pending_index == MAX_CLIENTS)
    ||  ((pending_index > 0) && (packet->sequence != pending_packets [0]->sequence)))
    {
        display_pending_packets ();
        pending_index = 0;
    }

    if (0 != packet)
    {
        pending_packets [pending_index++] = packet;
    }

}

/**
 *  Display pending packets in sorted order
 */
static void display_pending_packets (void)
{
    int i;

    qsort (pending_packets, pending_index, sizeof (data_packet_t *), compare_packet);

    if (oneline)
    {
        for (i = 0; i < pending_index; i++)
        {
            process_packet(pending_packets [i], false);
        }
    }
    else
    {
        data_packet_t   *packet = pending_packets [0];

        printf ("%4d C %ld.%09ld :", packet->sequence, packet->seconds, packet->nanoseconds);
        for (i = 0; i < pending_index; i++)
        {
            process_packet(pending_packets [i], true);
        }
        printf ("\n");
    }
}

/**
 *  Compare two packets by IP address.
 */
static int compare_packet (const void *first, const void *second)
{
    const data_packet_t   *first_packet = *(const data_packet_t **) first;
    const data_packet_t   *second_packet = *(const data_packet_t **) second;

    assert (0 != first_packet);
    assert (0 != second_packet);

    return  (int32_t) first_packet->slave_ip - (int32_t) second_packet->slave_ip;

}
