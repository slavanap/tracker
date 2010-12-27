/*
 * Mini torrent tracker written in C (POSIX)
 * Author: Napadovsky Vyacheslav, 2010
 */

#include <stdio.h>
#include <limits.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <signal.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <time.h>
#include <netdb.h>

#define DEFAULT_PORT 80
#define MAX_INCOMPLETEDCONN 50
#define HTTP_BUFFER_SIZE 2048
#define SEP_BUFFER_SIZE 40
#define PROCEED_WAIT_SECONDS 10
#define SIZE_PEERID 60
#define SIZE_INFOHASH 60

// --- Settings
/* 
 * Peers should wait at least this many seconds between announcements [seconds]
 */
#define min_announce_interval 900

/*
 * Maximum desired announcements per minute for all peers combined
 * (announce interval will be increased if necessary to achieve this)
 * [announcements per minute]
 */
#define max_announce_rate 500

/*
 * Consider a peer dead if it has not announced in a number of seconds equal
 * to this many times the calculated announce interval at the time of its last
 * announcement (must be greater than 1; recommend 1.2)
 */
#define expire_factor 1.2

/*
 * Peers should wait at least this many times the current calculated announce
 * interval between scrape requests
 */
#define scrape_factor 0.5  /* not released yet */

// --- END of Settings

#define TEXTCOLOR(color, text)	"\033[01;"#color"m"text"\033[0m"
#define SOCKET_ERR -1

int32_t endian_swap32(int32_t x)
{
    return (x>>24) | ((x<<8) & 0x00FF0000) | ((x>>8) & 0x0000FF00) | (x<<24);
}

void printtime(void)
{
    time_t t = time(NULL);
    char buffer[100];
    strftime(buffer, 100, "%d/%m/%Y %H:%M:%S", localtime(&t));
    printf("[%s] ", buffer);
}

// ### DataBase

struct peer {
    struct in_addr ip;
    int port;
    char peerid[SIZE_PEERID+1];
    long long uploaded, downloaded, left;
    time_t update, expire;
    struct peer *next;
};

struct base {
    char infohash[SIZE_INFOHASH+1];
    int cpeers;
    int cuppeers;
    struct peer *peers;
    struct base *next;
};
    
struct base *db = NULL;
int announce_interval;

struct peer * clear_peer(struct peer *peer)
{
    struct peer *p = peer->next;
    free(peer);
    return p;
}

struct base * clear_base(struct base *base)
{
    struct base *b = base->next;
    free(base);
    return b;
}

struct peer * update_peer(struct base *base, struct peer *peer)
{
    struct peer **p, *t;
    p = &base->peers;
    while (*p != NULL)
    {
        if (!memcmp(&peer->ip, &(*p)->ip, sizeof(peer->ip)) && 
           (peer->port == (*p)->port) && !strcasecmp(peer->peerid, (*p)->peerid))
            goto update;
        p = &(*p)->next;
    }
    *p = calloc(1, sizeof(**p));
    base->cpeers++;
    base->cuppeers++;
update:
    t = (*p)->next;
    **p = *peer;
    (*p)->next = t;
    (*p)->update = time(NULL);
    announce_interval = base->cpeers * base->cuppeers * 60 / (max_announce_rate * max_announce_rate);
    if (announce_interval < min_announce_interval)
        announce_interval = min_announce_interval;
    (*p)->expire = (*p)->update + (expire_factor*announce_interval);
    return *p;
}

struct base * search_infohash(char *infohash)
{
    struct base **b;
    b = &db;
    while (*b != NULL)
    {
        if (!strcasecmp(infohash, (*b)->infohash))
            return *b;
        b = &(*b)->next;
    }
    *b = calloc(1, sizeof(**b));
    strcpy((*b)->infohash, infohash);
    return *b;
}

void clearbase(struct base *base, time_t curtime)
{
    struct peer **p = &base->peers;
    base->cuppeers = 0;
    while (*p != NULL)
    {
        if ((*p)->expire < curtime)
        {
            *p = clear_peer(*p);
            base->cpeers--;
        }
        else
        {
            if ((*p)->expire-60 > curtime)
                base->cuppeers++;
            p = &(*p)->next;
        }
    }
}

void updatebases(void)
{
    struct base **b = &db;
    time_t curtime = time(NULL);
    while (*b != NULL)
    {
        clearbase(*b, curtime);
        if ((*b)->cpeers == 0)
            *b = clear_base(*b);
        else
            b = &(*b)->next;
    }
}



// ### Providing

struct request {
    char *name, *value;
    int tag;
};

const char *notfound =
    "HTTP/1.1 404 Not Found\r\n"
    "Server: mini-tracker\r\n"
    "Connection: close\r\n"
    "Content-Type: text/html\r\n\r\n"
    "<html><head>404 NOT FOUND</head><body><h1>404</h1>Page not found!</body></html>";
const char *answer = 
    "HTTP/1.1 200 OK\r\n"
    "Server: mini-tracker\r\n"
    "Connection: close\r\n"
    "Cache-Control: no-cache\r\n"
    "Content-Type: text/plain\r\n\r\n";
const char *invalid_req = 
    "d14:failure reason69:invalid request (see http://bitconjurer.org/BitTorrent/protocol.html)e";

struct request * lookfor(struct request *req, char *str)
{
    while (req->name != NULL)
    {
        if (!strcasecmp(req->name, str))
            return req;
        req++;
    }
    return NULL;
}

void provide_announce(struct in_addr *addr, int hsocket, struct request *req)
{
    char *buffer, *s, *infohash, *s2;
    struct request *r;
    struct peer peer, *p, *added;
    struct hostent *host;
    struct base *b;
    int32_t t;
    
    r = lookfor(req, "info_hash");
    if ((r == NULL) || (strlen(infohash = r->value) > SIZE_INFOHASH))
        goto error;
    r = lookfor(req, "port");
    if (r == NULL)
        goto error;
    peer.port = strtol(r->value, &s, 10);
    if (*s || (peer.port > USHRT_MAX) || (peer.port <= 0))
        goto error;
    r = lookfor(req, "peer_id");
    if ((r == NULL) || (strlen(r->value) > SIZE_PEERID))
        goto error;
    strcpy(peer.peerid, r->value);
    
    r = lookfor(req, "uploaded");
    if (r == NULL)
        goto error;
    peer.uploaded = strtoll(r->value, &s, 10);
    if (*s || (peer.uploaded < 0))
        goto error;
    r = lookfor(req, "downloaded");
    if (r == NULL)
        goto error;
    peer.downloaded = strtoll(r->value, &s, 10);
    if (*s || (peer.downloaded < 0))
        goto error;
    r = lookfor(req, "left");
    if (r == NULL)
        goto error;
    peer.left = strtoll(r->value, &s, 10);
    if (*s || (peer.left < 0))
        goto error;
    r = lookfor(req, "event");
    if ((r != NULL) && (strcmp(r->value, "started") && strcmp(r->value, "completed") && strcmp(r->value, "stopped")))
        goto error;
    
    peer.ip.s_addr = -1;
    r = lookfor(req, "ip");
    if (r != NULL)
    {
        printf("<ip:value='" TEXTCOLOR(33, "%s") "'>", r->value);
        if ((peer.ip.s_addr = inet_addr(r->value)) == -1)
        {
            host = gethostbyname(r->value);
            if ((host != NULL) && (host->h_addrtype == AF_INET) && (host->h_length == 4) && (host->h_addr_list != NULL) &&
                (*(struct in_addr **)host->h_addr_list != NULL))
            {
                peer.ip = **(struct in_addr **)host->h_addr_list;
                printf("<dns>");
            }
        }
    }
    if (peer.ip.s_addr == -1)
        peer.ip = *addr;
    printf("<debug:peer.ip=" TEXTCOLOR(33,"%s") ">\n", inet_ntoa(peer.ip));
    
    // Generating responce...
    b = search_infohash(infohash);
    added = update_peer(b, &peer);
    
    if (lookfor(req, "compact") != NULL)
    {
        #define COMPACT_SIZE (sizeof(int32_t)+sizeof(int16_t))
        s = buffer = malloc(COMPACT_SIZE * b->cpeers + 50);
        s += sprintf(s, "d8:intervali%de" "5:peers%d:", announce_interval, COMPACT_SIZE * (b->cpeers - 1));
        p = b->peers;
        while (p != NULL)
        {
            if (p != added)
            {
                t = endian_swap32(htonl(p->ip.s_addr));
                *(int16_t *)s = t & USHRT_MAX;
                *(int16_t *)(s+2) = t >> CHAR_BIT*2;
                s += sizeof(int32_t);
                *((int16_t *)s) = htons(p->port);
                s += sizeof(int16_t);
            }
            p = p->next;
        }
        *s++ = 'e';
    }
    else if (lookfor(req, "no_peer_id") != NULL)
    {
        #define NOPEERID_SIZE (15+6+15)
        s = buffer = malloc(NOPEERID_SIZE * b->cpeers + 50);
        s += sprintf(s, "d" "8:interval" "i%de" "5:peers" "l", announce_interval);
        p = b->peers;
        while (p != NULL)
        {
            if (p != added)
            {
                s2 = inet_ntoa(p->ip);
                s += sprintf(s, "d" "2:ip" "%d:%s" "4:port" "i%de" "e", strlen(s2), s2, p->port);
            }
            p = p->next;
        }
        *s++ = 'e';
        *s++ = 'e';
    }
    else
        goto error;
    
    send(hsocket, answer, strlen(answer), 0);
    send(hsocket, buffer, s-buffer, 0);
    free(buffer);
    printf(TEXTCOLOR(32, "Successfull request") "\n");
    return;
error:
    send(hsocket, invalid_req, strlen(invalid_req), 0);
    printf(TEXTCOLOR(31, "Invalid bittorrent request") "\n");
}

void provide_scrape(int hsocket, struct request *req)
{
    printf(TEXTCOLOR(33, "scrape.php is not released yet") "\n");
    send(hsocket, notfound, strlen(notfound), 0);
}



// ### Support

const char *announce = "/announce.php?";
const char *scrape = "/scrape.php?";

int hserver, hsocket = SOCKET_ERR;
int alrm_was;

void check(int value, char *error)
{
    if (value) return;
    printf(TEXTCOLOR(31, "%s") "\n", error);
    exit(-1);
}

void hsig_int(int sig)
{
    printf(TEXTCOLOR(35, "Exiting...") "\n");
    if (hsocket != SOCKET_ERR)
    {
        shutdown(hsocket, SHUT_RDWR);
        close(hsocket);
    }
    shutdown(hserver, SHUT_RDWR);
    close(hserver);
    exit(0);
}

void hsig_alrm(int sig)
{
    alrm_was = 1;
}

int req_cmp(struct request *a1, struct request *a2)
{
    return strcmp(a1->name, a2->name);
}

void req_free(struct request *req)
{
    struct request *p = req;
    if (req != NULL)
    {
        while (p->name != NULL)
        {
            free(p->name);
            if (p->value != NULL) free(p->value);
            p++;
        }
        free(req);
    }
}

void proceed(struct sockaddr_in *addr)
{
    char *buffer, *pos, *s;
    int r, i, left = HTTP_BUFFER_SIZE;
    struct request *req;
    
    alrm_was = 0;
    alarm(PROCEED_WAIT_SECONDS);
    buffer = pos = (char *)malloc(HTTP_BUFFER_SIZE+1);
    {
        r = recv(hsocket, pos, left, 0);
        if (r == -1)
        {
            if (errno != EAGAIN)
            {
                perror("ERROR");
                printf(TEXTCOLOR(31, "Error on socket") "\n");
                goto error;
            }
            r = 0;
        }
        pos += r;
        left -= r;
        if (left == 0)
        {
            printf(TEXTCOLOR(31, "Request is too big") "\n");
            goto error;
        }
        pos[0] = '\0';
    }
    while (!alrm_was && strcmp(pos-4, "\r\n\r\n"));
    alarm(0);
    if (alrm_was)
    {
        printf(TEXTCOLOR(31, "Request proceeded time is too long") "\n");
        goto error;
    }
    if (strncmp(buffer, "GET ", 4))
    {
    inv:
        printf(TEXTCOLOR(31, "Invalid request") "\n");
        goto error;
    }
    pos = buffer + 4;
    if (strncmp(pos, announce, r = strlen(announce)) && strncmp(pos, scrape, r = strlen(scrape)))
    {
        send(hsocket, notfound, strlen(notfound), 0);
        goto inv;
    }
    s = (pos += r-2);
    while (*s && (*s != ' '))
        s++;
    *s = '\0';
    strtok(pos, "?");
    
    req = calloc(SEP_BUFFER_SIZE+1, sizeof(*req));
    for (i = 0; i < SEP_BUFFER_SIZE; i++)
    {
        s = strtok(NULL, "=");
        if (s == NULL)
            break;
        req[i].name = strdup(s);
        s = strtok(NULL, "&");
        if (s == NULL)
        {
            req_free(req);
            goto inv;
        }
        req[i].value = strdup(s);
    }
    if (i == 0)
    {
        req_free(req);
        goto inv;
    }
    qsort(req, i, sizeof(struct request), (int(*)(const void *, const void *))req_cmp);

    updatebases();
    if (r == strlen(announce))
        provide_announce(&addr->sin_addr, hsocket, req);
    else
        provide_scrape(hsocket, req);
    req_free(req);
error:
    free(buffer);
}

int main(int argc, char *argv[])
{
    char *s;
    struct sockaddr_in saddr;
    int port = DEFAULT_PORT;
    socklen_t len;
    
    // Parsing params...
    if (argc > 2)
    {
    error:
        printf(TEXTCOLOR(31, "Error in arguments count.") "\n" TEXTCOLOR(33, "Usage: %s [bindport]") "\n", argv[0]);
        return -1;
    }
    if (argc == 2)
    {
        port = USHRT_MAX & strtol(argv[1], &s, 0);
        if (*s) goto error;
    }
    
    // Init sockets...
    hserver = socket(AF_INET, SOCK_STREAM, 0);
    check(hserver != SOCKET_ERR, "Can't create socket");
    saddr.sin_family = AF_INET;
    saddr.sin_port = htons(port);
    saddr.sin_addr.s_addr = INADDR_ANY;
    check(bind(hserver, (struct sockaddr *)&saddr, sizeof(saddr)) == 0, "Can't bind socket");
    check(listen(hserver, MAX_INCOMPLETEDCONN) == 0, "Can't exec 'listen'");
    printf(TEXTCOLOR(35, "Mini-Tracker started") "\n");
    
    signal(SIGALRM, hsig_alrm);
    signal(SIGINT, hsig_int);
    while (1)
    {
        len = sizeof(saddr);
        hsocket = accept(hserver, (struct sockaddr *)&saddr, &len);
        check(hsocket != SOCKET_ERR, "Error on 'accept'");
        printtime();
        printf("Incoming connection from %s (%d)\n", inet_ntoa(saddr.sin_addr), ntohs(saddr.sin_port));
        proceed(&saddr);
        shutdown(hsocket, SHUT_RDWR);
        close(hsocket);
        hsocket = SOCKET_ERR;
    }
    return 0;
}
