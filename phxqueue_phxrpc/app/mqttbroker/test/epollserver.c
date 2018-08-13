#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>


#define MAXEPOLLSIZE 10000
#define MAXLINE 10240

int handle(int connfd);

int setnonblocking(int sockfd) {
    if (fcntl(sockfd, F_SETFL, fcntl(sockfd, F_GETFD, 0)|O_NONBLOCK) == -1) {
        return -1;
    }
    return 0;
}

int main(int argc, char **argv) {
    int  servPort{6888};
    int listenq{1024};

    int listenfd, connfd, kdpfd, nfds, n, curfds,acceptCount = 0;
    struct sockaddr_in servaddr, cliaddr;
    socklen_t socklen = sizeof(struct sockaddr_in);
    struct epoll_event ev;
    struct epoll_event events[MAXEPOLLSIZE];
    struct rlimit rt;
    char buf[MAXLINE];

    rt.rlim_max = rt.rlim_cur = MAXEPOLLSIZE;
    if (setrlimit(RLIMIT_NOFILE, &rt) == -1) {
        perror("setrlimit error");
        return -1;
    }

    bzero(&servaddr, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htonl (INADDR_ANY);
    servaddr.sin_port = htons (servPort);

    listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenfd == -1) {
        perror("can't create socket file");
        return -1;
    }

    int opt{1};
    setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    if (setnonblocking(listenfd) < 0) {
        perror("setnonblock error");
    }

    if (bind(listenfd, (struct sockaddr *) &servaddr, sizeof(struct sockaddr)) == -1) {
        perror("bind error");
        return -1;
    }
    if (listen(listenfd, listenq) == -1) {
        perror("listen error");
        return -1;
    }
    // add listen fds to epoll
    kdpfd = epoll_create(MAXEPOLLSIZE);
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = listenfd;
    if (epoll_ctl(kdpfd, EPOLL_CTL_ADD, listenfd, &ev) < 0) {
        fprintf(stderr, "epoll set insertion error: fd=%d\n", listenfd);
        return -1;
    }
    curfds = 1;

    printf("epollserver startup,port %d, max connection is %d, backlog is %d\n", servPort, MAXEPOLLSIZE, listenq);

    for (;;) {
        // wait for events
        nfds = epoll_wait(kdpfd, events, curfds, -1);
        if (nfds == -1) {
            perror("epoll_wait");
            continue;
        }
        // handle events
        for (n = 0; n < nfds; ++n) {
            if (events[n].data.fd == listenfd) {
              connfd = accept(listenfd, (struct sockaddr *)&cliaddr, &socklen);
              if (connfd < 0) {
                  perror("accept error");
                  continue;
              }

              sprintf(buf, "accept form %s:%d\n", inet_ntoa(cliaddr.sin_addr), cliaddr.sin_port);
              printf("%d:%s", ++acceptCount, buf);

              if (curfds >= MAXEPOLLSIZE) {
                  fprintf(stderr, "too many connection, more than %d\n", MAXEPOLLSIZE);
                  close(connfd);
                  continue;
              }
              if (setnonblocking(connfd) < 0) {
                  perror("setnonblocking error");
              }
              ev.events = EPOLLIN | EPOLLET;
              ev.data.fd = connfd;
              if (epoll_ctl(kdpfd, EPOLL_CTL_ADD, connfd, &ev) < 0) {
                  fprintf(stderr, "add socket '%d' to epoll failed: %s\n", connfd, strerror(errno));
                  return -1;
              }
              curfds++;

              continue;
            }
            // handle client request
            if (handle(events[n].data.fd) < 0) {
                epoll_ctl(kdpfd, EPOLL_CTL_DEL, events[n].data.fd,&ev);
                curfds--;
            }
        }
    }
    close(listenfd);

    return 0;
}

int handle(int connfd) {
    int nread;
    char buf[MAXLINE];
    nread = read(connfd, buf, MAXLINE);  // read from client

    if (nread == 0) {
        printf("client close the connection\n");
        close(connfd);

        return -1;
    }
    if (nread < 0) {
        perror("read error");
        close(connfd);

        return -1;
    }
    write(connfd, buf, nread);  // write to client

    return 0;
}

