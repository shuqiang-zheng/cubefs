#ifndef RDMA_H_
#define RDMA_H_

#include <stdint.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <signal.h>
#include <fcntl.h>
#include <poll.h>

#include <unistd.h>
#include <sys/types.h>
#include <sys/poll.h>
#include <sys/stat.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "epoll.h"
#include "connection.h"
#include "transfer_event.h"






extern void ServerConnectionCallback(int, Connection*);
extern void ClientConnectionCallback(int, Connection*);
#define TEST_NZ(x) if ( (x)) fprintf(stderr, "%s:%d %d, %s\n", __FILE__, __LINE__, errno, strerror(errno));
#define TEST_Z(x)  if (!(x)) fprintf(stderr, "%s:%d %d, %s\n", __FILE__, __LINE__, errno, strerror(errno));

#define TEST_NZ_(x) if ((x)) { \
    fprintf(stderr, "%s:%d %d, %s\n", __FILE__, __LINE__, errno, strerror(errno)); \
    return NULL; \
}

#define TEST_Z_(x) if (!(x)) { \
    fprintf(stderr, "%s:%d %d, %s\n", __FILE__, __LINE__, errno, strerror(errno)); \
    return NULL; \
}

#endif

