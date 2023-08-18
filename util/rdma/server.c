#include <string>
#include <stdio.h>
#include "rdma.h"
#include "connection_event.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>


/*static void HelloServer() {
    puts("HelloClient \n");
}*/

struct Server {
    Connection *conn;
    char *ip;
    int port;
    struct ibv_pd *pd;
    struct rdma_cm_id *listen_id;
    struct rdma_event_channel *ec;
    struct ibv_comp_channel *comp_channel;
    struct ibv_cq *cq;
    //int serverId;
    string remoteAddr
    struct ConnectionEvent* conn_ev;
};

static void OnServerReady(int ret, void* ctx) {
    Connection* conn = (Connection*)ctx;
    struct Server* server = (struct Server*)conn->csContext;
    printf("server=%p, conn=%p, %s \n", server, conn, __FUNCTION__);
    ServerConnectionCallback(1, conn); //connection start
}

static void OnServerPreConnect(struct rdma_cm_id *id, void* ctx) {
    struct ConnectionEvent* conn_ev = (struct ConnectionEvent*)ctx;
    struct Server* server = (struct Server*)conn_ev->ctx;
    printf("server=%p, on %s \n", server, __FUNCTION__);

    Connection* conn = AllocConnection(id, conn_ev, 1);
    server->conn = conn;
    conn->remoteAddr = server->remoteAddr;
/*    if (ListenTransferRecvEvent(conn) == -1  || ListenTransferSendEvent(conn) == -1) {
         ServerConnectionCallback(0, conn); //connection close
         return;
    }*/

    conn->csContext = server;
    conn->readyCallback = OnServerReady;
    id->context = conn;

    EpollAddSendAndRecvEvent(server->comp_channel->fd, conn);
}

static void OnServerConnected(struct rdma_cm_id *id, void* ctx) {
    struct Server* server = (struct Server*)ctx;
    printf("server=%p, on %s \n", server, __FUNCTION__);

    Connection* conn = (Connection*)id->context;

    connRdmaRegisterRx(conn, id);
    //SendMemory(conn, conn->memory_pool->localRegion);
    id->context = conn;
}


static void OnServerDisconnected(struct rdma_cm_id *id, void* ctx) {
    struct Server* server = (struct Server*)ctx;
    printf("server=%p, on %s \n", server, __FUNCTION__);
    Connection* conn = (Connection*)id->context;
    ServerConnectionCallback(0, conn); //connection close
    free(conn);
}

static struct Server* StartServer(const char* ip, uint16_t port, string serverAddr) {
    printf("StartServer ip=%s, port=%d\n", ip, port);
    struct rdma_addrinfo hints, *res;
    struct ibv_qp_init_attr init_attr;
    memset(&hints, 0, sizeof hints);
    hints.ai_flags = RAI_PASSIVE;
    hints.ai_port_space = RDMA_PS_TCP;

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family  = AF_INET;
    addr.sin_port  = htons(port);
    addr.sin_addr.s_addr = inet_addr(ip);

    struct rdma_cm_id *listen_id;
    struct rdma_event_channel *ec = NULL;

    TEST_Z_(ec = rdma_create_event_channel());
    TEST_NZ_(rdma_create_id(ec, &listen_id, NULL, RDMA_PS_TCP));
    TEST_NZ_(rdma_bind_addr(listen_id, (struct sockaddr *)&addr));
    TEST_NZ_(rdma_listen(listen_id, 10));

    struct Server* server = (struct Server*)malloc(sizeof(struct Server));
    server->listen_id = listen_id;
    server->ec = ec;
    server->ip = ip;
    server->port = port;
    //server->serverAddr = serverAddr;

    struct ConnectionEvent* conn_ev = (struct ConnectionEvent*)malloc(sizeof(struct ConnectionEvent));
    conn_ev->cm_id = listen_id;
    conn_ev->ctx = server;
    conn_ev->preconnect_callback = OnServerPreConnect;
    conn_ev->connected_callback = OnServerConnected;
    conn_ev->disconnected_callback = OnServerDisconnected;

    /*if (ListenConnectionEvent(listen_id->channel->fd, conn_ev) == -1) {
        free(server);
        free(conn_ev);
    }*/
    server->conn_ev = conn_ev;
    printf("start server %p \n", server);

    return server;
}

static void StopServer(struct Server* server) {
    rdma_destroy_id(server->listen_id);
    rdma_destroy_event_channel(server->ec);
    free(server);
}