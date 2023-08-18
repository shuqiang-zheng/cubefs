#include <string>
#include <stdio.h>
#include <sys/socket.h>
#include <netdb.h>
#include "rdma.h"
#include "connection_event.h"

__attribute__((weak))
void HelloClient() {
    puts("HelloClient \n");
}

struct Client {
    struct rdma_cm_id *listen_id;
    struct rdma_event_channel *ec;
    //int    clientId;
    string targetAddr;
    struct ConnectionEvent* conn_ev;
};

static void OnClientReady(int ret, void* ctx) {
    Connection* conn = (Connection*)ctx;
    struct Client* client = (struct Client*)conn->csContext;
    printf("client=%p, conn=%p, %s \n", client, conn, __FUNCTION__);
    ClientConnectionCallback(1, conn);
}


static void OnClientPreConnect(struct rdma_cm_id *id, void* ctx, int memoryCapacity) {
    struct ConnectionEvent* conn_ev = (struct ConnectionEvent*)ctx;
    struct Client* client = conn_ev->conns;
    printf("client=%p, %s \n", client, __FUNCTION__);

    Connection* conn = AllocConnection(id, 2, memoryCapacity);
    conn->agentAddr = client->targetAddr;
    if (ListenTransferRecvEvent(conn) == -1 || ListenTransferSendEvent(conn) == -1) {
         ClientConnectionCallback(0, conn);
         return;
    }
    conn->csContext = client;
    conn->readyCallback = OnClientReady;

    //id->context = conn;
    conn_ev->conns[conn_index] = conn;
    conn_ev->conn_index += 1;
}

static void OnClientConnected(struct rdma_cm_id *id, void* ctx) {
    struct Client* client = (struct Client*)ctx;
    printf("client=%p, %s \n", client, __FUNCTION__);
    sleep(1);
    Connection* conn = (Connection*)id->context;
    SendMemoryCapacity(conn, conn->memory->memoryCapacity);
    SendMemory(conn, conn->memory->localRegion)

    id->context = conn;
}

static void OnClientDisconnected(struct rdma_cm_id *id, void* ctx) {
    struct Client* client = (struct Client*)ctx;
    printf("client=%p, %s \n", client, __FUNCTION__);

    Connection* conn = (Connection*)id->context;
    ClientConnectionCallback(0, conn);
    free(conn);
}

static struct Client* Connect(const char* ip, const char* port, string targetAddr) {

    struct addrinfo *addr;
    struct rdma_cm_id *conn = NULL;
    struct rdma_event_channel *ec = NULL;
    struct rdma_conn_param cm_params;

    printf("ip=%s, port=%s\n", ip, port);
    TEST_NZ_(getaddrinfo(ip, port, NULL, &addr));


    TEST_Z_(ec = rdma_create_event_channel());
    TEST_NZ_(rdma_create_id(ec, &conn, NULL, RDMA_PS_TCP));
    //conn->context = nullptr
    TEST_NZ_(rdma_resolve_addr(conn, NULL, addr->ai_addr, TIMEOUT_IN_MS));

    freeaddrinfo(addr);

    struct Client* client = (struct Client*)malloc(sizeof(struct Client));
    client->listen_id = conn;
    client->ec = ec;
    client->targetAddr = targetAddr;

    struct ConnectionEvent* conn_ev = (struct ConnectionEvent*)malloc(sizeof(struct ConnectionEvent));
    conn_ev->cm_id = conn;
    conn_ev->ctx = client;
    conn_ev->preconnect_callback = OnClientPreConnect;
    conn_ev->connected_callback = OnClientConnected;
    conn_ev->disconnected_callback = OnClientDisconnected;

    client->conn_ev = conn_ev;
    return client;
}

static void DisConnect(struct Client* client) {
    //rdma_destroy_event_channel(ec);
}