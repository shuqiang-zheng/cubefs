#ifndef CONNECTION_EVENT_H_
#define CONNECTION_EVENT_H_

//#define MAX_QPS 4
#define C_OK 0
#define C_ERR 1

#include "rdma.h"

/*static void build_connection(struct rdma_cm_id *id, struct ConnectionEvent *conn_ev) {
    struct ibv_device_attr device_attr;
    int ret = ibv_query_device(id->verbs, &device_attr);
    struct ibv_qp_init_attr qp_attr;
    memset(&qp_attr, 0, sizeof qp_attr);
    qp_attr.qp_type = IBV_QPT_RC;
    qp_attr.cap.max_recv_wr = qp_attr.cap.max_send_wr = 2048;//device_attr.max_qp_wr;
    qp_attr.cap.max_send_sge = device_attr.max_sge;
    qp_attr.cap.max_recv_sge = device_attr.max_sge;
    qp_attr.cap.max_inline_data = 16;
    TEST_NZ(rdma_create_qp(id, NULL, &qp_attr));
}*/

static int build_connection(struct rdma_cm_id *cm_id, struct ConnectionEvent *conn_ev) {
    int ret = C_OK;
    struct ibv_device_attr device_attr;
    struct ibv_qp_init_attr init_attr;
    struct ibv_comp_channel *comp_channel = NULL;
    struct ibv_cq *cq = NULL;
    struct ibv_pd *pd = NULL;

    if (ibv_query_device(cm_id->verbs, &device_attr)) {
        //serverLog(LL_WARNING, "RDMA: ibv ibv query device failed");
        //TODO error handler
        return C_ERR;
    }

    pd = ibv_alloc_pd(cm_id->verbs);
    if (!pd) {
        //serverLog(LL_WARNING, "RDMA: ibv alloc pd failed");
        //TODO error handler
        return C_ERR;
    }

    event_ev->ctx->pd = pd;

    comp_channel = ibv_create_comp_channel(cm_id->verbs);
    if (!comp_channel) {
        //serverLog(LL_WARNING, "RDMA: ibv create comp channel failed");
        //TODO error handler
        return C_ERR;
    }

    event->ctx->comp_channel = comp_channel;

    cq = ibv_create_cq(cm_id->verbs, REDIS_MAX_WQE * 2, NULL, comp_channel, redis_rdma_comp_vector % cm_id->verbs->num_comp_vectors);
    if (!cq) {
        //serverLog(LL_WARNING, "RDMA: ibv create cq failed");
        //TODO error handler
        return C_ERR;
    }

    event->ctx->cq = cq;
    ibv_req_notify_cq(cq, 0);

    memset(&init_attr, 0, sizeof(init_attr));
    init_attr.cap.max_send_wr = RDMA_MAX_WQE;
    init_attr.cap.max_recv_wr = RDMA_MAX_WQE;
    init_attr.cap.max_send_sge = device_attr.max_sge;
    init_attr.cap.max_recv_sge = 1;
    init_attr.qp_type = IBV_QPT_RC;
    init_attr.send_cq = cq;
    init_attr.recv_cq = cq;
    ret = rdma_create_qp(cm_id, pd, &init_attr);
    if (ret) {
        //serverLog(LL_WARNING, "RDMA: create qp failed");
        //TODO error handler
        return C_ERR;
    }

/*    if (rdmaSetupIoBuf(ctx, cm_id)) {
        return C_ERR;
    }*/

    return C_OK;
}


static void build_params(struct rdma_conn_param *params){
  memset(params, 0, sizeof(*params));

  params->initiator_depth = params->responder_resources = 1; //指定发送队列的深度
  params->rnr_retry_count = 7; /* infinite retry */
}

typedef void (*PreConnCb)(struct rdma_cm_id *id, void* ctx);
typedef void (*ConnectedCb)(struct rdma_cm_id *id, void* ctx);
typedef void (*DisConnectedCb)(struct rdma_cm_id *id, void* ctx);

struct ConnectionEvent {
    struct rdma_cm_id *cm_id;
    void* ctx;
    PreConnCb preconnect_callback;
    ConnectedCb connected_callback;
    DisConnectedCb disconnected_callback;
};

static const int TIMEOUT_IN_MS = 500;

static void connection_event_cb(void *ctx) {
  struct ConnectionEvent* conn_ev = (struct ConnectionEvent*)ctx;
  struct rdma_cm_event *event = NULL;
  int ret = rdma_get_cm_event(conn_ev->cm_id->channel, &event);
  if (ret < 0) {
      printf("rdma_get_cm_event failed:（%d：%s）。\n",errno,strerror(errno));
      return ;
  }

  if (!ret) {
      if (event->event == RDMA_CM_EVENT_ADDR_RESOLVED) { //client
          build_connection(event->id, conn_ev); //创建qp
          conn_ev->preconnect_callback(event->id, conn_ev, memoryCapacity); //创建connection
          rdma_resolve_route(event->id, TIMEOUT_IN_MS);
      } else if (event->event == RDMA_CM_EVENT_ROUTE_RESOLVED) { //client
          struct rdma_conn_param cm_params;
          build_params(&cm_params);
          rdma_connect(event->id, &cm_params);
      } else if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST) { //server
          build_connection(event->id, conn_ev);
          struct rdma_conn_param cm_params;
          build_params(&cm_params);
          rdma_accept(event->id, &cm_params);

          //可能会有问题，需要后面再检查一下
          struct sockaddr_in *srcAddrIn = (struct sockaddr_in *)event->id->route;
          char *srcIp = Inet_ntoa(srcAddrIn->sin_addr);
          int srcPort = ntohs(srcAddrIn->sin_port);
          char s[20];
          snprintf(s, 20, "%s:%d", srcIp,srcPort);
          string remoteAddr = s;
          conn_ev->ctx->remoteAddr = remoteAddr

          conn_ev->preconnect_callback(event->id, conn_ev);
      } else if (event->event == RDMA_CM_EVENT_ESTABLISHED) {
          conn_ev->connected_callback(event->id, conn_ev->ctx);
      } else if (event->event == RDMA_CM_EVENT_DISCONNECTED) {
          rdma_destroy_qp(event->id);
          conn_ev->disconnected_callback(event->id, conn_ev->ctx);
          rdma_destroy_id(event->id);
      } else {
          printf("unknown event %d \n", event->event);
      }
      rdma_ack_cm_event(event); //ack event
  }
}

/*static int ListenConnectionEvent(int fd, struct ConnectionEvent* conn_ev) {
    printf("ListenConnectionEvent %p fd=\n", conn_ev, fd);
    return epoll_rdma_event_add(fd, POLLIN, conn_ev, connection_event_cb);
}*/


#endif