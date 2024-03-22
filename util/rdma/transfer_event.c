#include "transfer_event.h"
#include "connection.h"


int connRdmaHandleRecv(Connection *conn, void *block, uint32_t byte_len) {
    MemoryEntry* entry;
    switch (conn->conntype) {
    case 1:
        log_debug("conn rdma handle recv header\n");
        entry = (MemoryEntry*)malloc(sizeof(MemoryEntry));
        entry->header_buff = block;
        entry->header_len = sizeof(Header);
        entry->isResponse = false;
        int ret = connRdmaRead(conn, block, entry);//rdmaMeta
        if(ret == C_ERR) {
            return C_ERR;
        }
        break;
    case 2:
        log_debug("conn rdma handle recv response\n");
        entry = (MemoryEntry*)malloc(sizeof(MemoryEntry));
        entry->response_buff = block;
        entry->response_len = sizeof(Response);
        entry->isResponse = true;
        if(EnQueue(conn->msgList, entry) == NULL) {
            log_debug("conn msgList enQueue failed, no more memory can be malloced\n");
            //printf("conn msgList enQueue failed, no more memory can be malloced\n");
        };
        notify_event(conn->mFd, 0);
        break;
    default:
        log_debug("RDMA: FATAL error, unknown message\n");
        //printf("RDMA: FATAL error, unknown message\n");
        return C_ERR;
    }
    return C_OK;
}

int connRdmaHandleRead(Connection *conn, MemoryEntry* entry, uint32_t byte_len) {
    if(EnQueue(conn->msgList, entry) == NULL) {
        log_debug("conn msgList enQueue failed, no more memory can be malloced\n");
        //printf("conn msgList enQueue failed, no more memory can be malloced\n");
    };
    //printf("conn msgList enQueue success, waitMsg size: %d\n",GetSize(conn->msgList));
    log_debug("conn msgList enQueue success, waitMsg size: %d\n",GetSize(conn->msgList));
    notify_event(conn->mFd, 0);
    return C_OK;
}

int connRdmaHandleSend(Connection *conn) {
    /* clear cmd and mark this cmd has already sent */
    return C_OK;
}
int transport_sendAndRecv_event_cb(void *ctx) {
    return C_OK;
}

int transport_sendAndRecv_event_handler(Connection *conn) {

    wait_group_add(&(conn->wg),1);
    if(conn->state != CONN_STATE_CONNECTED) {
        goto error;
    }
    struct rdma_cm_id* cm_id = conn->cm_id;
    struct ibv_cq *ev_cq = NULL;
    void *ev_ctx = NULL;
    //struct ibv_wc wcs[32];
    struct ibv_wc wc;
    void *block;
    MemoryEntry *entry;
    int ret;
    /*
    if (ibv_get_cq_event(conn->comp_channel, &ev_cq, &ev_ctx) < 0) {
        //printf("RDMA: get CQ event error");
        goto error;
    }
    ibv_ack_cq_events(conn->cq, 1);
    if (ibv_req_notify_cq(ev_cq, 0)) {
        //printf("RDMA: notify CQ error");
        goto error;
    }
    int ne = 0;
    */
    while((ret = ibv_poll_cq(conn->cq, 1 ,&wc)) == 1) {
        ret = 0;
        if(wc.status != IBV_WC_SUCCESS) {
            goto error;
        }
        switch (wc.opcode) {
          case IBV_WC_RECV:
              log_debug("ibv_wc_recv\n");
              block = (void *)wc.wr_id;
              if (connRdmaHandleRecv(conn, block, wc.byte_len) == C_ERR) {
                  log_debug("rdma recv failed\n");
                  //printf("rdma recv failed");
                  goto error;
              }
              break;

          case IBV_WC_RECV_RDMA_WITH_IMM:
              log_debug("ibv_wc_recv_with_imm\n");
              //printf("ibv_wc_recv_with_imm\n");
              break;
          case IBV_WC_RDMA_READ:
              log_debug("ibv_wc_rdma_read\n");
              entry = (MemoryEntry *)wc.wr_id;
              if (connRdmaHandleRead(conn, entry, wc.byte_len) == C_ERR) {
                  log_debug("rdma read failed\n");
                  //printf("rdma read failed");
                  goto error;
              }
              break;
          case IBV_WC_RDMA_WRITE:
              log_debug("ibv_wc_rdma_write\n");
              //printf("ibv_wc_rdma_write\n");
              break;
          case IBV_WC_SEND:
              log_debug("ibv_wc_send");
              if (connRdmaHandleSend(conn) == C_ERR) {
                  log_debug("rdma send failed\n");
                  //printf("rdma send failed");
                  goto error;
              }
              break;
          default:
              log_debug("RDMA: unexpected opcode 0x[%x]\n", wc.opcode);
              //printf("RDMA: unexpected opcode 0x[%x]\n", wc.opcode);
              goto error;
          }
    }
    if(ret) {
        goto error;
    }
    goto ok;

error:
    wait_group_done(&(conn->wg));
    //DisConnect(conn,true);
    return C_ERR;
ok:
    wait_group_done(&(conn->wg));
    return C_OK;
}

void *cq_thread(void *ctx) {
    Connection* conn = (Connection*)ctx;
	struct ibv_cq *ev_cq;
	void *ev_ctx;
    int ret;
    while(1) {
        pthread_testcancel();
        ret = ibv_get_cq_event(conn->comp_channel, &ev_cq, &ev_ctx);
        if(ret) {
            log_debug("RDMA: get CQ event error\n");
            //printf("RDMA: get CQ event error\n");
            goto error;
        }
        if(ev_cq != conn->cq) {
            goto error;
        }
        ret = ibv_req_notify_cq(conn->cq,0);
        if(ret) {
            log_debug("RDMA: notify CQ error\n");
            //printf("RDMA: notify CQ error");
            goto error;
        }
        ret = transport_sendAndRecv_event_handler(conn);
        ibv_ack_cq_events(conn->cq, 1);
        if (ret == C_ERR) {
            goto error;
        }
    }
error:
    DisConnect(conn,true);
    pthread_exit(NULL);
}


