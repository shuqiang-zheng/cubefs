#include <stdio.h>
#include <string.h>
#include "rdma.h"
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>

#ifndef TRANSFER_EVENT_H_
#define TRANSFER_EVENT_H_

typedef void (*CompleteCb)(struct rdma_cm_id *id, void* ctx);
typedef void (*EPoolCb)(void* ctx);

extern int RecvMessageCallback(MemoryEntry*, Connection*);

/*extern void EpollAddSendEvent(int, void*);
extern void EpollAddRecvEvent(int, void*);*/
extern void EpollAddSendAndRecvEvent(int, void*);

#define C_OK 0
#define C_ERR 1

/*static int post_receive(struct rdma_cm_id *id) {
    struct ibv_recv_wr wr, *bad_wr = NULL;

    memset(&wr, 0, sizeof(wr));

    wr.wr_id = (uintptr_t)id;
    wr.sg_list = NULL;
    wr.num_sge = 0;

    ibv_post_recv(id->qp, &wr, &bad_wr);
}*/

/*static void transport_recv_event_cb(void *ctx) {
    Connection* conn = ctx;
    struct rdma_cm_id* cm_id = conn->cm_id;

    struct ibv_wc wcs[32];
    struct ibv_cq *ev_cq;
    void *ev_ctx;
    int ret = ibv_get_cq_event(cm_id->recv_cq_channel, &ev_cq, &ev_ctx); //获取completion queue event。对于epoll水平触发模式，必须要执行ibv_get_cq_event并将该cqe取出，否则会不断重复唤醒epoll
    if (ret) {
        fprintf(stderr, "Failed to get cq_event\n");
        return ;
    }
    assert(ev_cq == cm_id->recv_cq);
    ibv_ack_cq_events(ev_cq, 1); //ack cqe

    ret = ibv_req_notify_cq(cm_id->recv_cq, 0);
    if (ret) {
        fprintf(stderr, "Couldn't request CQ notification\n");
        return;
    }

    int ne = 0;
    do {
        ne = ibv_poll_cq(ev_cq, 32, wcs); //从完成队列 (Completion Queue) 中获取完成事件
        if (ne < 0) {
            fprintf(stderr, "Failed to poll completions from the CQ: ret = %d\n",
                    ne);
            return;
        }
        *//* there may be an extra event with no completion in the CQ *//*
        if (ne == 0)
            continue;

        for (int i = 0; i < ne; ++i) {

            if ((MemoryRegion*)wcs[i].wr_id == conn->memory->remoteRegion) {
                printf("recv remote mem region %d\n", wcs[i].status);
                if (wcs[i].status == IBV_WC_SUCCESS) {
                    SetRemoteMemory(conn->memory_pool, (MemoryRegion*)wcs[i].wr_id);
                    conn->readyCallback(0, conn);
                } else {
                    conn->readyCallback(-1, conn);
                }
                continue;
            } else if ((int*)wcs[i].wr_id == conn->memory->memoryCapacity) {
                print("recv remote mem capacity %d\n", wcs[i].status);
                if (wcs[i].status == IBV_WC_SUCCESS) {
                    SetRemoteMemoryCapacity(conn->memory, (int*)wcs[i].wr_id);
                    //TODO
                    //conn->readyCallback(0, conn);
                } else {
                    conn->readyCallback(-1, conn); //TODO
                }
                continue;
            }

            if (wcs[i].status != IBV_WC_SUCCESS) {
                printf("recv Completion with status=%d, opcode=%d conn=%p\n", wcs[i].status, wcs[i].opcode, conn);
                RecvMessageCallback(NULL, NULL);
            } else {
                if (wcs[i].opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
                    int ret = post_receive(cm_id);
                    if (ret != 0) {
                        fprintf(stderr, "recv Completion with status %d \n", wcs[i].status);
                    }
                    //TODO
                    //MemoryEntry* entry = GetMemoryEntry(conn->memory_pool, wcs[i].imm_data);
                    TRACE_PRINT(printf("recv Completion with status=%d, opcode=%d index=%d entry=%p\n", wcs[i].status, wcs[i].opcode, wcs[i].imm_data, entry));
                    RecvMessageCallback(entry,  conn);
                } else {
                    MemoryEntry* entry = (MemoryEntry*)wcs[i].wr_id;
                    TRACE_PRINT(printf("recv Completion with status=%d, opcode=%d entry=%p\n", wcs[i].status, wcs[i].opcode, entry));
                    RecvMessageCallback(entry, conn);
                    int ret = rdma_post_recv(cm_id, entry, (void*)entry->buf, entry->buf_len, conn->msg_mr);
                    if (ret != 0) {
                        fprintf(stderr, "recv Completion with status %d \n",wcs[i].status);
                    }
                }
            }

        }
    } while (ne);
}*/

/*static void transport_send_event_cb(void *ctx) {
    Connection* conn = ctx;
    struct rdma_cm_id* cm_id = conn->cm_id;

    struct ibv_wc wcs[32];
    struct ibv_cq *ev_cq;
    void *ev_ctx;
    int ret = ibv_get_cq_event(cm_id->send_cq_channel, &ev_cq, &ev_ctx); //获取completion queue event。对于epoll水平触发模式，必须要执行ibv_get_cq_event并将该cqe取出，否则会不断重复唤醒epoll
    if (ret) {
        fprintf(stderr, "Failed to get cq_event\n");
        return ;
    }
    assert(ev_cq == cm_id->send_cq);
    ibv_ack_cq_events(ev_cq, 1); //ack cqe

    ret = ibv_req_notify_cq(cm_id->send_cq, 0);
    if (ret) {
        fprintf(stderr, "Couldn't request CQ notification\n");
        return;
    }

    int ne = 0;
    do {
        ne = ibv_poll_cq(ev_cq, 32, wcs);
        if (ne < 0) {
            fprintf(stderr, "Failed to poll completions from the CQ: ret = %d\n",
                    ne);
            return;
        }
        *//* there may be an extra event with no completion in the CQ *//*
        if (ne == 0)
            continue;

        for (int i = 0; i < ne; ++i) {
            if ((MemoryRegion*)wcs[i].wr_id == conn->memory->localRegion) {
                printf("send MemoryRegion Completion with status=%d, opcode=%d conn=%p\n", wcs[i].status, wcs[i].opcode, conn);
                continue;
            } else if((int*)wcs[i].wr_id == conn->memory->memoryCapacity) {
                printf("send MemoryCapacity Completion with status=%d, opcode=%d conn=%p\n", wcs[i].status, wcs[i].opcode, conn);
                continue;
            }

            if (wcs[i].status != IBV_WC_SUCCESS) {
                printf("send Completion with status=%d, opcode=%d conn=%p\n", wcs[i].status, wcs[i].opcode, conn);
            }
            MemoryEntry* entry = (MemoryEntry*)wcs[i].wr_id;
            TRACE_PRINT(printf("send Completion with status=%d, opcode=%d entry=%p\n", wcs[i].status, wcs[i].opcode, entry));
            *//*if (entry->free) {
                FreeMemoryEntry(conn->memory_pool, entry);
            }*//*
        }
    } while (ne);
}*/

static int connRdmaHandleRecv(Connection *conn, struct rdma_cm_id *cm_id, RdmaMessage *msg, uint32_t byte_len) {
    if (unlikely(byte_len != sizeof(RdmaMessage))) {
        //serverLog(LL_WARNING, "RDMA: FATAL error, recv corrupted cmd");
        //TODO error handler
        return C_ERR;
    }

    switch (ntohs(msg->opcode)) {
    case RegisterXferMemory:
        conn->dx_addr = (char *)ntohu64(msg->memory.addr);
        conn->dx.length = ntohl(msg->memory.length);
        conn->dx_key = ntohl(msg->memory.key);
        conn->dx.offset = 0;
        conn->readyCallback(0, conn)
        //rdmaAdjustSendbuf(ctx, ctx->tx.length);
        break;

    case ResponseRdmaMessage:
        conn->dx.pos = msg->response.pos
        if(msg->response.opcode == 200) {
            // success, callback
        } else {
            // fail, callback
        }

    default:
        //serverLog(LL_WARNING, "RDMA: FATAL error, unknown cmd");
        //TODO error handler
        return C_ERR;
    }

    return rdmaPostRecv(onn, cm_id, msg);
}

static int connRdmaHandleRecvImm(Connection *conn, struct rdma_cm_id *cm_id, RdmaMessage *msg, uint32_t addrOffset) {

    uint32_t *base_addr = (uint32_t)conn->dx.addr + addrOffset;
    uint32_t byte_len = *(base_addr)

    assert(byte_len + addrOffset <= conn->dx.length);

    conn->dx.offset = addrOffset + byte_len;
    //TODO can read(addrOffset,conn->dx.offset)

    return rdmaPostRecv(conn, cm_id, msg);
}

static int connRdmaHandleWrite(Connection *conn, uint32_t byte_len) {
    //UNUSED(ctx);
    //UNUSED(byte_len);

    return C_OK;
}

static int connRdmaHandleSend(RdmaMessage *msg) {
    /* clear cmd and mark this cmd has already sent */
    memset(cmd, 0x00, sizeof(*msg));
    msg->keepalive.opcode = RDMA_INVALID_OPCODE;

    return C_OK;
}

static int transport_sendAndRecv_event_cb(void *ctx) {
    Connection* conn = ctx;
    struct rdma_cm_id* cm_id = conn->cm_id;
    //RdmaContext *ctx = conn->csContext;
    struct ibv_cq *ev_cq = NULL;
    void *ev_ctx = NULL;
    struct ibv_wc wc = {0};
    RdmaMessage *msg;
    int ret;

    if (ibv_get_cq_event(conn->csContext->comp_channel, &ev_cq, &ev_ctx) < 0) {
        //serverLog(LL_WARNING, "RDMA: get CQ event error");
        //TODO error handler
        return C_ERR;
    } else if (ibv_req_notify_cq(ev_cq, 0)) {
        //serverLog(LL_WARNING, "RDMA: notify CQ error");
        //TODO error handler
        return C_ERR;
    }

pollcq:
    ret = ibv_poll_cq(conn->csContext->cq, 1, &wc);
    if (ret < 0) {
        //serverLog(LL_WARNING, "RDMA: poll recv CQ error");
        //TODO error handler
        return C_ERR;
    } else if (ret == 0) {
        return C_OK;
    }

    ibv_ack_cq_events(conn->csContext->cq, 1);

    if (wc.status != IBV_WC_SUCCESS) {
        //serverLog(LL_WARNING, "RDMA: CQ handle error status: %s[0x%x], opcode : 0x%x", ibv_wc_status_str(wc.status), wc.status, wc.opcode);
        //TODO error handler
        return C_ERR;
    }

    switch (wc.opcode) {
    case IBV_WC_RECV:
        msg = (RdmaMessage *)wc.wr_id;
        if (connRdmaHandleRecv(conn, cm_id, msg, wc.byte_len) == C_ERR) {
            return C_ERR;
        }
        break;

    case IBV_WC_RECV_RDMA_WITH_IMM:
        msg = (RdmaMessage *)wc.wr_id;
        if (connRdmaHandleRecvImm(conn, cm_id, msg, ntohl(wc.imm_data)) == C_ERR) {
            //rdma_conn->c.state = CONN_STATE_ERROR;
            //TODO error handler
            return C_ERR;
        }

        break;
    case IBV_WC_RDMA_WRITE:
        if (connRdmaHandleWrite(conn, wc.byte_len) == C_ERR) {
            //TODO error handler
            return C_ERR;
        }

        break;

    case IBV_WC_SEND:
        msg = (RdmaMessage *)wc.wr_id;
        if (connRdmaHandleSend(msg) == C_ERR) {
            //TODO error handler
            return C_ERR;
        }

        break;

    default:
        //serverLog(LL_WARNING, "RDMA: unexpected opcode 0x[%x]", wc.opcode);
        //TODO error handler
        return C_ERR;
    }

    //goto pollcq;
    return C_OK;
}


//static const int IO_DEEP = 128;
/*static int ListenTransferRecvEvent(Connection* conn) {
    //recv mem region
    printf("ListenTransferRecvEvent remote mem capacity %p %p\n", (MemoryRegion*)conn->memory->memoryCapacity, conn->memory->memoryCapacity);
        int ret = rdma_post_recv(conn->cm_id, conn->memory->memoryCapacity, conn->memory->memoryCapacity, sizeof(int), conn->msg_mr);
        if (ret != 0) {
            printf("rdma_post_recv %d\n", ret);
            return -1;
    }

    //recv mem region
    printf("ListenTransferRecvEvent remote mem region %p %p\n", (MemoryRegion*)conn->memory->remoteRegion, conn->memory->remoteRegion);
    int ret = rdma_post_recv(conn->cm_id, conn->memory->remoteRegion, conn->memory->remoteRegion, sizeof(MemoryRegion), conn->msg_mr);
    if (ret != 0) {
        printf("rdma_post_recv %d\n", ret);
        return -1;
    }

    int index = 0;
    while (index < IO_DEEP) {
        if (RdmaWrite == 0) {
            MemoryEntry* entry = GetFreeMemoryEntry(conn->memory_pool);
            ret = rdma_post_recv(conn->cm_id, entry, entry->buf, entry->buf_len, conn->msg_mr);
            printf("rdma_post_recv %p %d\n", entry, entry->buf_len);
            if (ret != 0) {
                return -1;
            }
        } else {
            ret = post_receive(conn->cm_id);
            if (ret != 0) {
                fprintf(stderr, "post_receive %d \n", ret);
            }
        }
        index++;
    }

    ret = ibv_req_notify_cq(conn->cm_id->recv_cq, 0);
    if (ret) {
        fprintf(stderr, "Couldn't request CQ notification\n");
        return ret;
    }

    printf("ListenTransferRecvEvent %p fd=%d\n", conn, conn->cm_id->recv_cq_channel->fd);
    EpollAddRecvEvent(conn->cm_id->recv_cq_channel->fd, conn); return 0;
    //return epoll_rdma_event_add(conn->cm_id->recv_cq_channel->fd, POLLIN, conn, transport_recv_event_cb);
}*/

/*
static int ListenTransferSendEvent(Connection* conn) {
    int ret = ibv_req_notify_cq(conn->cm_id->send_cq, 0);
    if (ret) {
        fprintf(stderr, "Couldn't request CQ notification\n");
        return ret;
    }

    printf("ListenTransferSendEvent %p fd=%d\n", conn, conn->cm_id->send_cq_channel->fd);
    EpollAddSendEvent(conn->cm_id->send_cq_channel->fd, conn); return 0;
    //return epoll_rdma_event_add(conn->cm_id->send_cq_channel->fd, POLLIN, conn, transport_send_event_cb);
}*/
#endif