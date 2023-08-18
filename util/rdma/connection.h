#include <stdio.h>
#include <string.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>

#include "rdma.h"
#include "memory_pool.h"
#ifndef CONNECTION_H_
#define CONNECTION_H_
static const int trace = 0;
#define TRACE_PRINT(fn) if (trace) fn
typedef void (*ConnectionReady)(int, void* conn);

#define RDMA_DEFAULT_DX_SIZE  (1024*1024)
static int rdma_dx_size = RDMA_DEFAULT_DX_SIZE;
#define C_OK 0
#define C_ERR 1
#define RDMA_INVALID_OPCODE 0xffff

typedef struct RdmaResponse {
    uint16_t opcode;
    uint32_t pos;
    uint8_t rsvd[20];
} RdmaResponse;

typedef struct RdmaMemory {
    /* defined as following Opcodes */
    //uint16_t opcode;
    //uint8_t rsvd[14];
    /* address of a transfer buffer which is used to receive remote streaming data,
     * aka 'RX buffer address'. The remote side should use this as 'TX buffer address' */
    uint64_t addr;
    /* length of the 'RX buffer' */
    uint32_t length;
    /* the RDMA remote key of 'RX buffer' */
    uint32_t key;
} RdmaMemory;

typedef struct RdmaKeepalive {
    /* defined as following Opcodes */
    uint16_t opcode;
    uint8_t rsvd[30];
} RdmaKeepalive;

typedef union RdmaMessage {
    uint16_t opcode;
    RdmaKeepalive keepalive;
    RdmaResponse response;
    RdmaMemory memory;
} RdmaMessage;

typedef enum RdmaOpcode {
    RegisterXferMemory = 0,
    ResponseRdmaMessage = 1;
} RdmaOpcode;

typedef enum {
    CONN_STATE_NONE = 0,
    CONN_STATE_CONNECTING,
    CONN_STATE_ACCEPTING,
    CONN_STATE_CONNECTED,
    CONN_STATE_CLOSED,
    CONN_STATE_ERROR
} ConnectionState;

typedef struct RdmaXfer {
    struct ibv_mr *mr; /* memory region of the transfer buffer */
    char *addr;        /* address of transfer buffer in local memory */
    uint32_t length;   /* bytes of transfer buffer */
    uint32_t offset;   /* the offset of consumed transfer buffer */
    uint32_t pos;      /* the position in use of the transfer buffer */
} RdmaXfer;

typedef struct {
    //int     connId;
    //int     agentId;
    string  remoteAddr
    int     conntype;
    void    *buf;
    int     buf_len;

    struct rdma_cm_id * cm_id;
    struct ibv_mr *msg_mr;

    /* DataBuff */
    RdmaXfer dx;
    char *dx_addr;      /* remote transfer buffer address */
    uint32_t dx_key;    /* remote transfer buffer key */
    uint32_t dx_length; /* remote transfer buffer length */
    //uint32_t dx_offset; /* remote transfer buffer offset */
    //uint32_t dx_ops;    /* operations on remote transfer */

    /* CMD 0 ~ RDMA_MAX_WQE for recv buffer
     * RDMA_MAX_WQE ~ 2 * RDMA_MAX_WQE -1 for send buffer */
    RdmaMessage *ctl_buf;
    struct ibv_mr *ctl_mr;

    //MemoryPool* memory_pool;
    //MemoryArea* memory;

    ConnectionReady readyCallback;

    void* csContext;

    ConnectionState state;
} Connection;

/*typedef struct {
    void* g_ctx;
    void* g_buf;
    uint32_t  g_buf_len;
} TransContext;*/

static int rdmaPostRecv(Connection *conn, struct rdma_cm_id *cm_id, RdmaMessage *msg) {
    struct ibv_sge sge;
    size_t length = sizeof(RdmaMessage);
    struct ibv_recv_wr recv_wr, *bad_wr;
    int ret;

    sge.addr = (uint64_t)msg;
    sge.length = length;
    sge.lkey = conn->ctl_mr->lkey;

    recv_wr.wr_id = (uint64_t)msg;
    recv_wr.sg_list = &sge;
    recv_wr.num_sge = 1;
    recv_wr.next = NULL;

    ret = ibv_post_recv(cm_id->qp, &recv_wr, &bad_wr);
    if (ret) {
        //serverLog(LL_WARNING, "RDMA: post recv failed: %d", ret);
        //TODO error handler
        return C_ERR;
    }

    return C_OK;
}

static void *page_aligned_zalloc(size_t size) {
    void *tmp;
    size_t aligned_size, page_size = sysconf(_SC_PAGESIZE);

    aligned_size = (size + page_size - 1) & (~(page_size - 1));
    if (posix_memalign(&tmp, page_size, aligned_size)) {
        serverPanic("posix_memalign failed");
    }

    memset(tmp, 0x00, aligned_size);

    return tmp;
}

static int rdmaSetupIoBuf(Connection *conn, struct ConnectionEvent* conn_ev, struct rdma_cm_id *cm_id) {
    int access = IBV_ACCESS_LOCAL_WRITE;
    size_t length = sizeof(RdmaMessage) * RDMA_MAX_WQE * 2;
    RdmaMessage *msg;
    int i;

    /* setup ctl buf & MR */
    conn->ctl_buf = page_aligned_zalloc(length);
    conn->ctl_mr = ibv_reg_mr(conn_ev->ctx->pd, conn->ctl_buf, length, access);
    if (!conn->ctl_mr) {
        //serverLog(LL_WARNING, "RDMA: reg mr for CMD failed");
        //TODO error handler
        goto destroy_iobuf;
    }

    //
    for (i = 0; i < RDMA_MAX_WQE; i++) {
        msg = conn->ctl_buf + i;

        if (rdmaPostRecv(conn, cm_id, msg) == C_ERR) {
            //serverLog(LL_WARNING, "RDMA: post recv failed");
            //TODO error handler
            goto destroy_iobuf;
        }
    }

    // ListenTransferSendEvent
    for (i = RDMA_MAX_WQE; i < RDMA_MAX_WQE * 2; i++) {
        msg = conn->ctl_buf + i;
        msg->keepalive.opcode = RDMA_INVALID_OPCODE;
    }
    EpollAddSendEvent(conn->cm_id->send_cq_channel->fd, conn);

    /* setup data buf & MR */
    access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    length = rdma_dx_size;
    conn->dx.addr = page_aligned_zalloc(length);
    conn->dx.length = length;
    conn->dx.mr = ibv_reg_mr(conn_ev->ctx->pd, conn->dx.addr, length, access);
    if (!conn->dx.mr) {
        //serverLog(LL_WARNING, "RDMA: reg mr for recv buffer failed");
        //TODO error handler
        goto destroy_iobuf;
    }

    return C_OK;

destroy_iobuf:
    rdmaDestroyIoBuf(conn);
    return C_ERR;
}

static void rdmaDestroyIoBuf(Connection *conn) {
    if (conn->dx.mr) {
        ibv_dereg_mr(conn->dx.mr);
        conn->dx.mr = NULL;
    }

    zlibc_free(conn->dx.addr);
    conn->dx.addr = NULL;

    if (conn->ctl_mr) {
        ibv_dereg_mr(conn->ctl_mr);
        conn->ctl_mr = NULL;
    }

    zlibc_free(conn->ctl_buf);
    conn->ctl_buf = NULL;
}

static Connection* AllocConnection(struct rdma_cm_id * cm_id, struct ConnectionEvent* conn_ev, int conntype) {
    Connection* conn = (Connection*)malloc(sizeof(Connection));
    //conn->memory_pool = InitMemoryPool(MEMORY_BLOCK_COUNT, MEMORY_BLOCK_SIZE);
    //conn->memory = InitMemoryArea(memoryCapacity)
    rdmaSetupIoBuf(conn, conn_ev, cm_id)
    //conn->msg_mr = ibv_reg_mr(cm_id->pd, conn->memory_pool->original_mem, conn->memory_pool->original_mem_size, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    conn->cm_id = cm_id;
    //printf("AllocConnection %ld %ld %ld\n", (uint64_t)conn->memory->original_mem,  (uint64_t)conn->memory->original_mem + conn->memory->original_mem_size,conn->msg_mr->rkey);
    //conn->memory->localRegion->key = conn->msg_mr->rkey;
    //conn->connId = -1;
    conn->conntype = conntype;
    return conn;
}

/*static int SendMemoryCapacity(Connection* conn, int* memoryCapacity) {
    int ret = rdma_post_send(conn->cm_id, memoryCapacity, memoryCapacity, sizeof(int), conn->msg_mr, IBV_SEND_SIGNALED);
    if (ret != 0) {
        return ret;
    }

    TRACE_PRINT(printf("Send memory: conn=%p, cm=%p, ret=%d\n", conn, conn->cm_id,ret));
    return ret;
}*/

static int rdmaSendCommand(Connection *conn, struct rdma_cm_id *cm_id, RdmaMessage *msg) {
    struct ibv_send_wr send_wr, *bad_wr;
    struct ibv_sge sge;
    RdmaMessage *_msg;
    int i, ret;

    /* find an unused cmd buffer */
    for (i = RDMA_MAX_WQE; i < 2 * RDMA_MAX_WQE; i++) {
        _msg = conn->ctl_buf + i;
        if (_msg->keepalive.opcode == RDMA_INVALID_OPCODE) {
            break;
        }
    }

    assert(i < 2 * RDMA_MAX_WQE);

    memcpy(_msg, msg, sizeof(RdmaMessage));
    sge.addr = (uint64_t)_msg;
    sge.length = sizeof(RdmaMessage);
    sge.lkey = conn->ctl_mr->lkey;

    send_wr.sg_list = &sge;
    send_wr.num_sge = 1;
    send_wr.wr_id = (uint64_t)_msg;
    send_wr.opcode = IBV_WR_SEND;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.next = NULL;
    ret = ibv_post_send(cm_id->qp, &send_wr, &bad_wr);
    if (ret) {
        //serverLog(LL_WARNING, "RDMA: post send failed: %d", ret);
        //TODO error handler
        return C_ERR;
    }

    return C_OK;
}

static int connRdmaRegisterRx(Connection *conn, struct rdma_cm_id *cm_id) {
    RdmaMessage msg;

    msg.memory.opcode = htons(RegisterXferMemory); // TODO maybe modify
    msg.memory.addr = htonu64((uint64_t)conn->dx.addr);
    msg.memory.length = htonl(conn->dx.length);
    msg.memory.key = htonl(conn->dx.mr->rkey);

    conn->dx.offset = 0;
    conn->dx.pos = 0;

    return rdmaSendCommand(conn, cm_id, &msg);
}

/*static int SendMemory(Connection* conn, MemoryRegion* region) {
    int ret = rdma_post_send(conn->cm_id, region, region, sizeof(MemoryRegion), conn->msg_mr, IBV_SEND_SIGNALED);
    if (ret != 0) {
        return ret;
    }

    TRACE_PRINT(printf("Send memory: conn=%p, cm=%p, ret=%d\n", conn, conn->cm_id,ret));
    return ret;
}*/

static size_t RdmaWriteWithImm(Connection *conn, const void *data, size_t data_len) { //TODO need to modify
    //rdma_connection *rdma_conn = (rdma_connection *)conn;
    struct rdma_cm_id *cm_id = conn->cm_id;
    //RdmaContext *ctx = cm_id->context;
    struct ibv_send_wr send_wr, *bad_wr;
    struct ibv_sge sge;
    uint32_t off = conn->dx.offset;
    char *addr = conn->fx.addr + off;
    char *remote_addr = conn->dx_addr + conn->dx.offset;
    int ret;

    memcpy(addr,&data_len,4);
    memcpy(addr+4, data, data_len);

    sge.addr = (uint64_t)addr;
    sge.lkey = conn->dx.mr->lkey;
    sge.length = data_len+4;

    send_wr.sg_list = &sge;
    send_wr.num_sge = 1;
    send_wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
    //send_wr.send_flags = (++conn->dx.ops % (RDMA_MAX_WQE / 2)) ? 0 : IBV_SEND_SIGNALED;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.imm_data = htonl(conn->dx.offset);
    send_wr.wr.rdma.remote_addr = (uint64_t)remote_addr;
    send_wr.wr.rdma.rkey = conn->dx_key;
    send_wr.wr_id = 0;
    send_wr.next = NULL;
    ret = ibv_post_send(cm_id->qp, &send_wr, &bad_wr);
    if (ret) {
        //serverLog(LL_WARNING, "RDMA: post send failed: %d", ret);
        //TODO error handler
        conn->state = CONN_STATE_ERROR;
        return C_ERR;
    }

    conn->dx.offset += (data_len+4);

    return data_len;
}

static int connRdmaWriteWithImm(Connection *conn, const void *data, size_t data_len) { //非异步
    //rdma_connection *rdma_conn = (rdma_connection *)conn;
    struct rdma_cm_id *cm_id = conn->cm_id;
    //RdmaContext *ctx = cm_id->context;
    uint32_t towrite;

    if (conn->state == CONN_STATE_ERROR || conn->state == CONN_STATE_CLOSED) {
        return C_ERR;
    }

    if (conn->dx.pos <= conn->dx.offset) {
        if (conn->dx.offset + 4 + data_len > conn->dx.length) {
            conn->dx.offset = 0;
            while (conn->dx.offset + data_len >= conn->dx.pos) {
                //TODO wait
            }
        }
    } else {
        while(conn->dx.offset + data_len + 4 >= conn->dx.pos) {
            //TODO wait
        }
    }

/*    towrite = MIN(ctx->tx.length - ctx->tx.offset, data_len);
    if (!towrite) {
        return 0;
    }*/

    return RdmaWriteWithImm(conn, data, data_len);
}

static uint32_t rdmaRead(Connection *conn, void *buf, uint32_t baseAddrOffset) {
    uint32_t data_len;
    memcpy(&data_len,conn->dx.addr + baseAddrOffset,4);

    //toread = MIN(conn->dx.offset - conn->dx.pos, buf_len);

    if(conn->dx.pos < conn->dx.offset) {
        assert(baseAddrOffset + data_len + 4 <= conn->dx.offset);
    } else if (conn->dx.pos > conn->dx.offset) {
        if (baseAddrOffset > conn->dx.pos) {
            assert(baseAddrOffset + data_len + 4 <= conn->dx.length)
        } else {
            assert(baseAddrOffset + data_len + 4 <= conn->dx.offset)
        }
    }

    memcpy(buf, conn->dx.addr + baseAddrOffset + 4, data_len);

    conn->dx.pos = baseAddrOffset + data_len + 4;

    return data_len;
}

static int connRdmaRead(Connection *conn, void *buf, uint32_t baseAddrOffset) {
    //rdma_connection *rdma_conn = (rdma_connection *)conn;
    struct rdma_cm_id *cm_id = conn->cm_id;
    //RdmaContext *ctx = cm_id->context;

    if (conn->state == CONN_STATE_ERROR || conn->state == CONN_STATE_CLOSED) {
        return C_ERR;
    }

    if (conn->dx.pos < conn->dx.offset) {
        assert(conn->dx.pos < baseAddrOffset && baseAddrOffset < conn->dx.offset);
    } else if (conn->dx.pos > conn->dx.offset){
        assert(conn->dx.pos < baseAddrOffset || baseAddrOffset < conn->dx.offset);
    } else { //No more data to read
        return -1;
    }

    return rdmaRead(conn, buf, baseAddrOffset);
}

/*static int Send(Connection* conn, MemoryEntry* entry) {
    int ret = rdma_post_send(conn->cm_id, entry, entry->buf, entry->buf_len, conn->msg_mr, IBV_SEND_SIGNALED);
    if (ret != 0) {
        return ret;
    }

    TRACE_PRINT(printf("Send: conn=%p, cm=%p, entry=%p,ret=%d\n", conn, conn->cm_id, entry, ret));
    return ret;
}*/

/*static int Recv(Connection* conn, char* buf, int len) {
    return -1;
}*/

/*static int write_remote(struct rdma_cm_id *id, void *context, void *addr,
                  size_t len, struct ibv_mr *mr, int flags,
                  uint64_t remote_addr, uint32_t rkey, int index, int immi) {
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    memset(&wr, 0, sizeof(wr));

    wr.wr_id = (uintptr_t)context;
    wr.opcode = immi ? IBV_WR_RDMA_WRITE_WITH_IMM : IBV_WR_RDMA_WRITE;
    wr.send_flags = flags;
    wr.imm_data = index;
    wr.wr.rdma.remote_addr = remote_addr;
    wr.wr.rdma.rkey = rkey;

    if (len) {
        wr.sg_list = &sge;
        wr.num_sge = 1;

        sge.addr = (uintptr_t)addr;
        sge.length = len;
        sge.lkey = mr->lkey;
    }

    return ibv_post_send(id->qp, &wr, &bad_wr);
}*/

/*static int Write(Connection* conn, MemoryEntry* entry) {
    int ret = write_remote(conn->cm_id, entry, entry->buf, entry->buf_len, conn->msg_mr,
        IBV_SEND_SIGNALED, (uint64_t)entry->remote_buf, conn->memory_pool->remoteRegion->key, 0, 0);
    if (ret != 0) {
        return ret;
    }

    TRACE_PRINT(printf("write: conn=%p, cm=%p, entry=%p,ret=%d\n", conn, conn->cm_id, entry, ret));
    return ret;
}*/

/*static int WriteImm(Connection* conn, MemoryEntry* entry, int len) {
    if (len == -1) {
        len = entry->buf_len;
    }
    int ret = write_remote(conn->cm_id, entry, entry->buf, len, conn->msg_mr,
        IBV_SEND_SIGNALED, (uint64_t)entry->remote_buf, conn->memory_pool->remoteRegion->key, entry->index, 1);
    if (ret != 0) {
        return ret;
    }

    TRACE_PRINT(printf("write imm: conn=%p, cm=%p, entry=%p,%ld, ret=%d %d\n", conn, conn->cm_id, entry, (uint64_t)entry->remote_buf, ret, conn->memory_pool->remoteRegion->key));
    return ret;
}*/

/*static MemoryEntry* Malloc(Connection* conn, int len) {
    MemoryEntry* entry = GetFreeMemoryEntry(conn->memory_pool);
    return entry;
}*/

#endif