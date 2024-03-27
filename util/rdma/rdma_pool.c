#include "rdma_pool.h"
#include "connection_event.h"

int WQ_DEPTH = 32;
int MIN_CQE_NUM = 1024;

struct RdmaPool *rdmaPool = NULL;
struct RdmaPoolConfig *rdmaPoolConfig = NULL;
FILE *fp;

struct RdmaPoolConfig* getRdmaPoolConfig() {
    rdmaPoolConfig = (struct RdmaPoolConfig*)malloc(sizeof(struct RdmaPoolConfig));
    memset(rdmaPoolConfig, 0, sizeof(struct RdmaPoolConfig));
    rdmaPoolConfig->memBlockNum = 8 * 5 * 1024;//
    rdmaPoolConfig->memBlockSize = 65536 * 2;
    rdmaPoolConfig->memPoolLevel = 18;
    rdmaPoolConfig->headerBlockNum = 32 * 1024;
    rdmaPoolConfig->headerPoolLevel = 15;
    rdmaPoolConfig->responseBlockNum = 32 * 1024;
    rdmaPoolConfig->responsePoolLevel = 15;
    rdmaPoolConfig->wqDepth = 32;
    rdmaPoolConfig->minCqeNum = 1024;
    rdmaPoolConfig->enableRdmaLog = 0;
    return rdmaPoolConfig;
}

void destroyRdmaPool() {
    if(rdmaPool == NULL) {
        return;
    }
    if(rdmaPool->memoryPool != NULL) {
        CloseMemoryPool(rdmaPool->memoryPool);
    }
    if(rdmaPool->headerPool != NULL) {
        CloseObjectPool(rdmaPool->headerPool);
    }
    if(rdmaPool->responsePool != NULL) {
        CloseObjectPool(rdmaPool->responsePool);
    }
    free(rdmaPoolConfig);
    free(rdmaPool);
    if (fp) {
        fclose(fp);
    }
}

int initRdmaPool(struct RdmaPoolConfig* config) {
    if(config == NULL) {
        return C_ERR;
    }
    rdmaPoolConfig = config;

    if (rdmaPoolConfig->enableRdmaLog == 1) {
        log_set_level(0);
        log_set_quiet(0);
        fp = fopen("./c_debug.log", "ab");
        if(fp == NULL) {
            return C_ERR;
        }
        log_add_fp(fp, LOG_DEBUG);
    } else {
        log_set_quiet(1);
    }

    WQ_DEPTH = rdmaPoolConfig->wqDepth;
    MIN_CQE_NUM = rdmaPoolConfig->minCqeNum;
    rdmaPool = (struct RdmaPool*)malloc(sizeof(struct RdmaPool));
    memset(rdmaPool, 0, sizeof(struct RdmaPool));
    rdmaPool->memoryPool = InitMemoryPool(rdmaPoolConfig->memBlockNum, 64);
    if(rdmaPool->memoryPool == NULL) {
        goto error;
    }
    rdmaPool->headerPool = InitObjectPool(rdmaPoolConfig->headerBlockNum, getHeaderSize(), rdmaPoolConfig->headerPoolLevel);
    if(rdmaPool->headerPool == NULL) {
        goto error;
    }
    rdmaPool->responsePool = InitObjectPool(rdmaPoolConfig->responseBlockNum, getResponseSize(), rdmaPoolConfig->responsePoolLevel);
    if(rdmaPool->responsePool == NULL) {
        goto error;
    }
    return C_OK;
error:
    destroyRdmaPool();
    return C_ERR;
}
