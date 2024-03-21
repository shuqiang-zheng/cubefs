#ifndef MEMORY_POOL_H
#define MEMORY_POOL_H

#include <stdlib.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <unistd.h>
#include <pthread.h>

static int MEMORY_BLOCK_SIZE   = 128;
static int MEMORY_BLOCK_COUNT   = 1280;

typedef struct MemoryPool {
    void*  original_mem;
    int64_t size;
    struct ibv_pd* pd;
    struct ibv_mr* mr;
    pthread_mutex_t lock;
    int8_t *used;
    int current_index;
    int block_num;
    int block_size;
} MemoryPool;

MemoryPool* InitMemoryPool(int block_num, int block_size);

void CloseMemoryPool(MemoryPool* pool);

#endif