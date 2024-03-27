#ifndef MEMORY_POOL_H
#define MEMORY_POOL_H

#include <stdlib.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <unistd.h>
#include <pthread.h>

// memory block size is 1MB
static int MEMORY_BLOCK_SIZE   = 1048576;
static int MEMORY_BLOCK_COUNT   = 1024;

typedef struct MemoryPool {
    void** mem;
    struct ibv_pd* pd;
    struct ibv_mr** mr;
    pthread_mutex_t lock;
    int8_t *used;
    int current_index;
    int block_num;
    int block_size;
} MemoryPool;

MemoryPool* InitMemoryPool(int block_num);
void CloseMemoryPool(MemoryPool* pool);

#endif