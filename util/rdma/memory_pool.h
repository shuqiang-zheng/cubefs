#ifndef MEMORY_POOL_H
#define MEMORY_POOL_H

#include <stdlib.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <unistd.h>
#include <pthread.h>

// memory block size is 1MB
static int MEMORY_BLOCK_SIZE_1M   = 1048576;
static int MEMORY_BLOCK_SIZE_128K   = 131072;
static int MEMORY_BLOCK_COUNT   = 1024;

struct DataBuffer {
    int block_num;
    int block_size;
    void** mem;
    struct ibv_mr** mr;
    int8_t *used;
    int current_index;
};

typedef struct MemoryPool {
    pthread_mutex_t lock;
    struct ibv_pd* pd;
    struct DataBuffer buffer_1m;
    struct DataBuffer buffer_128k;
} MemoryPool;

MemoryPool* InitMemoryPool(int block_1M_num, int block_128K_num);
void CloseMemoryPool(MemoryPool* pool);

#endif