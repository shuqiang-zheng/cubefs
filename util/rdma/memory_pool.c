#include "memory_pool.h"


struct ibv_pd* mem_pool_alloc_pd() {
    struct ibv_device **dev_list = ibv_get_device_list(NULL);
    if(dev_list == NULL) {
        return NULL;
    }
    struct ibv_device *selected_device = dev_list[0];
    ibv_free_device_list(dev_list);
    struct ibv_context *context =  ibv_open_device(selected_device);
    if(context == NULL) {
        return NULL;
    }
    struct ibv_pd *pd = ibv_alloc_pd(context);
    if (!pd) {
        //printf("RDMA: ibv alloc pd failed\n");
        return NULL;
    }
    return pd;
}

int MemoryPool_init_data_buffer(struct DataBuffer *buffer, struct ibv_pd* pd) {
    int access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    int i = 0;

    buffer->mem = malloc(sizeof(void*) * buffer->block_num);
    if (buffer->mem == NULL) {
        return -1;
    }
    memset(buffer->mem, 0, sizeof(void*) * buffer->block_num);
    for (i = 0; i< buffer->block_num; i++) {
        buffer->mem[i] = malloc(buffer->block_size);
        if (buffer->mem[i] == NULL) {
            return -1;
        }
    }

    buffer->used = malloc(sizeof(int8_t) * buffer->block_num);
    if (buffer->used == NULL) {
        return -1;
    }
    memset(buffer->used, 0, buffer->block_num * sizeof(int8_t));

    buffer->mr = malloc(sizeof(struct ibv_mr*) * buffer->block_num);
    if (buffer->mr == NULL) {
        return -1;
    }
    memset(buffer->mr, 0, sizeof(struct ibv_mr*) * buffer->block_num);
    for (i = 0; i< buffer->block_num; i++) {
        buffer->mr[i] = ibv_reg_mr(pd, buffer->mem[i], buffer->block_size, access);
        if (buffer->mr[i] == NULL) {
            return -1;
        }
    }

    return 0;
}

MemoryPool* InitMemoryPool(int block_1M_num, int block_128K_num) {
    struct ibv_pd* pd = NULL;
    int ret = 0;

    MemoryPool * pool = (MemoryPool*)malloc(sizeof(MemoryPool));
    if (pool == NULL) {
        return NULL;
    }
    memset(pool, 0, sizeof(MemoryPool));
    pthread_mutex_init(&(pool->lock), 0);

    pd = mem_pool_alloc_pd();
    if(!pd) {
        CloseMemoryPool(pool);
        return NULL;
    }
    pool->pd = pd;

    pool->buffer_1m.block_num = block_1M_num;
    pool->buffer_1m.block_size = MEMORY_BLOCK_SIZE_1M;
    ret = MemoryPool_init_data_buffer(&(pool->buffer_1m), pd);
    if (ret != 0) {
        CloseMemoryPool(pool);
        return NULL;
    }

    pool->buffer_128k.block_num = block_128K_num;
    pool->buffer_128k.block_size = MEMORY_BLOCK_SIZE_128K;
    ret = MemoryPool_init_data_buffer(&(pool->buffer_128k), pd);
    if (ret != 0) {
        CloseMemoryPool(pool);
        return NULL;
    }

    return pool;
}

void MemoryPool_free_data_buffer(struct DataBuffer *buffer) {
    int i = 0;

    if (buffer->mr != NULL) {
        for (i = 0; i < buffer->block_num; i++) {
            if (buffer->mr[i] != NULL) {
                ibv_dereg_mr(buffer->mr[i]);
            }
        }
        free(buffer->mr);
    }

    if (buffer->used != NULL) {
        free(buffer->used);
    }

    if (buffer->mem != NULL) {
        for (i = 0; i < buffer->block_num; i++) {
            if (buffer->mem[i] != NULL) {
                free(buffer->mem[i]);
            }
        }
        free(buffer->mem);
    }
}

void CloseMemoryPool(MemoryPool* pool) {
    int i = 0;

    if (pool == NULL) {
        return;
    }

    MemoryPool_free_data_buffer(&(pool->buffer_1m));
    MemoryPool_free_data_buffer(&(pool->buffer_128k));

    if (pool->pd != NULL) {
        ibv_dealloc_pd(pool->pd);
    }

    pthread_mutex_destroy(&(pool->lock));
    free(pool);
}
