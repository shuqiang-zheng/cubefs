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

MemoryPool* InitMemoryPool(int block_num) {
    struct ibv_pd* pd = NULL;
    struct ibv_mr* mr = NULL;
    int i = 0;
    int access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;

    MemoryPool * pool = (MemoryPool*)malloc(sizeof(MemoryPool));
    if (pool == NULL) {
        return NULL;
    }
    pool->block_num = block_num;
    pool->block_size = MEMORY_BLOCK_SIZE;
    pool->current_index = 0;
    pthread_mutex_init(&(pool->lock), 0);

    pool->mem = malloc(sizeof(void*) * block_num);
    if (pool->mem == NULL) {
        CloseMemoryPool(pool);
        return NULL;
    }
    memset(pool->mem, 0, sizeof(void*) * block_num);
    for (i = 0; i< block_num; i++) {
        pool->mem[i] = malloc(MEMORY_BLOCK_SIZE);
        if (pool->mem[i] == NULL) {
            CloseMemoryPool(pool);
            return NULL;
        }
    }

    pool->used = malloc(sizeof(int8_t) * block_num);
    if (pool->used == NULL) {
        CloseMemoryPool(pool);
        return NULL;
    }
    memset(pool->used, 0, block_num * sizeof(int8_t));

    pd = mem_pool_alloc_pd();
    if(!pd) {
        return NULL;
    }
    pool->pd = pd;

    pool->mr = malloc(sizeof(struct ibv_mr*) * block_num);
    if (pool->mr == NULL) {
        CloseMemoryPool(pool);
        return NULL;
    }
    memset(pool->mr, 0, sizeof(struct ibv_mr*) * block_num);
    for (i = 0; i< block_num; i++) {
        pool->mr[i] = ibv_reg_mr(pd, pool->mem[i], MEMORY_BLOCK_SIZE, access);
        if (pool->mr[i] == NULL) {
            CloseMemoryPool(pool);
            return NULL;
        }
    }

    return pool;
}

void CloseMemoryPool(MemoryPool* pool) {
    int i = 0;

    if (pool == NULL) {
        return;
    }

    if (pool->mr != NULL) {
        for (i = 0; i< pool->block_num; i++) {
            if (pool->mr[i] != NULL) {
                ibv_dereg_mr(pool->mr[i]);
            }
        }
        free(pool->mr);
    }

    if (pool->pd != NULL) {
        ibv_dealloc_pd(pool->pd);
    }

    if (pool->used != NULL) {
        free(pool->used);
    }

    if (pool->mem != NULL) {
        for (i = 0; i< pool->block_num; i++) {
            if (pool->mem[i] != NULL) {
                free(pool->mem[i]);
            }
        }
        free(pool->mem);
    }

    pthread_mutex_destroy(&(pool->lock));
    free(pool);
}
