#ifndef MEMORY_POOL_H_
#define MEMORY_POOL_H_
#include <stdlib.h>

/*static int MEMORY_BLOCK_SIZE   = 128;
static int MEMORY_BLOCK_COUNT   = 1280;
static int RdmaWrite = 0;
static void initParam(int block_size, int block_num, int write) {
    MEMORY_BLOCK_SIZE  = block_size;
    MEMORY_BLOCK_COUNT = 1024+block_num*2;
    RdmaWrite = write;
}*/

/*static int AllocAlignMemory(void** mem_addr) {
    int len = MEMORY_BLOCK_SIZE * MEMORY_BLOCK_COUNT;
    int ret = posix_memalign(mem_addr, sysconf(_SC_PAGESIZE), len);
    return len;
}*/

 typedef struct MemoryEntry_{
    void *buf;
    uint32_t buf_len;

    void *remote_buf;
    uint32_t remote_buf_len;
    //struct MemoryEntry_* next;

    //int index;

    //int free;
} MemoryEntry;

typedef struct {
    uint64_t base_addr;
    uint64_t base_addr_len;
    //uint32_t block_size;
    //uint32_t block_num;
    uint32_t key;
} MemoryRegion;

typedef struct  {
    //MemoryEntry* freeList;
    //MemoryEntry* freeTailList;
    void *original_mem;
    uint32_t original_mem_size;
    char *data_mem;
    uint32_t data_mem_size;
    //int size;
    int* memoryCapacity;

    //void *remote_buf;
    //uint32_t remote_len;

    MemoryRegion* localRegion;
    MemoryRegion* remoteRegion;

    MemoryEntry* entry;
    //MemoryEntry* allList;
} MemoryArea;

static MemoryArea* InitMemoryArea(int memoryCapacity) {
    MemoryArea * memory = (MemoryArea*)malloc(sizeof(MemoryArea));
    memory->original_mem_size = 4 + memoryCapacity + sizeof(MemoryRegion) * 2;

    posix_memalign((void**)&memory->original_mem, sysconf(_SC_PAGESIZE), memory->original_mem_size);

    memory->memoryCapacity = (int*)memory->original_mem;
    *(memory->memoryCapacity) = memoryCapacity
    //memory->localRegion = (MemoryRegion*)memory->original_mem;
    memory->localRegion = (MemoryRegion*)(memory->original_mem+4);
    memory->remoteRegion = memory->localRegion+1;

    memory->data_mem = memory->original_mem + sizeof(MemoryRegion) * 2;
    memory->data_mem_size = memory->original_mem_size-sizeof(MemoryRegion) * 2;

    memory->entry->buf = memory->data_mem
    memory->entry->buf = memory->data_mem_size

    memory->localRegion->base_addr = (uint64_t)memory->data_mem;
    memory->localRegion->base_addr_len = (uint64_t)memory->data_mem_size;
    return memory;
}

/*static MemoryPool* InitMemoryPool(uint32_t block_num, uint32_t block_size) {
    MemoryPool * pool = (MemoryPool*)malloc(sizeof(MemoryPool));
    pool->original_mem_size = block_num * block_size*2 + sizeof(MemoryRegion) * 2;
    posix_memalign((void**)&pool->original_mem, sysconf(_SC_PAGESIZE), pool->original_mem_size);

    pool->localRegion = (MemoryRegion*)pool->original_mem;
    pool->remoteRegion = pool->localRegion+1;

    pool->data_mem = pool->original_mem + sizeof(MemoryRegion) * 2;
    pool->data_mem_size = pool->original_mem_size-sizeof(MemoryRegion) * 2;

    pool->freeList = (MemoryEntry*)malloc(sizeof(MemoryEntry) * block_num);
    pool->allList = pool->freeList;
    MemoryEntry* freeList = pool->freeList;
    for(int i = 0; i < block_num; i++) {
        freeList->buf = pool->data_mem+block_size*i;
        freeList->buf_len = block_size;
        freeList->index = i;
        if (i == block_num -1) {
            freeList->next = NULL;
            pool->freeTailList = freeList;
            break;
        }

        freeList->next = (MemoryEntry*)(freeList+1);
        freeList++;
    }
    pool->size = block_num;

    pool->localRegion->block_size = block_size;
    pool->localRegion->block_num = block_num;
    pool->localRegion->base_addr = (uint64_t)pool->data_mem;
    pool->localRegion->base_addr_len = (uint64_t)pool->data_mem_size;
    return pool;
}*/

static int SetRemoteMemory(MemoryArea* memory, MemoryRegion* remoteRegion) {

    printf("SetRemoteMemory %d %d %ld %d\n", memory->localRegion->base_addr_len, remoteRegion->base_addr_len,
        (uint64_t)memory->localRegion->base_addr, (uint64_t)remoteRegion->base_addr);
    if (memory->localRegion->base_addr_len != remoteRegion->base_addr_len) {
        return -1;
    }

    void *remote_buf = (void*)remoteRegion->base_addr;
    memory->entry->remote_buf = remote_buf;
    memory->entry->remote_buf_len = remoteRegion->base_addr_len;
    printf("remote_buf %p\n", remote_buf);
}

static int SetRemoteMemoryCapacity(MemoryArea* memory, int* remoteMemoryCapacity) {

    printf("SetRemoteMemoryCapacity %d\n", remoteMemoryCapacity);
    *(memory->memoryCapacity) = *(remoteMemoryCapacity)
}

/*static MemoryEntry* GetMemoryEntry(MemoryPool* pool, int index) {
    MemoryEntry*entry = pool->allList+index;
    entry->free = 0;
    return entry;
}*/

/*static MemoryEntry* GetFreeMemoryEntry(MemoryPool* pool) {
    if (pool->freeList == NULL) {
        printf("GetFreeMemoryEntry NULL size=%d\n", pool->size);
        return NULL;
    }

    MemoryEntry* entry = (MemoryEntry*)pool->freeList;
    pool->freeList = entry->next;
    pool->size--;
    entry->next = NULL;
    printf("GetFreeMemoryEntry %d, %p %p\n", pool->size, entry, pool->freeList);

    entry->free = 1;
    if (entry == pool->freeTailList) {
        pool->freeTailList = NULL;
    }
    return entry;
}*/

/*static void FreeMemoryEntry(MemoryPool* pool, MemoryEntry* free) {
    free->free = 0;
    if (pool->freeList == NULL) {
        pool->freeList = free;
        pool->freeTailList = free;
        return;
    }
    pool->size++;
    pool->freeTailList->next = (MemoryEntry*)free;
    pool->freeTailList = free;
    printf("FreeMemoryEntry %d %p %p\n", pool->size, free, pool->freeList);
}*/

#endif