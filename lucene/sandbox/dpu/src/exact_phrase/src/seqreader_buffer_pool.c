#include "seqreader_buffer_pool.h"
#include <sem.h>
#include <mutex.h>
#include <assert.h>

SEMAPHORE_INIT(pool_sem, NB_SEQ_READERS);
MUTEX_INIT(pool_mutex);

void seqreader_buffer_pool_init(seqreader_buffer_pool_t *pool) {

    for(int i = 0; i < NB_SEQ_READERS; i++) {
        pool->buffers[i] = seqread_alloc();
    }
    pool->index = 0;
}

seqreader_buffer_t seqreader_buffer_pool_get(seqreader_buffer_pool_t *pool) {

    // use a semaphore to ensure that a resource is available or wait otherwise
    sem_take(&pool_sem);

    mutex_lock(pool_mutex);
    assert(pool->index < NB_SEQ_READERS);
    seqreader_buffer_t buffer = pool->buffers[pool->index];
    pool->index++;
    mutex_unlock(pool_mutex);
    return buffer;
}

void seqreader_buffer_pool_put(seqreader_buffer_pool_t *pool, seqreader_buffer_t buffer) {

    mutex_lock(pool_mutex);
    assert(pool->index > 0);
    pool->buffers[pool->index] = buffer;
    pool->index--;
    mutex_unlock(pool_mutex);

    // release the semaphore to indicate that a resource is available
    sem_give(&pool_sem);
}