#ifndef __SEQREADER_BUFFER_POOL_H__
#define __SEQREADER_BUFFER_POOL_H__

#define SEQREAD_CACHE_SIZE 64
#include <seqread.h>
#include "common.h"

typedef struct seqreader_buffer_pool {
    seqreader_buffer_t buffers[NB_SEQ_READERS];
    uint32_t index;
} seqreader_buffer_pool_t;

void seqreader_buffer_pool_init(seqreader_buffer_pool_t *pool);
seqreader_buffer_t seqreader_buffer_pool_get(seqreader_buffer_pool_t *pool);
void seqreader_buffer_pool_put(seqreader_buffer_pool_t *pool, seqreader_buffer_t buffer);

#endif

