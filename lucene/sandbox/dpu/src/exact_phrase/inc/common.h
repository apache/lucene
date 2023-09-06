#ifndef _COMMON_H_
#define _COMMON_H_

/**
 * Maximum number of queries in a batch
 * TODO enforce it on the host
 */
#define DPU_MAX_BATCH_SIZE 128
/**
 * Maximum size in bytes of a batch of queries
 * TODO enforce it on the host
 */
#define DPU_QUERY_BATCH_BYTE_SIZE (1 << 18)
/**
 * Maximum size in bytes of results
 * TODO enforce it on the host
 */
#define DPU_RESULTS_MAX_BYTE_SIZE (1 << 20)
/**
 * Number of results in WRAM cache
 */
#define DPU_RESULTS_CACHE_SIZE (16)
/**
 * Maximum number of terms in a query
 * TODO enforce it on the host
 */
#define MAX_NR_TERMS 32
/**
 * Maximum number of segments in postings
 * TODO enforce it on the host
 */
#define MAX_NR_SEGMENTS 32
/**
 * Number of decoders allocated at init in the decoder pool
 * When not enough decoders are available left in the pool,
 * the thread handling the query will be stopped and resumed
 * when decoders are released by other tasklets
 */
#define NB_DECODERS_FOR_POSTINGS 128
#define NB_DECODERS ((NR_TASKLETS << 1) + NB_DECODERS_FOR_POSTINGS)

/**
 * Encoding for PIM phrase query type
 */
#define PIM_PHRASE_QUERY_TYPE 1

#endif

