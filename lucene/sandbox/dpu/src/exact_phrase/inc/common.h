#ifndef _COMMON_H_
#define _COMMON_H_

#include <attributes.h>
#include <stdint.h>

/**
 * Maximum number of queries in a batch
 * TODO enforce it on the host
 */
#define DPU_MAX_BATCH_SIZE 64
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
 * Maximum number of lucene segments supported
 */
#define DPU_MAX_NR_LUCENE_SEGMENTS 128
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

/*
 * Maximum number of scores stored
 */
#define MAX_NB_SCORES_LOG2 3

/*
 * Number of elements in the norm inverse cache
 */
#define NORM_INV_CACHE_SIZE 256

/*
 * type for storing the quantized values of norm inverse in the index
 */
#define NORM_INV_TYPE uint8_t

/*
 * type for accessing chars in MRAM
 */
typedef __mram_ptr uint8_t *mram_ptr_t;

#undef mram_write_int_atomic
/**
 * @def mram_write_int_atomic
 * @brief write an integer in MRAM atomically (i.e., multi-tasklet safe)
 * @param dest the integer address in MRAM
 * @param val the new integer value
 */
#define mram_write_int_atomic(dest, val)                                                                                         \
                                                                                                                                 \
    do {                                                                                                                         \
        uint16_t __mram_write_int_atomic_hash                                                                                    \
            = (uint16_t)(((uintptr_t)(dest) >> 3U) & ((1U << __MRAM_UNALIGNED_ACCESS_LOG_NB_VLOCK) - 1U));                       \
        uintptr_t __mram_write_int_atomic_dest_low = (((uintptr_t)(dest) >> 3U) << 3U);                                          \
        vmutex_lock(&__mram_unaligned_access_virtual_locks, __mram_write_int_atomic_hash);                                       \
        mram_read(((__mram_ptr void *)(__mram_write_int_atomic_dest_low)), &__mram_unaligned_access_buffer[me() << 3U], 8U);     \
        int __mram_write_int_atomic_val = (val);                                                                                 \
        memcpy(&__mram_unaligned_access_buffer[me() << 3U]                                                                       \
                + ((uintptr_t)(__mram_write_int_atomic_dest_low != (uintptr_t)(dest)) << 2U),                                    \
            &__mram_write_int_atomic_val,                                                                                        \
            4U);                                                                                                                 \
        mram_write(&__mram_unaligned_access_buffer[me() << 3U], ((__mram_ptr void *)(__mram_write_int_atomic_dest_low)), 8U);    \
        vmutex_unlock(&__mram_unaligned_access_virtual_locks, __mram_write_int_atomic_hash);                                     \
    } while (0)

#undef mram_update_int_atomic
/**
 * @def mram_update_int_atomic
 * @brief update an integer in MRAM atomically (i.e., multi-tasklet safe)
 * @param dest the integer address in MRAM
 * @param update_func the pointer to the update function
 * @param args a void* pointer, context passed to the update function
 */
#define mram_update_int_atomic(dest, update_func, args)                                                                          \
                                                                                                                                 \
    do {                                                                                                                         \
        uint16_t __mram_update_int_atomic_hash                                                                                   \
            = (uint16_t)(((uintptr_t)(dest) >> 3U) & ((1U << __MRAM_UNALIGNED_ACCESS_LOG_NB_VLOCK) - 1U));                       \
        uintptr_t __mram_update_int_atomic_dest_low = (((uintptr_t)(dest) >> 3U) << 3U);                                         \
        vmutex_lock(&__mram_unaligned_access_virtual_locks, __mram_update_int_atomic_hash);                                      \
        mram_read(((__mram_ptr void *)(__mram_update_int_atomic_dest_low)), &__mram_unaligned_access_buffer[me() << 3U], 8U);    \
        int __mram_update_int_atomic_val;                                                                                        \
        void *_buffer_ptr = &__mram_unaligned_access_buffer[me() << 3U]                                                          \
            + ((uintptr_t)(__mram_update_int_atomic_dest_low != (uintptr_t)(dest)) << 2U);                                       \
        memcpy(&__mram_update_int_atomic_val, _buffer_ptr, 4U);                                                                  \
        update_func(&__mram_update_int_atomic_val, args);                                                                        \
        memcpy(_buffer_ptr, &__mram_update_int_atomic_val, 4U);                                                                  \
        mram_write(&__mram_unaligned_access_buffer[me() << 3U], ((__mram_ptr void *)(__mram_update_int_atomic_dest_low)), 8U);   \
        vmutex_unlock(&__mram_unaligned_access_virtual_locks, __mram_update_int_atomic_hash);                                    \
    } while (0)

#endif
