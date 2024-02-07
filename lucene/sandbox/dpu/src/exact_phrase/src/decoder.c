/*
 * Implementation of exact phrase search algorithm on DPUs.
 */

#include <assert.h>      // for assert
#include <defs.h>        // for me
#include <dpuruntime.h>  // for __resume, __stop
#include <limits.h>      // for CHAR_BIT
#include <mram.h>        // for DPU_MRAM_HEAP_POINTER
#include <mutex.h>       // for mutex_unlock, mutex_lock, MUTEX_INIT
#define SEQREAD_CACHE_SIZE 64
#include <seqread.h>     // for seqread64_get, seqreader_t, seqread_get, seq...
#include <stddef.h>      // for NULL
#include <stdint.h>      // for uint32_t, uint8_t, uintptr_t, uint16_t
#include <string.h>      // for memcpy

#include "common.h"      // for NB_DECODERS, mram_ptr_t

// counting the number of bytes read will make the measure of time to match not accurate
/* #define COUNT_BYTES_READ */

/**
 * Decoder structure: holds a sequential reader to read the index
 */
typedef struct _decoder {
    seqreader_t reader;
    uint8_t *ptr;
#if defined(STATS_ON) & defined(COUNT_BYTES_READ)
    uint32_t nb_bytes_read;
    uint32_t nb_bytes_read_useful;
#endif
} decoder_t;

/**
 * Decoder pool variables
 */
// NOLINTNEXTLINE(misc-misplaced-const)
MUTEX_INIT(decoder_mutex);
static uint32_t decoder_pool_index = 0;
static uint32_t tasklets_sleeping = 0;
static decoder_t decoders[NB_DECODERS];
static decoder_t *decoders_pool[NB_DECODERS];

/**
 * For statistics, counting the number of bytes read from the index
 */
#if defined(STATS_ON) & defined(COUNT_BYTES_READ)
#define READ_BYTE(decoder) decoder->nb_bytes_read_useful++;
#define READ_256_BYTES(decoder) decoder->nb_bytes_read += SEQ_READ_SIZE;

uint32_t
get_bytes_read(decoder_t *decoder)
{
    return decoder->nb_bytes_read;
}
uint32_t
get_bytes_read_useful(decoder_t *decoder)
{
    return decoder->nb_bytes_read_useful;
}

#else
#define READ_BYTE(decoder)
#define READ_256_BYTES(decoder)
uint32_t
get_bytes_read(__attribute__((unused)) decoder_t *decoder)
{
    return 0;
}
uint32_t
get_bytes_read_useful(__attribute__((unused)) decoder_t *decoder)
{
    return 0;
}
#endif

void
seek_decoder(decoder_t *decoder, mram_ptr_t target_address)
{
    uintptr_t prev_mram = decoder->reader.mram_addr;
    decoder->ptr = seqread_seek(target_address, &(decoder->reader));
    if (prev_mram != decoder->reader.mram_addr) {
        READ_256_BYTES(decoder);
    }
}

void
initialize_decoder_pool()
{
    // create the pool of decoders
    for (uint32_t i = 0; i < NB_DECODERS; ++i) {
        decoders[i].ptr = seqread_init(seqread_alloc(), DPU_MRAM_HEAP_POINTER, &(decoders[i].reader));
        decoders_pool[i] = &decoders[i];
    }
    decoder_pool_index = 0;
    tasklets_sleeping = 0;
}

void
initialize_decoder(decoder_t *decoder, mram_ptr_t mram_addr)
{
    seek_decoder(decoder, mram_addr);
}

void
decoder_pool_get(uint32_t nb_decoders, void (*next_decoder)(decoder_t *, uint32_t, void *), void *ctx)
{
    int dec_id = -1;
    if (nb_decoders == 0) {
        return;
    }
    while (dec_id < 0) {
        mutex_lock(decoder_mutex);
        if (decoder_pool_index + nb_decoders - 1 < NB_DECODERS) {
            dec_id = (int)decoder_pool_index;
            for (uint32_t i = 0; i < nb_decoders; ++i) {
                next_decoder(decoders_pool[decoder_pool_index], i, ctx);
                decoders_pool[decoder_pool_index++] = 0;
            }
            mutex_unlock(decoder_mutex);
        } else {
            tasklets_sleeping |= (1U << me());
            // Note: if the stop() is executed between the mutex unlock
            // and the time another tasklet has already tried to wake it up through a resume,
            // this tasklet would never wake up. But this is prevented by the fact that the resume instruction
            // loops over until the tasklet is really sleeping and successfully woken up.
            mutex_unlock(decoder_mutex);
            __stop();
        }
    }
}

static void
next_decoder_get_one(decoder_t *decoder, __attribute__((unused)) uint32_t id, void *ctx)
{
    *(decoder_t **)ctx = decoder;
}

decoder_t *
decoder_pool_get_one()
{
    decoder_t *res = NULL;
    decoder_pool_get(1, next_decoder_get_one, &res);
    return res;
}

void
decoder_pool_release(uint32_t nb_decoders, decoder_t *(*next_decoder)(uint32_t, void *), void *ctx)
{
    mutex_lock(decoder_mutex);
    assert(decoder_pool_index >= nb_decoders);
    for (uint32_t i = 0; i < nb_decoders; ++i) {
        assert(decoders_pool[decoder_pool_index - i - 1] == 0);
        decoders_pool[decoder_pool_index - i - 1] = next_decoder(i, ctx);
    }
    decoder_pool_index -= nb_decoders;
    if (tasklets_sleeping) {
        uint8_t tasklet_id = 0;
        while (tasklets_sleeping) {
            if (tasklets_sleeping & 1U) {
                __resume(tasklet_id, "0");
            }
            tasklet_id++;
            tasklets_sleeping >>= 1U;
        }
    }
    mutex_unlock(decoder_mutex);
}

static decoder_t *
next_decoder_release_one(__attribute__((unused)) uint32_t id, void *ctx)
{
    return (decoder_t *)ctx;
}

void
decoder_pool_release_one(decoder_t *decoder)
{
    decoder_pool_release(1, next_decoder_release_one, decoder);
}

// Computes an absolute address from the current position of this decoder in
// memory.
mram_ptr_t
get_absolute_address_from(decoder_t *decoder)
{
    return seqread_tell(decoder->ptr, &(decoder->reader));
}

void
skip_bytes_decoder(decoder_t *decoder, uint32_t nb_bytes)
{
    mram_ptr_t curr_addr = get_absolute_address_from(decoder);
    seek_decoder(decoder, curr_addr + nb_bytes);
}

// Fetches a variable-length integer value from a parsed buffer.
// Returns the decoded value.
uint32_t
decode_vint_from(decoder_t *decoder)
{
    uint32_t value = 0;
    uint8_t byte = 0;
    uint32_t byte_shift = 0;
    uint8_t *ptr = decoder->ptr;
    uintptr_t prev_mram = decoder->reader.mram_addr;
    const uint32_t nr_bits = 7;
    const uint32_t lsb_mask = (1U << nr_bits) - 1U;
    const uint32_t msb_mask = 1U << nr_bits;

    do {
        byte = *ptr;
        value = value | ((byte & lsb_mask) << byte_shift);
        // value = value | ((byte & (unsigned)INT8_MAX) << byte_shift);
        byte_shift += nr_bits;

        ptr = seqread_get(ptr, sizeof(uint8_t), &(decoder->reader));
        READ_BYTE(decoder);
    } while (byte & msb_mask);

    if (prev_mram != decoder->reader.mram_addr) {
        READ_256_BYTES(decoder);
    }
    decoder->ptr = ptr;
    return value;
}

uint8_t
decode_byte_from(decoder_t *decoder)
{
    uintptr_t prev_mram = decoder->reader.mram_addr;
    uint8_t byte = *(decoder->ptr);
    decoder->ptr = seqread_get(decoder->ptr, sizeof(uint8_t), &(decoder->reader));
    if (prev_mram != decoder->reader.mram_addr) {
        READ_256_BYTES(decoder);
    }
    return byte;
}

uint32_t
decode_short_from(decoder_t *decoder)
{
    uintptr_t prev_mram = decoder->reader.mram_addr;
    uint32_t val = (uint32_t)(decoder->ptr[0] + (decoder->ptr[1] << (sizeof(uint8_t) * CHAR_BIT)));
    decoder->ptr = seqread_get(decoder->ptr, sizeof(uint16_t), &(decoder->reader));
    if (prev_mram != decoder->reader.mram_addr) {
        READ_256_BYTES(decoder);
    }
    return val;
}

uint32_t
decode_int_from(decoder_t *decoder)
{
    uintptr_t prev_mram = decoder->reader.mram_addr;
    uint32_t val = 0;
    memcpy(&val, decoder->ptr, sizeof(uint32_t));
    decoder->ptr = seqread_get(decoder->ptr, sizeof(uint32_t), &(decoder->reader));
    if (prev_mram != decoder->reader.mram_addr) {
        READ_256_BYTES(decoder);
    }
    return val;
}

int
decode_zigzag_from(decoder_t *decoder)
{
    uint32_t i = decode_vint_from(decoder);
    return (int)((i >> 1U) ^ -(i & 1U));
}
