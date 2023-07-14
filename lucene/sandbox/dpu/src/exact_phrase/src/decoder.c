/*
 * Implementation of exact phrase search algorithm on DPUs.
 */

#include <attributes.h>
#include <defs.h>
#include <mram.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <built_ins.h>
#define SEQREAD_CACHE_SIZE 64
#include <seqread.h>
#include <mutex.h>
#include <dpuruntime.h>
#include <assert.h>
#include "common.h"
//#include "seqreader_buffer_pool.h"

// counting the number of bytes read will make the measure of time to match not accurate
/* #define COUNT_BYTES_READ */

typedef struct _decoder {
    seqreader_t reader;
    uint8_t *ptr;
#if defined(STATS_ON) & defined(COUNT_BYTES_READ)
    uint32_t nb_bytes_read;
    uint32_t nb_bytes_read_useful;
#endif
} decoder_t;

MUTEX_INIT(decoder_mutex);
uint32_t decoder_pool_index = 0;
uint32_t tasklets_sleeping = 0;
static decoder_t decoders[NB_DECODERS];
static decoder_t* decoders_pool[NB_DECODERS];

#if defined(STATS_ON) & defined(COUNT_BYTES_READ)
#define READ_BYTE(decoder) decoder->nb_bytes_read_useful++;
#define READ_256_BYTES(decoder) decoder->nb_bytes_read += 256;
/*
uint32_t get_bytes_read(uint32_t term_id, uint32_t segment_id) { return decoders[segment_id][term_id].nb_bytes_read; }
uint32_t get_bytes_read_useful(uint32_t term_id, uint32_t segment_id)
{
    return decoders[segment_id][term_id].nb_bytes_read_useful;
}
*/
#else
#define READ_BYTE(decoder)
#define READ_256_BYTES(decoder)
/*
uint32_t get_bytes_read(__attribute__((unused)) uint32_t term_id, __attribute__((unused)) uint32_t segment_id) { return 0; }
uint32_t get_bytes_read_useful(__attribute__((unused)) uint32_t term_id, __attribute__((unused)) uint32_t segment_id)
{
    return 0;
}
*/
#endif

// ============================================================================
// LOAD AND DECODE DATA
// ============================================================================
void skip_bytes_to_jump(decoder_t *decoder, uint32_t target_address)
{
    uintptr_t prev_mram = decoder->reader.mram_addr;
    decoder->ptr = seqread_seek((__mram_ptr void *)target_address, &(decoder->reader));
    if (prev_mram != decoder->reader.mram_addr) {
        READ_256_BYTES(decoder);
    }
}

void initialize_decoder_pool()
{
    // create the pool of decoders
   for (int i = 0; i < NB_DECODERS; ++i) {
           decoders[i].ptr
               = seqread_init(seqread_alloc(),
                       (__mram_ptr void *)(uintptr_t)DPU_MRAM_HEAP_POINTER, &(decoders[i].reader));
           decoders_pool[i] = &decoders[i];
   }
   decoder_pool_index = 0;
   tasklets_sleeping = 0;
}

void initialize_decoder(decoder_t* decoder, uintptr_t mram_addr)
{
    /*decoder->ptr = seqread_seek(seqreader_buffer_pool_get(&pool),
            (__mram_ptr void *)mram_addr, &(decoder->reader));*/
    decoder->ptr = seqread_seek((__mram_ptr void *)mram_addr, &(decoder->reader));
    //return decoder;
}

void decoder_pool_get(uint32_t nb_decoders, void(*next_decoder)(decoder_t*, uint32_t, void*), void* ctx)
{
    int dec_id = -1;
    if(nb_decoders == 0) return;
    while(dec_id < 0) {
        mutex_lock(decoder_mutex);
        if(decoder_pool_index + nb_decoders - 1 < NB_DECODERS) {
            dec_id = decoder_pool_index;
            decoder_pool_index += nb_decoders;
            mutex_unlock(decoder_mutex);
        }
        else {
            tasklets_sleeping &= (1 << me());
            // Note: if the stop() is executed between the mutex unlock
            // and the time another tasklet has already tried to wake it up through a resume,
            // this tasklet would never wake up. But this is prevented by the fact that the resume instruction
            // loops over until the tasklet is really sleeping and successfully woken up.
            mutex_unlock(decoder_mutex);
            __stop();
        }
    }
    for(int i = 0; i < nb_decoders; ++i) {
        next_decoder(decoders_pool[dec_id + i], i, ctx);
    }
}

static void next_decoder_get_one(decoder_t* decoder, uint32_t id, void* ctx) {
    *(decoder_t**)ctx = decoder;
}

decoder_t* decoder_pool_get_one() {

    decoder_t* res;
    decoder_pool_get(1, next_decoder_get_one, &res);
    return res;
}

void decoder_pool_release(uint32_t nb_decoders, decoder_t*(*next_decoder)(uint32_t, void*), void* ctx)
{
    mutex_lock(decoder_mutex);
    assert(decoder_pool_index >= nb_decoders);
    for(int i = 0; i < nb_decoders; ++i) {
        decoders_pool[decoder_pool_index - i - 1] = next_decoder(i, ctx);
    }
    decoder_pool_index-=nb_decoders;
    if(tasklets_sleeping) {
        uint8_t tasklet_id = 0;
        while(tasklets_sleeping) {
            if(tasklets_sleeping & 1)
                __resume(tasklet_id, "0");
            tasklet_id++;
            tasklets_sleeping >>= 1;
        }
    }
    mutex_unlock(decoder_mutex);
}

static decoder_t* next_decoder_release_one(uint32_t id, __attribute__((unused)) void* ctx) {
    return (decoder_t*)ctx;
}

void decoder_pool_release_one(decoder_t* decoder)
{
   decoder_pool_release(1, next_decoder_release_one, decoder);
}

// Computes an absolute address from the current position of this decoder in
// memory.
unsigned int get_absolute_address_from(decoder_t *decoder)
{
    return (unsigned int)seqread_tell(decoder->ptr, &(decoder->reader));
}

// Fetches a variable-length integer value from a parsed buffer.
// Returns the decoded value.
uint32_t decode_vint_from(decoder_t *decoder)
{
    uint32_t value = 0;
    uint8_t byte;
    uint32_t byte_shift = 0;
    uint8_t *ptr = decoder->ptr;
    uintptr_t prev_mram = decoder->reader.mram_addr;

    do {
        byte = *ptr;
        value = value | ((byte & 127) << byte_shift);
        byte_shift += 7;

        ptr = seqread_get(ptr, sizeof(uint8_t), &(decoder->reader));
        READ_BYTE(decoder);
    } while (byte & 128);

    if (prev_mram != decoder->reader.mram_addr) {
        READ_256_BYTES(decoder);
    }
    decoder->ptr = ptr;
    return value;
}

//TODO implement decode_byte_from and decode_short_from
uint32_t decode_byte_from(decoder_t *decoder)
{
    uintptr_t prev_mram = decoder->reader.mram_addr;
    uint32_t byte = *(decoder->ptr);
    decoder->ptr = seqread_get(decoder->ptr, sizeof(uint8_t), &(decoder->reader));
    if (prev_mram != decoder->reader.mram_addr) {
            READ_256_BYTES(decoder);
    }
    return byte;
}

uint32_t decode_short_from(decoder_t *decoder)
{
    uintptr_t prev_mram = decoder->reader.mram_addr;
    uint32_t val = *(uint16_t*)(decoder->ptr);
    decoder->ptr = seqread_get(decoder->ptr, sizeof(uint16_t), &(decoder->reader));
    if (prev_mram != decoder->reader.mram_addr) {
            READ_256_BYTES(decoder);
    }
    return val;
}

int decode_zigzag_from(decoder_t *decoder)
{
    uint32_t i = decode_vint_from(decoder);
    return ((i >> 1) ^ -((int)i & 1));
}

/*
int decode_int_big_endian_from(decoder_t* decoder) {

    int res;
   __builtin_lw_erri("!big", res, decoder->ptr, "0");
    decoder->ptr = seqread_get(decoder->ptr, sizeof(int), &(decoder->reader));
    return res;
}

bool decoder_is_aligned_with(decoder_t* decoder, const uint8_t* term) {
    return (((uintptr_t)term) & 7) == (((uintptr_t)(decoder->ptr) & 7));
}
*/