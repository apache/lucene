/*
 * Implementation of exact phrase search algorithm on DPUs.
 */

#include <attributes.h>
#include <defs.h>
#include <mram.h>
#include <seqread.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <built_ins.h>
#include "common.h"

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

static decoder_t decoders[NR_TASKLETS][MAX_NR_TERMS];

#if defined(STATS_ON) & defined(COUNT_BYTES_READ)
#define READ_BYTE(decoder) decoder->nb_bytes_read_useful++;
#define READ_256_BYTES(decoder) decoder->nb_bytes_read += 256;
uint32_t get_bytes_read(uint32_t term_id, uint32_t segment_id) { return decoders[segment_id][term_id].nb_bytes_read; }
uint32_t get_bytes_read_useful(uint32_t term_id, uint32_t segment_id)
{
    return decoders[segment_id][term_id].nb_bytes_read_useful;
}
#else
#define READ_BYTE(decoder)
#define READ_256_BYTES(decoder)
uint32_t get_bytes_read(__attribute__((unused)) uint32_t term_id, __attribute__((unused)) uint32_t segment_id) { return 0; }
uint32_t get_bytes_read_useful(__attribute__((unused)) uint32_t term_id, __attribute__((unused)) uint32_t segment_id)
{
    return 0;
}
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

void initialize_decoders()
{
    for (int i = 0; i < NR_TASKLETS; ++i) {
        for (int j = 0; j < MAX_NR_TERMS; ++j) {
            decoders[i][j].ptr
                = seqread_init(seqread_alloc(),
                        (__mram_ptr void *)(uintptr_t)DPU_MRAM_HEAP_POINTER, &(decoders[i][j].reader));
        }
    }
}

decoder_t *initialize_decoder(uintptr_t mram_addr, uint32_t term_id)
{
    decoder_t *decoder = &decoders[me()][term_id];
    decoder->ptr = seqread_seek((__mram_ptr void *)mram_addr, &(decoder->reader));
    return decoder;
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

int decode_int_big_endian_from(decoder_t* decoder) {

    int res;
   __builtin_lw_erri("!big", res, decoder->ptr, "0");
    decoder->ptr = seqread_get(decoder->ptr, sizeof(int), &(decoder->reader));
    return res;
}

bool decoder_is_aligned_with(decoder_t* decoder, const uint8_t* term) {
    return (((uintptr_t)term) & 7) == (((uintptr_t)(decoder->ptr) & 7));
}