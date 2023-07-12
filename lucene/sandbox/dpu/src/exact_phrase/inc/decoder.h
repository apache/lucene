#ifndef DECODER_H_
#define DECODER_H_

#include <stdint.h>

typedef struct _decoder decoder_t;

decoder_t *initialize_decoder(uintptr_t mram_addr, uint32_t term_id);
void initialize_decoders();
void initialize_decoder_section();

unsigned int decode_vint_from(decoder_t *decoder);
unsigned int decode_byte_from(decoder_t *decoder);
unsigned int decode_short_from(decoder_t *decoder);
int decode_zigzag_from(decoder_t *decoder);
int decode_int_big_endian_from(decoder_t* decoder);
void skip_bytes_to_jump(decoder_t *decoder, uint32_t target_address);
unsigned int get_absolute_address_from(decoder_t *decoder);
bool decoder_is_aligned_with(decoder_t* decoder, const uint8_t* term);

#endif /* DECODER_H_ */
