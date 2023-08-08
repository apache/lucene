#ifndef DECODER_H_
#define DECODER_H_

#include <stdint.h>

/**
 * structure used to read and decode the index
 */
typedef struct _decoder decoder_t;

/**
* The decoders are allocated from a pool.
* Each decoder uses a sequential reader with a buffer in WRAM, hence the
* number of decoders is limited by the size of the WRAM.
* The pool is initialized with a call to initialize_decoder_pool().
* A call to decoder_pool_get() returns a pointer to a decoder.
* When no decoder is available from the pool, the calling thread will stop
* and be resumed once some decoders are released to the pool.
*/

/**
* Initialize the pool of decoders.
*/
void initialize_decoder_pool();

/**
* Get a single decoder from the pool.
*/
decoder_t* decoder_pool_get_one();
/**
* Release a single decoder to the pool.
* Wakes up the threads waiting for decoders.
*/
void decoder_pool_release_one(decoder_t* decoder);
/**
* Get a number of decoders from the pool.
* The next_decoder function is called for each decoder returned from the pool.
* Doing so avoids to allocate an array of decoders to be returned, each decoder
* can be stored by the caller wherever it needs to be stored.
* If less than nb_decoders are available in the pool, the calling thread will stop
* and be resumed once some decoders are released to the pool.
*/
void decoder_pool_get(uint32_t nb_decoders, void(*next_decoder)(decoder_t*, uint32_t, void*), void* ctx);
/**
* Release a number of decoders to the pool.
* Wakes up the threads waiting for decoders.
*/
void decoder_pool_release(uint32_t nb_decoders, decoder_t*(*next_decoder)(uint32_t, void*), void* ctx);

/**
* Initialize a decoder to start decoding at a given MRAM address.
*/
void initialize_decoder(decoder_t* decoder, uintptr_t mram_addr);

/**
* decodes a variable-length integer from the decoder.
*/
unsigned int decode_vint_from(decoder_t *decoder);
/**
* decodes a single byte from the decoder.
*/
unsigned int decode_byte_from(decoder_t *decoder);
/**
* decodes a short from the decoder.
*/
unsigned int decode_short_from(decoder_t *decoder);
/**
* decodes a zigzag-encoded integer from the decoder.
*/
int decode_zigzag_from(decoder_t *decoder);

//int decode_int_big_endian_from(decoder_t* decoder);

/**
* Jump to the target address in the decoder.
*/
void seek_decoder(decoder_t *decoder, uint32_t target_address);

/**
* Skip nb_bytes from the decoder.
*/
void skip_bytes_decoder(decoder_t *decoder, uint32_t nb_bytes);

/**
* Get the absolute address of the current position in the decoder.
*/
unsigned int get_absolute_address_from(decoder_t *decoder);

//bool decoder_is_aligned_with(decoder_t* decoder, const uint8_t* term);

#endif /* DECODER_H_ */
