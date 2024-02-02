#ifndef TERM_H_
#define TERM_H_

#include <stdint.h>

/**
 * Forward declaration of the decoder
 */
typedef struct _decoder decoder_t;

/**
 * Structure to hold a term to be read using a decoder
 */
typedef struct {
    decoder_t *term_decoder;
    uint32_t size;
} term_t;

#endif
