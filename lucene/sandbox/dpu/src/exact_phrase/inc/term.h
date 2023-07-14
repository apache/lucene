#ifndef TERM_H_
#define TERM_H_

typedef struct _decoder decoder_t;

typedef struct _term {
    decoder_t* term_decoder;
    uint32_t size;
} term_t;

#endif
