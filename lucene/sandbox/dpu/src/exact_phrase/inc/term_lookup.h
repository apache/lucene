#ifndef _TERM_LOOKUP_H_
#define _TERM_LOOKUP_H_

#include <stdint.h>
#include <defs.h>
#include <mram.h>
#define SEQREAD_CACHE_SIZE 256
#include <seqread.h>
#include <built_ins.h>
#include "parser.h"
#include "term.h"

bool get_field_address(uintptr_t index, const term_t* field, uintptr_t* field_address);

bool get_term_postings(uintptr_t field_address,
                        const term_t* term, uintptr_t* postings_address,
                        uint32_t* postings_byte_size);

#endif
