#ifndef _TERM_LOOKUP_H_
#define _TERM_LOOKUP_H_

#include <built_ins.h>
#include <defs.h>
#include <mram.h>
#include <stdint.h>
#define SEQREAD_CACHE_SIZE 256
#include <seqread.h>

#include "parser.h"
#include "postings_util.h"
#include "term.h"

/**
 * Lookup the addresses needed for a field in the index
 * Provides the address of the norms table and the address of the terms block table
 */
bool
get_field_addresses(uintptr_t index, const term_t *field, uintptr_t *field_norms_address, uintptr_t *field_bt_address);

/**
 * Lookup the postings of a term in the index for all its segments
 */
bool
get_term_postings(uintptr_t field_address, const term_t *term, postings_info_t *postings_for_segments);

#endif
