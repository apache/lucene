#include "term_lookup.h"
#include "decoder.h"

/**
 * structure to hold the information for a block
 * in the block table (field or query term)
 */
typedef struct _block {
    uintptr_t term;
    uint32_t term_size;
    uint32_t block_address;
    uint32_t block_size;
} block_t;

static block_t term_blocks[NR_TASKLETS];

/* offsets to different sections in the index */
static uintptr_t index_begin_addr = 0;
static uint32_t block_table_offset = 0;
static uint32_t block_list_offset = 0;
static uint32_t postings_offset = 0;
static uint32_t nr_segments = 0;

// compare terms, return 0 if equal, < 0 if term1 < term2, > 0 if term1 > term2
static int compare_terms(decoder_t* decoder_term1, int32_t term1_length,
                            decoder_t* decoder_term2, int32_t term2_length) {
    int term1_w, term2_w;
    int offset = 0;
    /* TODO implement this optim if valuable
    // terms are encoded as UTF-8, so we can compare the bytes directly
    // perform the comparison 4B per 4B if the terms have the same alignment modulo 8
    if(decoder_is_aligned_with(decoder_term1, decoder_term2)) {
        // Need to be cautious of the endianness when loading as integer
        // Perform a load as big endian while the DPU is little-endian, hence using a builtin
        // Also need a prelude to align
        int realign = ((uintptr_t)term1) & 7;
        for(; offset < realign && offset < term1_length && offset < term2_length; offset++) {
            uint8_t term2 = decode_byte_from(decoder_term2);
            if(term1[offset] != term2) {
                return term1[offset] - term2;
            }
        }
        for(; (offset + 4) < term1_length && (offset + 4) < term2_length; offset += 4) {
            __builtin_lw_erri("!big", term1_w, term1 + offset, "0");
            term2_w = decode_int_big_endian_from(decoder_term2);
            if(term1_w != term2_w) {
                return term1_w - term2_w;
            }
        }
    }
    */
    for(; offset < term1_length && offset < term2_length; offset++) {
        uint8_t term1 = decode_byte_from(decoder_term1);
        uint8_t term2 = decode_byte_from(decoder_term2);
        if(term1 != term2) {
            return term1 - term2;
        }
    }
    return term1_length - term2_length;
}

static int compare_with_next_term(decoder_t* decoder_term1, uint32_t term1_length,
                                   decoder_t* decoder_term2, uint32_t term2_length) {

    uintptr_t curr_term = get_absolute_address_from(decoder_term2);
    uintptr_t searched_term = get_absolute_address_from(decoder_term1);

    // compare the searched term with the next term the decoder points to
    int cmp = compare_terms(decoder_term1, term1_length, decoder_term2, term2_length);

    // jump to the end of the term for further processing
    seek_decoder(decoder_term2, curr_term + term2_length);

    // reset the decoder to the searched term for next comparison
    seek_decoder(decoder_term1, searched_term);

    return cmp;
}

static void skip_term(decoder_t* decoder) {

    uint32_t term_length = decode_vint_from(decoder);
    seek_decoder(decoder, get_absolute_address_from(decoder) + term_length);
}

static void reset_block(block_t* block) {
    block->term = 0;
    block->term_size = 0;
    block->block_address = 0;
    block->block_size = 0;
}

// search for a particular block in the block table in MRAM (floor operation in BST)
static int lookup_term_block(decoder_t* decoder, const term_t* term, block_t* block) {

    reset_block(block);

    uintptr_t succ_node = 0;
    uintptr_t succ_node_ancestor = 0;
    int found_cmp = 0;
    uint8_t succ_left_most = 0;

    while (1) {

        uintptr_t curr_block = get_absolute_address_from(decoder);
        uint32_t block_term_length = decode_vint_from(decoder);
        uintptr_t curr_term = get_absolute_address_from(decoder);

        // compare the searched term with the current term of the block table
        int cmp = compare_with_next_term(term->term_decoder, term->size, decoder, block_term_length);

        // read child info and address
        uint32_t child_info = decode_vint_from(decoder);
        uint32_t address = decode_vint_from(decoder);

        if(cmp == 0) {
            // term found in the block table
            block->term = curr_term;
            block->term_size = block_term_length;
            block->block_address = address;
            found_cmp = 0;

            // update successor node
            // if there is a right child the successor is its left-most child
            // Otherwise this is the first ancestor for which the searched term is in the left subtree
            int right_child_offset = child_info >> 2;
            if(right_child_offset) {
                succ_node = get_absolute_address_from(decoder) + right_child_offset;
                succ_left_most = 1;
            }
            else {
                succ_node = succ_node_ancestor;
                succ_left_most = 0;
            }
            break;
        }
        else if(cmp < 0) {
            // searched term is smaller than current term, go to left child
            // the left child is simply the next node in the block table
            uint8_t has_left_child = (child_info & 1) != 0;
            if(!has_left_child) {
                // the left child is the next node in the byte array
                // If no left child, we are done
                break;
            }
            succ_node_ancestor = curr_block;
        }
        else {
            // searched term is larger than current term, this is the new floor element
            // then go to right child if any
            block->term = curr_term;
            block->term_size = block_term_length;
            block->block_address = address;
            found_cmp = cmp;

            int right_child_offset = child_info >> 2;
            if(right_child_offset) {
                succ_node =
                    get_absolute_address_from(decoder) + right_child_offset;
                seek_decoder(decoder, succ_node);
                succ_left_most = 1;
            }
            else {
                succ_node = succ_node_ancestor;
                succ_left_most = 0;
                // no right child, we are done
                break;
            }
        }
    }

    if(block->term) {
        // a term has been found, set the block size as the difference
        // between the next address and the block address
        int addr;
        if(succ_node == 0) {
            // special case when this node is the last
            // read the last address from the block table
            // As this node has no successor and the tree is written in pre-order,
            // the last address is the next element after this node
            succ_node = block->term + block->term_size;
            seek_decoder(decoder, succ_node);
            uint32_t child_info = decode_vint_from(decoder);
            decode_vint_from(decoder);
            while((child_info & 1) != 0) {
                skip_term(decoder);
                child_info = decode_vint_from(decoder);
                decode_vint_from(decoder);
            }
            addr = decode_vint_from(decoder);
        }
        else {
            seek_decoder(decoder, succ_node);
            skip_term(decoder);
            uint32_t child_info = decode_vint_from(decoder);
            addr = decode_vint_from(decoder);
            // when the stored successor is not an ancestor but a right child
            // of the found node, traverse the tree towards the left-most child 
            // to find the true successor
            if(succ_left_most != 0) {
              while(child_info & 1) {
                skip_term(decoder);
                child_info = decode_vint_from(decoder);
                addr = decode_vint_from(decoder);
              }
            }
        }
        block->block_size = addr - block->block_address;
    }
    return found_cmp;
}


bool get_field_address(uintptr_t index, const term_t* field, uintptr_t* field_address) {

    // get a decoder from the pool
    decoder_t* decoder = decoder_pool_get_one();
    initialize_decoder(decoder, index);

    // read index offsets
    nr_segments = 1 << decode_byte_from(decoder);
    block_table_offset = decode_vint_from(decoder);
    block_list_offset = decode_vint_from(decoder);
    postings_offset = decode_vint_from(decoder);

    index_begin_addr = get_absolute_address_from(decoder);

    // lookup the field in the field table
    lookup_term_block(decoder, field, &term_blocks[me()]);

    if(term_blocks[me()].term == 0) {
        *field_address = 0;
        // field not found
        decoder_pool_release_one(decoder);
        return false;
    }

    *field_address = index_begin_addr + block_table_offset + term_blocks[me()].block_address;
    decoder_pool_release_one(decoder);

    return true;
}

static void decode_postings_address_foreach_segment(decoder_t* decoder,
                                                    uint32_t offset,
                                                    postings_info_t* postings_for_segments) {
    int addr = offset + decode_vint_from(decoder);
    decode_vint_from(decoder); // ignore skip info
    for(int i = 0; i < nr_segments; ++i) {
        postings_for_segments[i].addr = addr;
        postings_for_segments[i].size = decode_vint_from(decoder);
        addr += postings_for_segments[i].size;
    }
}

bool get_term_postings(uintptr_t field_address,
                        const term_t* term,
                        postings_info_t* postings_for_segments) {

    // get a decoder from the pool
    decoder_t* decoder = decoder_pool_get_one();
    initialize_decoder(decoder, field_address);
    bool res = false;

    // search for the term in the block table
    int cmp = lookup_term_block(decoder, term, &term_blocks[me()]);
    if(term_blocks[me()].term == 0) {
        // term not found
        goto end;
    }

    // jump to the right block in the block list
    seek_decoder(decoder, index_begin_addr + block_list_offset + term_blocks[me()].block_address);

    // first check if the term seeked is the same as the first in the block
    if(cmp == 0) {
        // the term is the first in the block, decode the mram address to postings for each segment
        decode_postings_address_foreach_segment(decoder,
                                    index_begin_addr + postings_offset, postings_for_segments);
        res = true;
        goto end;
    }

    // ignore first term postings address and segment info
    uint32_t addr = decode_vint_from(decoder);
    uint32_t skip = decode_vint_from(decoder);
    skip_bytes_decoder(decoder, skip);

    // loop over remaining terms and compare the bytes to find the seeked term
    uintptr_t curr_addr = get_absolute_address_from(decoder);
    uintptr_t last_addr = index_begin_addr + block_list_offset + term_blocks[me()].block_address
                            + term_blocks[me()].block_size;

    while(curr_addr < last_addr) {

        uint32_t term_length = decode_vint_from(decoder);

        // compare the searched term with the current term of the block list
        int cmp = compare_with_next_term(term->term_decoder, term->size, decoder, term_length);

        if(cmp == 0) {
            // term found, decode the mram address to postings for each segment
            decode_postings_address_foreach_segment(decoder,
                                               index_begin_addr + postings_offset, postings_for_segments);
            res = true;
            goto end;
        }
        else if(cmp < 0) {
            // term is larger than the seeked term, term not found
            goto end;
        }

        //skip postings address
        decode_vint_from(decoder);
        // where to jump for the next term (skip segment info for this term)
        uint32_t skip = decode_vint_from(decoder);
        skip_bytes_decoder(decoder, skip);
        curr_addr = get_absolute_address_from(decoder);
    }

end:
    decoder_pool_release_one(decoder);
    return res;
}

