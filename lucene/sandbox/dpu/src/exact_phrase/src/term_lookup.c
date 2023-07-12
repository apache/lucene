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

// compare terms, return 0 if equal, < 0 if term1 < term2, > 0 if term1 > term2
static int compare_terms(const term_t* term1_ptr, decoder_t* decoder_term2, uint32_t term2_length) {
    // terms are encoded as UTF-8, so we can compare the bytes directly
    // perform the comparison 4B per 4B if the terms have the same alignment modulo 8
    const uint8_t* term1 = term1_ptr->term;
    uint32_t term1_length = term1_ptr->size;
    int term1_w, term2_w;
    int offset = 0;
    if(decoder_is_aligned_with(decoder_term2, term1)) {
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
    for(; offset < term1_length && offset < term2_length; offset++) {
        uint8_t term2 = decode_byte_from(decoder_term2);
        if(term1[offset] != term2) {
            return term1[offset] - term2;
        }
    }
    return term1_length - term2_length;
}

static int compare_with_next_term(const term_t* term, decoder_t* decoder, uint32_t* term_length) {

    *term_length = decode_vint_from(decoder);
    uintptr_t curr_term = get_absolute_address_from(decoder);

    // compare the searched term with the current term the decoder points to
    int cmp = compare_terms(term, decoder, *term_length);

    // jump to the end of the term for further processing
    skip_bytes_to_jump(decoder, curr_term + *term_length);

    return cmp;
}

static void skip_term(decoder_t* decoder) {

    uint32_t term_length = decode_vint_from(decoder);
    skip_bytes_to_jump(decoder, get_absolute_address_from(decoder) + term_length);
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

    while (1) {

        uintptr_t curr_block = get_absolute_address_from(decoder);
        uint32_t block_term_length = decode_vint_from(decoder);
        uintptr_t curr_term = get_absolute_address_from(decoder);

        // compare the searched term with the current term of the block table
        int cmp = compare_terms(term, decoder, block_term_length);

        skip_bytes_to_jump(decoder, curr_term + block_term_length);

        // read child info and address
        uint32_t childInfo = decode_vint_from(decoder);
        uint32_t address = decode_vint_from(decoder);

        if(cmp == 0) {
            // term found in the block table
            block->term = curr_term;
            block->term_size = block_term_length;
            block->block_address = address;
            found_cmp = 0;

            // update successor node
            // if there is a right child this is the successor
            // Otherwise this is the first ancestor for which the searched term is in the left subtree
            int right_child_offset = childInfo >> 2;
            if(right_child_offset) {
                succ_node = get_absolute_address_from(decoder) + right_child_offset;
            }
            else {
                succ_node = succ_node_ancestor;
            }
            break;
        }
        else if(cmp < 0) {
            // searched term is smaller than current term, go to left child
            // the left child is simply the next node in the block table
            uint8_t hasLeftChild = (childInfo & 1) != 0;
            if(!hasLeftChild) {
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

            int right_child_offset = childInfo >> 2;
            if(right_child_offset) {
                succ_node =
                    get_absolute_address_from(decoder) + right_child_offset;
                skip_bytes_to_jump(decoder, succ_node);
            }
            else {
                succ_node = succ_node_ancestor;
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
            skip_bytes_to_jump(decoder, succ_node);
            uint32_t childInfo = decode_vint_from(decoder);
            decode_vint_from(decoder);
            while((childInfo & 1) != 0) {
                skip_term(decoder);
                childInfo = decode_vint_from(decoder);
                decode_vint_from(decoder);
            }
            addr = decode_vint_from(decoder);
        }
        else {
            skip_bytes_to_jump(decoder, succ_node);
            skip_term(decoder);
            decode_vint_from(decoder);
            addr = decode_vint_from(decoder);
        }
        block->block_size = addr - block->block_address;
    }
    return found_cmp;
}


bool get_field_address(uintptr_t index, const term_t* field, uintptr_t* field_address) {

    // when looking up the field address, the phrase match algorithm has not started
    // we can use the same decoder as for the first term postings
    decoder_t* decoder = initialize_decoder(index, 0 /*term id*/);
    // read index offsets
    block_table_offset = decode_vint_from(decoder);
    block_list_offset = decode_vint_from(decoder);
    postings_offset = decode_vint_from(decoder);

    index_begin_addr = get_absolute_address_from(decoder);

    // lookup the field in the field table
    lookup_term_block(decoder, field, &term_blocks[me()]);

    if(term_blocks[me()].term == 0) {
        *field_address = 0;
        // field not found
        return false;
    }

    *field_address = index_begin_addr + block_table_offset + term_blocks[me()].block_address;

    return true;
}

bool get_term_postings(uintptr_t field_address,
                        const term_t* term, uintptr_t* postings_address,
                        uint32_t* postings_byte_size) {

    // when looking up the term postings, the phrase match algorithm has not started
    // we can use the same decoder as for reading the term postings
    decoder_t* decoder = initialize_decoder(field_address, term->id);
    *postings_address = 0;
    *postings_byte_size = 0;

    // search for the term in the block table
    int cmp = lookup_term_block(decoder, term, &term_blocks[me()]);
    if(term_blocks[me()].term == 0) {
        // term not found
        return false;
    }

    // jump to the right block in the block list
    skip_bytes_to_jump(decoder, index_begin_addr + block_list_offset + term_blocks[me()].block_address);

    // first check if the term seeked is the same as the first in the block
    if(cmp == 0) {
        // the term is the first in the block, return the mram address to postings
        uint32_t address = decode_vint_from(decoder);
        uint32_t size = decode_vint_from(decoder);
        *postings_address = index_begin_addr + postings_offset + address;
        *postings_byte_size = size;
        return true;
    }

    // ignore first term postings address and size
    decode_vint_from(decoder);
    decode_vint_from(decoder);

    // loop over remaining terms and compare the bytes to find the seeked term
    uintptr_t curr_addr = get_absolute_address_from(decoder);
    uintptr_t last_addr = index_begin_addr + postings_offset + term_blocks[me()].block_address
                            + term_blocks[me()].block_size;

    while(curr_addr < last_addr) {

        uint32_t term_length;

        // compare the searched term with the current term of the block list
        int cmp = compare_with_next_term(term, decoder, &term_length);

        uint32_t address = decode_vint_from(decoder);
        uint32_t size = decode_vint_from(decoder);

        if(cmp == 0) {
            // term found, set the mram address to postings
            *postings_address = index_begin_addr + postings_offset + address;
            *postings_byte_size = size;
            return true;
        }
        else if(cmp < 0) {
            // term is larger than the seeked term, term not found
            return false;
        }
        curr_addr = get_absolute_address_from(decoder);
    }

    return false;
}
