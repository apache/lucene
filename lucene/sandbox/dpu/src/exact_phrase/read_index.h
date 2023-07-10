#include <stdint.h>
#include <defs.h>
#include <mram.h>
#define SEQREAD_CACHE_SIZE 256
#include <seqread.h>
#include <built_ins.h>

/**
 * structure to hold the information relative to a block
 * of the block table
 */
struct Block {
    __mram_ptr const uint8_t* term;
    uint32_t term_size;
    uint32_t block_address;
    uint32_t block_size;
};

struct Term {
    const uint8_t* term;
    uint32_t size;
};

int readByte_mram(const uint8_t** data, seqreader_t* reader) {
    int i = **data & 0x7F;
    *data = seqread_get((void*)*data, 1, reader);
    return i;
}

int readShort_mram(const uint8_t** data, seqreader_t* reader) {
    int i = **data & 0x7F;
    *data = seqread_get((void*)*data, 1, reader);
    i |= (**data & 0x7F) << 7;
    *data = seqread_get((void*)*data, 1, reader);
    return i;
}

// read a variable length integer in MRAM
int readVInt_mram(const uint8_t** data, seqreader_t* reader) {
    int i = **data & 0x7F;
    for (int shift = 7; (**data & 0x80) != 0; shift += 7) {
        *data = seqread_get((void*)*data, 1, reader);
        i |= ((**data) & 0x7F) << shift;
    }
    *data = seqread_get((void*)*data, 1, reader);
    return i;
}

//TODO we never read a VLong because the address would be passed the MRAM size
// read a variable length long in MRAM
long readVLong_mram(const uint8_t** data, seqreader_t* reader) {
    long i = **data & 0x7F;
    for (int shift = 7; (**data & 0x80) != 0; shift += 7) {
        *data = seqread_get((void*)*data, 1, reader);
        i |= ((**data) & 0x7F) << shift;
    }
    *data = seqread_get((void*)*data, 1, reader);
    return i;
}

// read a variable length integer in MRAM
int readVInt(const uint8_t** data) {
    int i = **data & 0x7F;
    for (int shift = 7; (**data & 0x80) != 0; shift += 7) {
        *data+=1;
        i |= ((**data) & 0x7F) << shift;
    }
    return i;
}

// compare terms, return 0 if equal, < 0 if term1 < term2, > 0 if term1 > term2
int compare_terms(const struct Term* term1_ptr, const struct Term* term2_ptr, seqreader_t* term2_reader) {
    // terms are encoded as UTF-8, so we can compare the bytes directly
    // perform the comparison 4B per 4B if the terms have the same alignment modulo 8
    const uint8_t* term1 = term1_ptr->term;
    const uint8_t* term2 = term2_ptr->term;
    uint32_t term1_length = term1_ptr->size;
    uint32_t term2_length = term2_ptr->size;
    int term1_w, term2_w;
    int offset = 0;
    if((((uintptr_t)term1) & 7) == (((uintptr_t)term2) & 7)) {
        // Need to be cautious of the endianness when loading as integer
        // Perform a load as big endian while the DPU is little-endian, hence using a builtin
        // Also need a prelude to align
        int realign = ((uintptr_t)term1) & 7;
        for(; offset < realign && offset < term1_length && offset < term2_length; offset++) {
            if(term1[offset] != *term2) {
                return term1[offset] - *term2;
            }
            term2 = seqread_get((void*)term2, sizeof(uint8_t), term2_reader);
        }
        for(; (offset + 4) < term1_length && (offset + 4) < term2_length; offset += 4) {
            __builtin_lw_erri("!big", term1_w, term1 + offset, "0");
            __builtin_lw_erri("!big", term2_w, term2, "0");
            if(term1_w != term2_w) {
                return term1_w - term2_w;
            }
            term2 = seqread_get((void*)term2, sizeof(int), term2_reader);
        }
    }
    for(; offset < term1_length && offset < term2_length; offset++) {
        if(term1[offset] != *term2) {
            return term1[offset] - *term2;
        }
        term2 = seqread_get((void*)term2, sizeof(uint8_t), term2_reader);
    }
    return term1_length - term2_length;
}

// search for a particular block in the block table in MRAM (floor operation in BST)
int get_block_from_table(const uint8_t* block_table, seqreader_t* block_table_reader,
        const struct Term* term, struct Block* block) {

    block->term = 0;
    block->term_size = 0;
    block->block_address = 0;
    block->block_size = 0;
    __mram_ptr const uint8_t* succ_node = 0;
    __mram_ptr const uint8_t* succ_node_ancestor = 0;
    int found_cmp = 0;

    while (1) {
        __mram_ptr const uint8_t* curr_block = seqread_tell((void*)block_table, block_table_reader);
        int block_term_length = readVInt_mram(&block_table, block_table_reader);
        __mram_ptr const uint8_t* curr_term = seqread_tell((void*)block_table, block_table_reader);

        struct Term term2 = {.term = block_table, .size = block_term_length};
        int cmp = compare_terms(term, &term2, block_table_reader);
        block_table = seqread_seek((__mram_ptr void*)(curr_term + block_term_length), block_table_reader);
        int childInfo = readVInt_mram(&block_table, block_table_reader);
        int address = readVLong_mram(&block_table, block_table_reader);
        if(cmp == 0) {
            block->term = curr_term;
            block->term_size = block_term_length;
            block->block_address = address;
            found_cmp = 0;

            // update successor node
            // if there is a right child this is the successor
            // Otherwise this is the first ancestor for which the searched term is in the left subtree
            int right_child_offset = childInfo >> 2;
            if(right_child_offset) {
                succ_node = (__mram_ptr const uint8_t*)(seqread_tell((void*)block_table, block_table_reader))
                    + right_child_offset;
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
                __mram_ptr uint8_t* right_child_mram_addr =
                    seqread_tell((void*)block_table, block_table_reader) + right_child_offset;
                block_table = seqread_seek(right_child_mram_addr, block_table_reader);
                succ_node = right_child_mram_addr;
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
            const uint8_t* last_node = seqread_seek((__mram_ptr void*)succ_node, block_table_reader);
            uint32_t childInfo = readVInt_mram(&last_node, block_table_reader);
            readVLong_mram(&last_node, block_table_reader);
            while((childInfo & 1) != 0) {
                int block_term_length = readVInt_mram(&last_node, block_table_reader);
                __mram_ptr const uint8_t* curr_term = seqread_tell((void*)last_node, block_table_reader);
                last_node = seqread_seek((__mram_ptr void*)(curr_term + block_term_length), block_table_reader);
                childInfo = readVInt_mram(&last_node, block_table_reader);
                readVLong_mram(&last_node, block_table_reader);
            }
            addr = readVLong_mram(&last_node, block_table_reader);
        }
        else {
            const uint8_t* last_node = seqread_seek((__mram_ptr void*)(succ_node), block_table_reader);
            int succ_term_length = readVInt_mram(&last_node, block_table_reader);
            __mram_ptr const uint8_t* succ_node_addr =
                seqread_tell((void*)last_node, block_table_reader) + succ_term_length;
            last_node = seqread_seek((__mram_ptr void*)succ_node_addr, block_table_reader);
            readVInt_mram(&last_node, block_table_reader);
            addr = readVLong_mram(&last_node, block_table_reader);
        }
        block->block_size = addr - block->block_address;
    }
    return found_cmp;
}

 __mram_ptr const uint8_t*
 get_term_postings_from_index(const uint8_t* index, seqreader_t *seqread,
                               const struct Term* term,
                              __mram_ptr const uint8_t* start_addr_block_list,
                              __mram_ptr const uint8_t* start_addr_postings,
                               struct Block* block) {

    // search for the term in the block table
    int cmp = get_block_from_table(index, seqread, term, block);
    if(block->term == 0) {
        // term not found
        block->term = 0;
        block->term_size = 0;
        block->block_address = 0;
        block->block_size = 0;
        return 0;
    }

    // search for the term in the block list
    index = seqread_seek((__mram_ptr void*)(start_addr_block_list + block->block_address), seqread);
    // first check if the term seeked is the same as the first in the block
    if(cmp == 0) {
        // the term is the first in the block, return the mram address to postings
        block->block_address = readVLong_mram(&index, seqread);
        block->block_size = readVLong_mram(&index, seqread);
        return (__mram_ptr const uint8_t*)(start_addr_postings + block->block_address);
    }

    // ignore first term postings address and size
    readVLong_mram(&index, seqread);
    readVLong_mram(&index, seqread);

    // loop over remaining terms and compare to find the seeked term
    uintptr_t curr_addr = (uintptr_t)seqread_tell((void*)index, seqread);
    uintptr_t last_addr = (uintptr_t)(start_addr_block_list + block->block_address + block->block_size);
    block->term = 0;
    block->term_size = 0;
    block->block_address = 0;
    block->block_size = 0;
    struct Term term2;
    while(curr_addr < last_addr) {
        int term_length = readVInt_mram(&index, seqread);
        __mram_ptr const uint8_t* curr_term = seqread_tell((void*)index, seqread);
        term2.term = index;
        term2.size = term_length;
        int cmp = compare_terms(term, &term2, seqread);
        index = seqread_seek((__mram_ptr void*)(curr_term + term_length), seqread);
        int address = readVLong_mram(&index, seqread);
        int size = readVLong_mram(&index, seqread);
        if(cmp == 0) {
            // term found, return the mram address to postings
            block->block_address = address;
            block->block_size = size;
            return (__mram_ptr const uint8_t*)(start_addr_postings + address);
        }
        else if(cmp < 0) {
            // term is larger than the seeked term, term not found
            return 0;
        }
        curr_addr = (uintptr_t)seqread_tell((void*)index, seqread);
    }

    return 0;
 }
