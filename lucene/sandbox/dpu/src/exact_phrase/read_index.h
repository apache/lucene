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
int compare_terms(const uint8_t* term1, int term1_length,
        const uint8_t* term2, seqreader_t* term2_reader, int term2_length) {
    // terms are encoded as UTF-8, so we can compare the bytes directly
    // perform the comparison 4B per 4B if the terms have the same alignment modulo 8
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
void get_block_from_table(const uint8_t* block_table, seqreader_t* block_table_reader,
        const uint8_t* term, int term_length, struct Block* block) {

    block->term = 0;
    block->term_size = 0;
    block->block_address = 0;
    block->block_size = 0;
    __mram_ptr const uint8_t* succ_node = 0;
    __mram_ptr const uint8_t* succ_node_ancestor = 0;

    while (1) {
        __mram_ptr const uint8_t* curr_block = seqread_tell((void*)block_table, block_table_reader);
        int block_term_length = readVInt_mram(&block_table, block_table_reader);
        __mram_ptr const uint8_t* curr_term = seqread_tell((void*)block_table, block_table_reader);

        int cmp = compare_terms(term, term_length, block_table, block_table_reader, block_term_length);
        block_table = seqread_seek((__mram_ptr void*)(curr_term + block_term_length), block_table_reader);
        int childInfo = readVInt_mram(&block_table, block_table_reader);
        int address = readVLong_mram(&block_table, block_table_reader);
        if(cmp == 0) {
            block->term = curr_term;
            block->term_size = block_term_length;
            block->block_address = address;

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
            readVInt_mram(&last_node, block_table_reader);
            readVLong_mram(&last_node, block_table_reader);
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
}

 __mram_ptr const uint8_t*
 get_term_postings_from_index(const uint8_t* index, seqreader_t *seqread,
                               const uint8_t* term, uint32_t term_size, struct Block* block) {

    //TODO
    return 0;
 }
