/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

#ifndef DPU_RANK_ID_ALLOCATOR_H
#define DPU_RANK_ID_ALLOCATOR_H

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <dpu_types.h>

// MAX_NUMBER_OF_RANKS must be a multiple of 64
#define MAX_NUMBER_OF_RANKS 64

#define NUMBER_OF_RANKS_PER_ENTRY 64
#define NUMBER_OF_ENTRIES_IN_RANK_ALLOCATOR ((MAX_NUMBER_OF_RANKS) / (NUMBER_OF_RANKS_PER_ENTRY))
static uint64_t rank_id_allocator_bitfield[NUMBER_OF_ENTRIES_IN_RANK_ALLOCATOR];

static pthread_mutex_t rank_id_allocator_lock = PTHREAD_MUTEX_INITIALIZER;

static bool
dpu_get_next_rank_id(dpu_rank_id_t *rank_id)
{
    bool found = false;
    pthread_mutex_lock(&rank_id_allocator_lock);

    for (int each_entry = 0; each_entry < NUMBER_OF_ENTRIES_IN_RANK_ALLOCATOR; ++each_entry) {
        if (rank_id_allocator_bitfield[each_entry] != 0xffffffffffffffffl) {
            uint32_t id = __builtin_ctzl(~rank_id_allocator_bitfield[each_entry]);

            rank_id_allocator_bitfield[each_entry] |= 1l << id;
            *rank_id = each_entry * NUMBER_OF_RANKS_PER_ENTRY + id;
            found = true;
        }
    }

    pthread_mutex_unlock(&rank_id_allocator_lock);

    return found;
}

static void
dpu_release_rank_id(dpu_rank_id_t rank_id)
{
    pthread_mutex_lock(&rank_id_allocator_lock);

    uint32_t entry_id = rank_id / NUMBER_OF_RANKS_PER_ENTRY;
    uint32_t id = rank_id % NUMBER_OF_RANKS_PER_ENTRY;

    rank_id_allocator_bitfield[entry_id] &= ~(1l << id);

    pthread_mutex_unlock(&rank_id_allocator_lock);
}

#endif // DPU_RANK_ID_ALLOCATOR_H
