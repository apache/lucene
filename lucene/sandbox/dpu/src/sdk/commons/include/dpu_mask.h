/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

#ifndef DPU_MASK_H
#define DPU_MASK_H

#include <stdio.h>
#include <stdarg.h>
#include <stdbool.h>

#include "dpu_types.h"

static inline dpu_bitfield_t
dpu_mask_empty(void)
{
    return (dpu_bitfield_t)0;
}

static inline dpu_bitfield_t
dpu_mask_one(dpu_member_id_t dpu_id)
{
    return (dpu_bitfield_t)(1 << dpu_id);
}

static inline dpu_bitfield_t
dpu_mask_all(uint8_t nb_of_dpus)
{
    return (dpu_bitfield_t)((1L << nb_of_dpus) - 1);
}

static inline dpu_bitfield_t
dpu_mask_union(dpu_bitfield_t mask1, dpu_bitfield_t mask2)
{
    return mask1 | mask2;
}

static inline dpu_bitfield_t
dpu_mask_intersection(dpu_bitfield_t mask1, dpu_bitfield_t mask2)
{
    return mask1 & mask2;
}

static inline dpu_bitfield_t
dpu_mask_difference(dpu_bitfield_t mask1, dpu_bitfield_t mask2)
{
    return mask1 & ~mask2;
}

static inline dpu_bitfield_t
dpu_mask_disjoint(dpu_bitfield_t mask1, dpu_bitfield_t mask2)
{
    return mask1 ^ mask2;
}

static inline dpu_bitfield_t
dpu_mask_select(dpu_bitfield_t mask, dpu_member_id_t dpu_id)
{
    return dpu_mask_union(mask, dpu_mask_one(dpu_id));
}

static inline dpu_bitfield_t
dpu_mask_unselect(dpu_bitfield_t mask, dpu_member_id_t dpu_id)
{
    return dpu_mask_difference(mask, dpu_mask_one(dpu_id));
}

static inline dpu_bitfield_t
dpu_bitfield_toggle(dpu_bitfield_t mask, dpu_member_id_t dpu_id)
{
    return dpu_mask_disjoint(mask, dpu_mask_one(dpu_id));
}

static inline bool
dpu_mask_is_selected(dpu_bitfield_t mask, dpu_member_id_t dpu_id)
{
    return dpu_mask_intersection(mask, dpu_mask_one(dpu_id)) != dpu_mask_empty();
}

static inline uint32_t
dpu_mask_count(dpu_bitfield_t mask)
{
    return __builtin_popcount(mask);
}

#endif // DPU_MASK_H
