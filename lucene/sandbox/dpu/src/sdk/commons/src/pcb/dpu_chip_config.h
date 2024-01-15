/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

#ifndef DPU_CHIP_CONFIG_H
#define DPU_CHIP_CONFIG_H

#include <stddef.h>
#include <dpu_description.h>

extern const dpu_description_t dpu_chip_descriptions[NEXT_DPU_CHIP_IDX];

static inline dpu_description_t
default_description_for_chip(dpu_chip_id_e chip_id)
{
    unsigned int chip_idx = chip_id_to_idx(chip_id);
    return (chip_idx < NEXT_DPU_CHIP_IDX) ? dpu_chip_descriptions[chip_idx] : NULL;
}

#endif // DPU_CHIP_CONFIG_H
