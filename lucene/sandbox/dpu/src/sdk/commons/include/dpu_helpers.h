/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

#ifndef DPU_HELPERS__H
#define DPU_HELPERS__H

#include <stdint.h>
#include <dpu_types.h>

#define likely(expr) __builtin_expect((expr), 1)
#define unlikely(expr) __builtin_expect((expr), 0)

/*
 * Convert an integer division factor for the DPU clock into
 * the hardware setting.
 *
 * @param factor clock divider
 * @return the clock setting used to program the DPU hardware
 */
dpu_clock_division_t
from_division_factor_to_dpu_enum(uint8_t factor);

#endif /* DPU_HELPERS__H */
