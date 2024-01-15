/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

#ifndef DPU_UFI_TYPES_H
#define DPU_UFI_TYPES_H

#include <stdint.h>

typedef uint32_t dpu_selected_mask_t;

typedef enum dpu_transfer_type_t {
    DPU_TRANSFER_FROM_MRAM,
    DPU_TRANSFER_TO_MRAM,
} dpu_transfer_type_t;

#endif // DPU_UFI_TYPES_H
