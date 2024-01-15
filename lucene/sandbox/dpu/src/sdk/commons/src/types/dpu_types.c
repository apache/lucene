/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

#include <dpu_types.h>

const char *dpu_slice_target_names[NR_OF_DPU_SLICE_TARGETS]
    = { "DPU_SLICE_TARGET_NONE", "DPU_SLICE_TARGET_DPU", "DPU_SLICE_TARGET_ALL", "DPU_SLICE_TARGET_GROUP" };