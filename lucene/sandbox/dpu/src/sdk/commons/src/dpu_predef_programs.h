/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

#ifndef DPU_PREDEF_PROGRAMS_H
#define DPU_PREDEF_PROGRAMS_H

#include <dpu_types.h>

dpuinstruction_t *
fetch_core_dump_program(iram_size_t *size);

dpuinstruction_t *
fetch_restore_registers_program(iram_size_t *size);

dpuinstruction_t *
fetch_mram_access_program(iram_size_t *size);

dpuinstruction_t *
fetch_internal_reset_program(iram_size_t *size);

#endif // DPU_PREDEF_PROGRAMS_H
