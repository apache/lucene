/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

#ifndef BACKENDS_DPU_LOG_UTILS_H
#define BACKENDS_DPU_LOG_UTILS_H

#include <dpu_macro_utils.h>

#define LOG_FMT_RANK "%04x...."
#define LOG_FMT_CI "%04x%02x.."
#define LOG_FMT_DPU "%04x%02x%02x"

#define WARNING W
#define INFO I
#define DEBUG D
#define VERBOSE V

#define LOG(lvl) _CONCAT(LOG, lvl)

#define LOG_FN_VC(lvl, vc, fmt, ...) LOG(lvl)(vc, "%s: " fmt, __func__, ##__VA_ARGS__)
#define LOG_FN(lvl, fmt, ...) LOG(lvl)(__vc(), "%s: " fmt, __func__, ##__VA_ARGS__)
#define LOG_RANK(lvl, rank, fmt, ...) LOG(lvl)(__vc(), "[" LOG_FMT_RANK "] %s: " fmt, rank->rank_id, __func__, ##__VA_ARGS__)
#define LOG_CI(lvl, rank, ci, fmt, ...) LOG(lvl)(__vc(), "[" LOG_FMT_CI "] %s: " fmt, rank->rank_id, ci, __func__, ##__VA_ARGS__)
#define LOG_DPU(lvl, dpu, fmt, ...)                                                                                              \
    LOG(lvl)(__vc(), "[" LOG_FMT_DPU "] %s: " fmt, dpu->rank->rank_id, dpu->slice_id, dpu->dpu_id, __func__, ##__VA_ARGS__)

#endif // BACKENDS_DPU_LOG_UTILS_H
