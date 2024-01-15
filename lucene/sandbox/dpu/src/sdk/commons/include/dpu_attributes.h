/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

#ifndef DPU_ATTRIBUTES_H
#define DPU_ATTRIBUTES_H

#define __API_SYMBOL__ __attribute__((visibility("default"), used))
#define __USED_SYMBOL__ __attribute__((used))
#define __PERF_PROFILING_SYMBOL__ __attribute__((noinline))

#endif // DPU_ATTRIBUTES_H
