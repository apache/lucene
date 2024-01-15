/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

#ifndef DPU_VERBOSE_STATIC_VERBOSE_H
#define DPU_VERBOSE_STATIC_VERBOSE_H

#include "verbose_control.h"

/**
 * @param module_name name of a module for which we want verbosity level
 * @return the corresponding verbose control
 */
struct verbose_control *
get_verbose_control_for(const char *module_name);

#endif /* DPU_VERBOSE_STATIC_VERBOSE_H */
