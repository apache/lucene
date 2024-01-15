/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

/**
 * @brief Controls the verbosity of a specific module.
 *
 * A module must use LOGW, LOGI, LOGD and LOGV to display (or not) messages.
 * Whether to display or not is controlled by a verbose_control structure,
 * created with verbose_control_setup on the basis of a verbose control
 * configuration.
 *
 * When finishing, the module should delete the control structure with
 * verbose_control_clean.
 */
#ifndef DPU_VERBOSE_CONTROL_H
#define DPU_VERBOSE_CONTROL_H

#include <stdbool.h>

#include "dpu_helpers.h"
#include "verbose_config.h"

struct verbose_control {
    void (*printw)(struct verbose_control *vcs, const char *format, ...);

    void (*printi)(struct verbose_control *vcs, const char *format, ...);

    void (*printd)(struct verbose_control *vcs, const char *format, ...);

    void (*printv)(struct verbose_control *vcs, const char *format, ...);

    FILE *output_file;
    const char *module_name;
    void *filter;
    bool color_enable;
};

/* Macros (properly optimize-out disabled verbosity levels) */
#define LOGW_ENABLED(vcs) (likely((vcs)->printw != NULL))
#define LOGI_ENABLED(vcs) (unlikely((vcs)->printi != NULL))
#define LOGD_ENABLED(vcs) (unlikely((vcs)->printd != NULL))
#define LOGV_ENABLED(vcs) (unlikely((vcs)->printv != NULL))

#define LOGW(vcs, ...)                                                                                                           \
    do {                                                                                                                         \
        if (LOGW_ENABLED(vcs))                                                                                                   \
            (vcs)->printw(vcs, __VA_ARGS__);                                                                                     \
    } while (0)
#define LOGI(vcs, ...)                                                                                                           \
    do {                                                                                                                         \
        if (LOGI_ENABLED(vcs))                                                                                                   \
            (vcs)->printi(vcs, __VA_ARGS__);                                                                                     \
    } while (0)
#define LOGD(vcs, ...)                                                                                                           \
    do {                                                                                                                         \
        if (LOGD_ENABLED(vcs))                                                                                                   \
            (vcs)->printd(vcs, __VA_ARGS__);                                                                                     \
    } while (0)
#define LOGV(vcs, ...)                                                                                                           \
    do {                                                                                                                         \
        if (LOGV_ENABLED(vcs))                                                                                                   \
            (vcs)->printv(vcs, __VA_ARGS__);                                                                                     \
    } while (0)

/**
 * @param module_name name of the module for which a new verbose control structure is needed
 * @param config the base verbose configuration used to output traces
 * @param control a control structure, initialized by this function
 */
void
verbose_control_setup(const char *module_name, struct verbose_config *config, struct verbose_control *control);

/**
 * @param control a verbose control structure, cleaned by this function
 */
void
verbose_control_clean(struct verbose_control *control);

#endif /* DPU_VERBOSE_CONTROL_H */
