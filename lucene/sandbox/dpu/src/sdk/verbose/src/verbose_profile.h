/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

#ifndef DPU_VERBOSE_PROFILE_H
#define DPU_VERBOSE_PROFILE_H

#include <stdbool.h>

typedef enum {
    SILENT = 0,
    WARNING = 1,
    INFO = 2,
    DEBUG = 3,
    VERBOSE = 4,
} verbose_log_level_t;

struct verbose_profile_definition {
    const char *module_name;
    verbose_log_level_t log_level;
};

struct verbose_profile {
    unsigned int nr_definitions;
    struct verbose_profile_definition *definitions;
};

/**
 * @param profile a verbose profile, filled with the profile fetched from the provided definition
 * @param source the profile definition string
 * @param logdir storage for the value of the special definition 'dir'
 * @param logname storage for the value of the special definition 'logname'
 * @param logstdout storage for the boolean value of whether to output logs on stdout
 * @return whether profile is successfully fetched
 */
bool
verbose_profile_setup(struct verbose_profile *profile, const char *source, char **logdir, char **logname, bool *logstdout);

/**
 * @param profile a verbose profile, cleaned by this function
 */
void
verbose_profile_clean(struct verbose_profile *profile);

/**
 * @param module_name name of a module for which we expect a profile
 * @param profile the list of profile definitions
 * @return the corresponding log level, or a default level if no such module was found
 */
verbose_log_level_t
verbose_profile_for_module(const char *module_name, struct verbose_profile *profile);

#endif /* DPU_VERBOSE_PROFILE_H */
