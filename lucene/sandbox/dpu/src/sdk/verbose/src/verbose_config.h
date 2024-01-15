/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */
/**
 * @brief generic verbose configuration of UPMEM modules based on environment variables.
 *
 * Traces are written:
 *   - into stdout if UPMEM_VERBOSE contains the '-' definition
 *   - otherwise into the file named after the 'logname' definition of UPMEM_VERBOSE if set
 *   - otherwise into dpu-<pid>.log
 *
 * If logs are written into a file, it is located:
 *   - In the 'dir' definition of UPMEM_VERBOSE if the environment variable and the definition is set
 *   - otherwise in /var/log/dpu/ if this directory is defined
 *   - otherwise in the current directory
 *
 * UPMEM_VERBOSE defines the verbosity level of each module. The string is expressed
 * as a sequence of profile definitions separated by commas (e.g. 'api:3,fsim:1,S'). Each
 * definition is either 'name:level', 'name:' or simply a verbosity level:
 *   - Either 'module:level': set the module's verbosity level to the specified value
 *   - Or 'module:': set the module's verbosity level to 'W'
 *   - Or simply 'level': an alias of 'default:level' to specify the verbosity level of
 *     unspecified modules
 *
 * Level values can be:
 *   - 0,S,s : silent
 *   - 1,W,w : print only warnings
 *   - 2,I,i : print warnings and information
 *   - 3,D,d : print warnings, information and debug messages
 *   - 4,V,v : print everything
 *
 * Usage: the "main" module must create a verbose configuration, with verbose_configuration_setup and
 * delete this configuration once all the control structures are set (verbose_configuration_clean). It must
 * use or provide this configuration to any implied module, so that they can set up their
 * verbose control structure.
 */
#ifndef DPU_VERBOSE_CONFIG_H
#define DPU_VERBOSE_CONFIG_H

#include <stdio.h>
#include <stdbool.h>
#include "verbose_profile.h"

struct verbose_config {
    FILE *output_file;
    bool manage_file_handle;
    struct verbose_profile profile;
};

/**
 * @brief reads the UPMEM_VERBOSE_... environment variables (if defined) to configure the verbosity
 * @return whether the setup succeeded (otherwise environment variables are not correctly defined)
 */
bool
verbose_config_setup(struct verbose_config *config);

/**
 * @param config a verbose configuration, cleaned, along with the underlying structures
 */
void
verbose_config_clean(struct verbose_config *config);

#endif /* DPU_VERBOSE_CONFIG_H */
