/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

#ifndef DPU_PROPERTIES_LOADER_H
#define DPU_PROPERTIES_LOADER_H

/**
 * @brief Loads the configuration of a type of DPU from standard or user specific files.
 *
 * The module gets a default set of properties, provided by a text buffer. It may
 * overload them by loading a specific file HOME/dpu_XXX.cfg where XXX is
 * the DPU type (e.g. casim, fsim, etc.).
 *
 * Configuration file format is similar to java property files, except that comments
 * are identified as lines starting with "//".
 *
 * Upon success, the created properties must be deleted with dpu_properties_delete when
 * done.
 */

#include "dpu_properties.h"

/**
 * @brief Creates a new collection of properties gathered from the standard property files.
 *
 * The function get default properties, expressed as a series of lines of property definitions,
 * ending with a NULL pointer.
 *
 * The default properties may then be completed/replaced, by user specific definitions,
 * found in the target specific custom properties.
 *
 * @param dpu_type            the target type name
 * @param default_properties  string containing the contents of a default properties file
 * @return the new property set, or DPU_PROPERTIES_INVALID if the parsing failed
 */
dpu_properties_t
dpu_properties_load_for_type(const char *dpu_type, const char **default_properties);

/**
 * @brief Loads a collection of properties defined by a text enclosed in a string.
 *
 * @param config the configuration text
 * @return the new property set, or DPU_PROPERTIES_INVALID if the parsing failed
 */
dpu_properties_t
dpu_properties_load_from_string(const char *config);

dpu_properties_t
dpu_properties_load_from_profile(const char *profile);

char *
dpu_profile_concat3(const char *first, const char *second, const char *third);

#endif
