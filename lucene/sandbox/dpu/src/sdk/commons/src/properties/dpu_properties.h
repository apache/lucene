/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

#ifndef DPU_PROPERTIES_H
#define DPU_PROPERTIES_H

/**
 * @brief DPU configuration
 *
 * A DPU configuration is a collection of properties (in the Java sense of it),
 * read from configuration files.
 *
 * A configuration is created with dpu_properties_create and deleted upon call to
 * dpu_properties_delete.
 *
 * New configuration items can be added using dpu_properties_add.
 *
 * Applications can request for specific configuration values, identified by their
 * keys, using dpu_properties_get.
 */

#include <stdlib.h>
#include <stdbool.h>
#include <static_verbose.h>

/**
 * @brief A DPU configuration object
 */
typedef void *dpu_properties_t;

/**
 * @brief To report errors
 */
#define DPU_PROPERTIES_INVALID ((dpu_properties_t)NULL)

/**
 * @brief To report that a property is not found
 */
#define DPU_PROPERTIES_NO_SUCH_PROPERTY ((const char *)NULL)

/**
 * @return a new DPU configuration object, or dpu_properties_INVALID if allocation failed.
 */
dpu_properties_t
dpu_properties_create(void);

/**
 * @brief Deletes a DPU configuration.
 * @param dpu_properties the deleted configuration
 */
void
dpu_properties_delete(dpu_properties_t dpu_properties);

/**
 * @brief Registers a new property to a DPU configuration.
 *
 * If a property with similar name exists, overrides its value.
 *
 * @param dpu_properties the updated configuration
 * @param name the property name
 * @param value the property value
 * @return dpu_properties_INVALID if the memory is full, otherwise, this configuration
 */
dpu_properties_t
dpu_properties_add(dpu_properties_t dpu_properties, const char *name, const char *value);

/**
 * @brief Requests for a given property in a DPU configuration.
 *
 * @param dpu_properties the requested configuration
 * @param name the property name
 * @return the associated value, if found, dpu_properties_NO_SUCH_PROPERTY otherwise
 */
const char *
dpu_properties_get(dpu_properties_t dpu_properties, const char *name);

bool
dpu_properties_get_idx(dpu_properties_t dpu_properties, unsigned int index, const char **name, const char **value, bool *used);

/**
 * @brief Simple utility function: print a configuration
 * @param dpu_properties the configuration
 */
void
dpu_properties_print(dpu_properties_t dpu_properties);

/**
 * @brief Log unused properties
 * @param dpu_properties the configuration
 * @param vc the output
 */
void
dpu_properties_log_unused(dpu_properties_t properties, struct verbose_control *vc);

#endif
