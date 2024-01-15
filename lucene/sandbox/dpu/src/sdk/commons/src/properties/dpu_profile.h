/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

#ifndef DPU_PROFILE_H
#define DPU_PROFILE_H

#include <stdbool.h>
#include <stdint.h>
#include <errno.h>
#include <limits.h>
#include <ctype.h>
#include <string.h>
#include <strings.h>
#include "dpu_properties.h"
#include "dpu_profile_properties.h"

static inline bool
fetch_integer_property(dpu_properties_t properties, const char *name, uint32_t *value, uint32_t default_value)
{
    const char *value_string;
    char *tailptr;
    long int parsed;

    if ((value_string = dpu_properties_get(properties, name)) == DPU_PROPERTIES_NO_SUCH_PROPERTY) {
        *value = default_value;
        return true;
    }

    if ((*value_string == '\0') || isspace((unsigned char)*value_string)) {
        return false;
    }

    errno = 0;
    parsed = strtol(value_string, &tailptr, 0);

    if ((*tailptr != '\0') || (parsed > UINT_MAX) || (errno == ERANGE && parsed == LONG_MAX)
        || (errno == ERANGE && parsed == LONG_MIN)) {
        return false;
    }

    *value = (uint32_t)parsed;
    return true;
}

static inline bool
fetch_long_property(dpu_properties_t properties, const char *name, uint64_t *value, uint64_t default_value)
{
    const char *value_string;
    char *tailptr;
    unsigned long int parsed;

    if ((value_string = dpu_properties_get(properties, name)) == DPU_PROPERTIES_NO_SUCH_PROPERTY) {
        *value = default_value;
        return true;
    }

    if ((*value_string == '\0') || isspace((unsigned char)*value_string)) {
        return false;
    }

    errno = 0;
    parsed = strtoul(value_string, &tailptr, 0);

    if ((*tailptr != '\0') || (errno == ERANGE && parsed == ULONG_MAX)) {
        return false;
    }

    *value = (uint64_t)parsed;
    return true;
}

static inline bool
fetch_boolean_property(dpu_properties_t properties, const char *name, bool *value, bool default_value)
{
    const char *value_string;

    if ((value_string = dpu_properties_get(properties, name)) == DPU_PROPERTIES_NO_SUCH_PROPERTY) {
        *value = default_value;
        return true;
    }

    *value = (strcasecmp("false", value_string) != 0) && (strcasecmp("0", value_string) != 0);
    return true;
}

static inline bool
fetch_string_property(dpu_properties_t properties, const char *name, char **value, const char *default_value)
{
    const char *value_string;

    if ((value_string = dpu_properties_get(properties, name)) == DPU_PROPERTIES_NO_SUCH_PROPERTY) {
        value_string = default_value;
    }

    if (value_string == NULL) {
        *value = NULL;
    } else {
        if ((*value = malloc(strlen(value_string) + 1)) == NULL) {
            return false;
        }

        strcpy(*value, value_string);
    }

    return true;
}

#endif // DPU_PROFILE_H
