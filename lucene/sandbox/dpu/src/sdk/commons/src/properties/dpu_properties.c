/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

#include "dpu_properties.h"
#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <static_verbose.h>
#include <dpu_log_utils.h>

typedef struct _dpu_properties_property {
    const char *name;
    const char *value;
    bool used;
} * _dpu_properties_property_t;

typedef struct _dpu_props {
    unsigned int nr_properties;
    _dpu_properties_property_t properties;
} * _dpu_props_t;

static inline _dpu_props_t
this_properties(dpu_properties_t dpu_properties)
{
    return (_dpu_props_t)dpu_properties;
}

static inline void
reset_properties(_dpu_props_t properties)
{
    properties->nr_properties = 0;
    properties->properties = NULL;
}

dpu_properties_t
dpu_properties_create()
{
    _dpu_props_t properties = (_dpu_props_t)malloc(sizeof(struct _dpu_props));
    if (properties == NULL) {
        return DPU_PROPERTIES_INVALID;
    }

    reset_properties(properties);
    return (dpu_properties_t)properties;
}

void
dpu_properties_delete(dpu_properties_t dpu_properties)
{
    _dpu_props_t properties = this_properties(dpu_properties);
    if (properties->properties != NULL) {
        unsigned int each_property;
        for (each_property = 0; each_property < properties->nr_properties; each_property++) {
            free((void *)properties->properties[each_property].name);
            free((void *)properties->properties[each_property].value);
        }
        free(properties->properties);
    }

    reset_properties(properties);
    free(properties);
}

_dpu_properties_property_t
lookup_for_property_called(_dpu_props_t properties, const char *name)
{
    unsigned int each_property;

    /* No need to use hash-codes here, there will be very few properties,
     * strcmp will perfectly do the job.
     */
    for (each_property = 0; each_property < properties->nr_properties; each_property++) {
        if (strcmp(name, properties->properties[each_property].name) == 0) {
            return &(properties->properties[each_property]);
        }
    }

    return NULL;
}

const char *
dpu_properties_get(dpu_properties_t dpu_properties, const char *name)
{
    _dpu_properties_property_t property = lookup_for_property_called(this_properties(dpu_properties), name);
    if (property == NULL) {
        return DPU_PROPERTIES_NO_SUCH_PROPERTY;
    } else {
        property->used = true;
        return property->value;
    }
}

static inline dpu_properties_t
add_properties(_dpu_props_t properties, const char *name, const char *value)
{
    properties->properties = (_dpu_properties_property_t)realloc(
        properties->properties, (properties->nr_properties + 1) * sizeof(struct _dpu_properties_property));

    if (properties->properties == NULL) {
        return DPU_PROPERTIES_INVALID;
    }

    _dpu_properties_property_t new_property = &(properties->properties[properties->nr_properties]);

    new_property->name = (char *)malloc(strlen(name) + 1);
    if (new_property->name == NULL) {
        return DPU_PROPERTIES_INVALID;
    }

    new_property->value = (char *)malloc(strlen(value) + 1);
    if (new_property->value == NULL) {
        free((void *)new_property->name);
        return DPU_PROPERTIES_INVALID;
    }

    (void)strcpy((char *)new_property->name, name);
    (void)strcpy((char *)new_property->value, value);
    new_property->used = false;
    properties->nr_properties++;

    return (dpu_properties_t)properties;
}

static inline dpu_properties_t
update_properties(_dpu_props_t properties, _dpu_properties_property_t property, const char *value)
{
    property->value = (char *)realloc((void *)property->value, strlen(value) + 1);
    if (property->value == NULL) {
        return DPU_PROPERTIES_INVALID;
    } else {
        (void)strcpy((char *)property->value, value);
    }
    return (dpu_properties_t)properties;
}

dpu_properties_t
dpu_properties_add(dpu_properties_t dpu_properties, const char *name, const char *value)
{
    _dpu_properties_property_t property = lookup_for_property_called(this_properties(dpu_properties), name);
    if (property != NULL) {
        return update_properties(this_properties(dpu_properties), property, value);
    } else {
        return add_properties(this_properties(dpu_properties), name, value);
    }
}

void
dpu_properties_print(dpu_properties_t dpu_properties)
{
    unsigned int each_property;
    _dpu_props_t properties = this_properties(dpu_properties);

    for (each_property = 0; each_property < properties->nr_properties; each_property++) {
        (void)printf("'%s' = '%s'\n", properties->properties[each_property].name, properties->properties[each_property].value);
    }
}

bool
dpu_properties_get_idx(dpu_properties_t dpu_properties, unsigned int index, const char **name, const char **value, bool *used)
{
    _dpu_props_t properties = this_properties(dpu_properties);

    if (index >= properties->nr_properties) {
        return false;
    }

    *name = properties->properties[index].name;
    *value = properties->properties[index].value;
    *used = properties->properties[index].used;

    return true;
}

void
dpu_properties_log_unused(dpu_properties_t properties, struct verbose_control *vc)
{
    if (LOGI_ENABLED(vc)) {
        unsigned int property_idx = 0;
        const char *name;
        const char *value;
        bool used;

        while (true) {
            if (!dpu_properties_get_idx(properties, property_idx, &name, &value, &used)) {
                break;
            }

            if (!used) {
                LOG_FN_VC(INFO, vc, "unused profile property: %s (= %s)", name, value);
            }

            property_idx++;
        }
    }
}
