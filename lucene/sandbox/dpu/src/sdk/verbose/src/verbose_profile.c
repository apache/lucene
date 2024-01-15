/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

#define _DEFAULT_SOURCE // For strdup

#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <stdint.h>

#include "verbose_profile.h"

static char *
get_next_definition(char **source)
{
    char *ptr = *source;
    char *next_sep = strchr(ptr, ',');
    if (next_sep != NULL) {
        *next_sep = '\0';
        *source = next_sep + 1;
    } else {
        *source = ptr + strlen(ptr);
    }
    return ptr;
}

static verbose_log_level_t
get_log_level_from_string(const char *string)
{
    switch (string[0]) {
        default:
        case 'w':
        case 'W':
        case '1':
            return WARNING;

        case 'i':
        case 'I':
        case '2':
            return INFO;

        case 'd':
        case 'D':
        case '3':
            return DEBUG;

        case 'v':
        case 'V':
        case '4':
            return VERBOSE;

        case 's':
        case 'S':
        case '0':
            return SILENT;
    };
}

static void
setup_profile_from_string(struct verbose_profile_definition *definition, char *string)
{
    char *next_sep = strchr(string, ':');
    if (next_sep == NULL) {
        definition->module_name = strdup("default");
        definition->log_level = get_log_level_from_string(string);
    } else {
        char *module_name = string;
        char *log_level;
        // If someone types, for example: 'foo:3:xxx:' specify default level = W
        if ((uintptr_t)(next_sep - string) == strlen(string) - 1) {
            log_level = "W";
        } else {
            log_level = next_sep + 1;
            *next_sep = '\0';
        }
        definition->module_name = strdup(module_name);
        definition->log_level = get_log_level_from_string(log_level);
    }
}

bool
verbose_profile_setup(struct verbose_profile *profile, const char *source, char **logdir, char **logname, bool *logstdout)
{
    profile->nr_definitions = 0;
    profile->definitions = NULL;
    *logdir = NULL;
    *logname = NULL;
    *logstdout = false;

    char *copy = strdup(source), *ptr = copy;
    do {
        char *current_definition = get_next_definition(&ptr);

        if (strcmp(current_definition, "-") == 0) {
            *logstdout = true;
        } else if (strncmp(current_definition, "dir:", strlen("dir:")) == 0) {
            *logdir = strdup(current_definition + strlen("dir:"));
        } else if (strncmp(current_definition, "logname:", strlen("logname:")) == 0) {
            *logname = strdup(current_definition + strlen("logname:"));
        } else {
            profile->nr_definitions++;
            profile->definitions = (struct verbose_profile_definition *)realloc(
                profile->definitions, profile->nr_definitions * sizeof(struct verbose_profile_definition));
            if (profile->definitions == NULL) {
                (void)fprintf(stderr, "*** could not allocate new profile definition\n");
                free(copy);
                return false;
            }
            setup_profile_from_string(profile->definitions + (profile->nr_definitions - 1), current_definition);
        }
    } while ((strlen(ptr) != 0));
    free(copy);
    return true;
}

void
verbose_profile_clean(struct verbose_profile *profile)
{
    if (profile->definitions != NULL) {
        for (unsigned int each_profile = 0; each_profile < profile->nr_definitions; each_profile++) {
            if (profile->definitions[each_profile].module_name != NULL)
                free((void *)profile->definitions[each_profile].module_name);
        }
    }
    free(profile->definitions);
    profile->definitions = NULL;
    profile->nr_definitions = 0;
}

verbose_log_level_t
verbose_profile_for_module(const char *module_name, struct verbose_profile *profile)
{
    unsigned int each_profile;
    verbose_log_level_t by_default = WARNING;
    for (each_profile = 0; each_profile < profile->nr_definitions; each_profile++) {
        struct verbose_profile_definition *current_definition = profile->definitions + each_profile;
        if (strcmp(module_name, current_definition->module_name) == 0) {
            return current_definition->log_level;
        } else if (strcmp("default", current_definition->module_name) == 0) {
            by_default = current_definition->log_level;
        }
    }
    return by_default;
}
