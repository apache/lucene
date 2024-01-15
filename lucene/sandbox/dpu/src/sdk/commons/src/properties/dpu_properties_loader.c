/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

#define _GNU_SOURCE
#include "dpu_api_log.h"
#include "dpu_properties_loader.h"

#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <ctype.h>

/* Configuration names are normalized and do not exceed a given size. */
static char file_name_buffer[32];

/**
 * @param dpu_type the target DPU type
 * @return the configuration file name for the given DPU type
 */
static const char *
properties_file_name_for_type(const char *dpu_type)
{
    (void)sprintf(file_name_buffer, "dpu_%s.cfg", dpu_type);
    return file_name_buffer;
}

/**
 * @param dpu_type the target DPU type
 * @return the path to the user configuration file for this type, or NULL if malloc failed or HOME is undefined
 */
static const char *
user_path_to_properties_file_for_type_or_null(const char *dpu_type)
{
    char *user_home = getenv("HOME");
    const char *file_name = properties_file_name_for_type(dpu_type);
    if (user_home != NULL) {
        char *result = (char *)malloc(strlen(user_home) + strlen(file_name) + 2);
        if (result == NULL) {
            (void)fprintf(stderr, "*** Could not allocate memory to store user configuration file name!\n");
            fflush(stderr);
            return NULL;
        }
        (void)sprintf(result, "%s/%s", user_home, file_name);
        return result;
    } else {
        return NULL;
    }
}

/**
 * @brief A trim function to produce clean strings
 *
 * Copies the input string into a new storage (must be freed when consumed)
 * removing irrelevant characters.
 *
 * @param input the input string
 * @return a clean version of input, or NULL if an error occurs
 */
static char *
trim_string(const char *input)
{
    char *result = (char *)malloc(strlen(input) + 1);
    int each_char, each_out_char = 0;

    if (result == NULL) {
        (void)fprintf(stderr, "could not allocate memory to store string '%s'\n", input);
        fflush(stderr);
        return NULL;
    }

    for (each_char = 0; input[each_char] != '\0'; each_char++) {
        if (isprint(input[each_char]) && !isspace(input[each_char]))
            break;
    }

    for (; input[each_char] != '\0'; each_char++) {
        if (isprint(input[each_char])) {
            result[each_out_char++] = input[each_char];
        }
    }
    result[each_out_char] = '\0';
    for (each_out_char = each_out_char - 1; each_out_char > 0; each_out_char--) {
        if (!isspace(result[each_out_char]) && isprint(result[each_out_char]))
            break;
        result[each_out_char] = '\0';
    }
    return result;
}

/**
 * @brief Decodes one line from a configuration file
 * @param line the input line
 * @param properties updated with the configuration found in line, if any
 * @return false if something very wrong was found in the file, or if malloc failed
 */
static bool
decode_properties_line(char *line, dpu_properties_t properties)
{
    bool status = true;

    char *sep, *begining_of_line = line;
    if (strlen(line) < 1)
        return status;
    while ((*begining_of_line != '\0') && (isspace(*begining_of_line) || !isprint(*begining_of_line))) {
        begining_of_line++;
    }

    if (*begining_of_line == '\0')
        return status;
    if ((begining_of_line[0] == '/') && (begining_of_line[1] == '/'))
        return status;

    sep = strchr(begining_of_line, '=');
    if (sep != NULL) {
        /* Simply ignore lines that do not have an assignment.
         * That's pretty lazy, but it will work fine.
         */
        *sep = '\0';
        char *property_name = trim_string(begining_of_line);
        char *property_value = trim_string(sep + 1);
        if ((property_name != NULL) && (property_value != NULL)) {
            if ((property_name[0] != '\0') && (property_value[0] != '\0')) {
                if (dpu_properties_add(properties, property_name, property_value) == DPU_PROPERTIES_INVALID) {
                    (void)fprintf(stderr, "*** failed to create property '%s': no memory left!\n", property_name);
                    fflush(stderr);
                    status = false;
                }
            }
            free(property_name);
            free(property_value);
        }
    }

    return status;
}

/**
 * @brief Loads the contents of a configuration given by a test string.
 * @param text the properties text
 * @param properties filled with the loaded configuration
 * @return false if the text contains errors
 */
static bool
try_load_properties_from_text(const char *text, dpu_properties_t properties)
{
    char *sep = strchr(text, '\n');
    char *next = (char *)text;
    bool status = true;

    while (status && sep != NULL) {
        *sep = '\0';
        status = decode_properties_line(next, properties);
        if (!status) {
            (void)fprintf(stderr, "found illegal property definition '%s'\n", next);
            fflush(stderr);
        }
        next = sep + 1;
    }

    return status;
}

/**
 * @brief Loads the contents of a configuration file.
 * @param file the file
 * @param properties  filled with the loaded configuration
 * @return false if the file contains errors
 */
static bool
try_load_properties_from(FILE *file, dpu_properties_t properties)
{
    char *lineptr = NULL;
    size_t n = 0;
    bool status = true;

    while (status && (getline(&lineptr, &n, file) >= 0)) {
        status = decode_properties_line(lineptr, properties);
    }

    if (lineptr != NULL)
        free(lineptr);
    return status;
}

/**
 * @brief Tries to open a given configuration file and load the enclosed configuration.
 * @param path path to the file
 * @param properties  filled with the loaded configuration if found
 * @return whether the configuration was successfully loaded
 */
static bool
try_to_load_properties_file(const char *path, dpu_properties_t properties)
{
    FILE *inf = fopen(path, "r");
    if (inf != NULL) {
        (void)printf("loading configuration file '%s'\n", path);
        bool result = try_load_properties_from(inf, properties);
        (void)fclose(inf);
        return result;
    } else {
        return false;
    }
}

static const char property_separator_character = ',';
static const char *property_separator_string = ",";
static const char legacy_property_separator_character = '&';

static bool
try_load_properties_from_profile(char *text, dpu_properties_t properties)
{
    bool result = true;
    char *current_char = text;
    bool still_some_properties_remaining = true;

    if (*current_char == '\0')
        goto end;

    do {
        char *next_char = NULL;
        char *property_separator = strchr(current_char, property_separator_character);
        char *legacy_property_separator = strchr(current_char, legacy_property_separator_character);

        if ((property_separator == NULL)
            || ((legacy_property_separator != NULL) && (legacy_property_separator < property_separator))) {
            property_separator = legacy_property_separator;
        }

        if (property_separator != NULL) {
            *property_separator = '\0';
            next_char = property_separator + 1;
        } else {
            still_some_properties_remaining = false;
        }

        char *equal_separator = strchr(current_char, '=');

        if (equal_separator == NULL) {
            result = false;
            goto end;
        }

        *equal_separator = '\0';

        if (dpu_properties_add(properties, current_char, equal_separator + 1) == DPU_PROPERTIES_INVALID) {
            result = false;
            goto end;
        }

        current_char = next_char;
    } while (still_some_properties_remaining);

end:
    return result;
}

static void
populate_properties_with_default(dpu_properties_t properties, const char **default_properties)
{
    unsigned int each_property;
    for (each_property = 0; default_properties[each_property] != NULL; each_property++) {
        char *copy_of_property = (char *)malloc(strlen(default_properties[each_property]) + 1);
        (void)strcpy(copy_of_property, default_properties[each_property]);
        decode_properties_line(copy_of_property, properties);
        free(copy_of_property);
    }
}

/**
 * @brief Tries to load the DPU configuration from a bunch of potential configuration files.
 * @param primary            use this file first
 * @param default_properties text of the default properties definition
 * @param properties         filled with the loaded DPU configuration
 */
static void
load_properties_using_file(const char *primary, const char **default_properties, dpu_properties_t properties)
{
    populate_properties_with_default(properties, default_properties);
    if (primary != NULL) {
        (void)try_to_load_properties_file(primary, properties);
    }
}

/**
 * @brief Tries to load the configuration of a given type of DPU.
 * @param dpu_type the target DPU type
 * @param default_properties the default properties file
 * @param properties filled with the loaded configuration
 */
static void
load_properties_for_dpu_type(const char *dpu_type, const char **default_properties, dpu_properties_t dpu_properties)
{
    const char *user_properties_file = user_path_to_properties_file_for_type_or_null(dpu_type);
    load_properties_using_file(user_properties_file, default_properties, dpu_properties);

    if (user_properties_file != NULL)
        free((void *)user_properties_file);
}

dpu_properties_t
dpu_properties_load_for_type(const char *dpu_type, const char **default_properties)
{
    dpu_properties_t properties;

    properties = dpu_properties_create();
    if (properties == DPU_PROPERTIES_INVALID) {
        (void)fprintf(stderr, "could not create properties: no memory left!\n");
        fflush(stderr);
    } else {
        load_properties_for_dpu_type(dpu_type, default_properties, properties);
    }

    return properties;
}

dpu_properties_t
dpu_properties_load_from_string(const char *config)
{
    dpu_properties_t properties;

    properties = dpu_properties_create();
    if (properties == DPU_PROPERTIES_INVALID) {
        (void)fprintf(stderr, "could not create properties: no memory left!\n");
        fflush(stderr);
    } else if (config != NULL) {
        if (!try_load_properties_from_text(config, properties)) {
            dpu_properties_delete(properties);
            return DPU_PROPERTIES_INVALID;
        }
    }

    return properties;
}

dpu_properties_t
dpu_properties_load_from_profile(const char *profile)
{
    dpu_properties_t properties;
    char *profile_copy;
    char *full_profile = dpu_profile_concat3(getenv("UPMEM_PROFILE_BASE"), profile, getenv("UPMEM_PROFILE"));

    LOG_FN(VERBOSE, "\"%s\"", full_profile);

    properties = dpu_properties_create();
    if (properties == DPU_PROPERTIES_INVALID) {
        (void)fprintf(stderr, "could not create properties: no memory left!\n");
        fflush(stderr);
    } else if (full_profile != NULL) {
        if ((profile_copy = malloc(strlen(full_profile) + 1)) == NULL) {
            (void)fprintf(stderr, "could not create properties: no memory left!\n");
            fflush(stderr);
            dpu_properties_delete(properties);
            properties = DPU_PROPERTIES_INVALID;
        } else {
            strcpy(profile_copy, full_profile);

            if (!try_load_properties_from_profile(profile_copy, properties)) {
                dpu_properties_delete(properties);
                properties = DPU_PROPERTIES_INVALID;
            }

            free(profile_copy);
        }
    }

    free(full_profile);
    return properties;
}

char *
dpu_profile_concat3(const char *first, const char *second, const char *third)
{
    char *result;

    const char *first_profile = (first == NULL) ? "" : first;
    const char *second_profile = (second == NULL) ? "" : second;
    const char *third_profile = (third == NULL) ? "" : third;

    bool first_profile_is_empty = first_profile[0] == '\0';
    bool second_profile_is_empty = second_profile[0] == '\0';
    bool third_profile_is_empty = third_profile[0] == '\0';

    bool first_separator_needed = !first_profile_is_empty && !second_profile_is_empty;
    bool second_separator_needed = !third_profile_is_empty && (!first_profile_is_empty || !second_profile_is_empty);

    const char *first_separator = first_separator_needed ? property_separator_string : "";
    const char *second_separator = second_separator_needed ? property_separator_string : "";

    if (asprintf(&result, "%s%s%s%s%s", first_profile, first_separator, second_profile, second_separator, third_profile) == -1) {
        return NULL;
    }

    return result;
}
