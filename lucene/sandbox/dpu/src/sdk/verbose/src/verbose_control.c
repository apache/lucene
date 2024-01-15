/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

#define _DEFAULT_SOURCE // For strdup

#include "verbose_control.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <time.h>
#include <unistd.h>

#define COLOR_RED_BOLD "\e[01;31m"
#define COLOR_NONE "\e[0m"

static struct tm *
get_current_time()
{
    time_t raw_time;
    time(&raw_time);
    return localtime(&raw_time);
}

static void
print(struct verbose_control *vcs, const char *log_type, const char *__restrict __format, va_list __arg, const char *color)
{
    FILE *out = vcs->output_file;
    bool color_enable = vcs->color_enable && color != NULL;
    char time_string[256];
    struct tm *now = get_current_time();
    strftime(time_string, sizeof(time_string), "%Y-%m-%d|%T", now);

    va_list dup_arg;
    va_copy(dup_arg, __arg);
    size_t needed = vsnprintf(NULL, 0, __format, dup_arg) + 1;
    va_end(dup_arg);

    char *content = malloc(needed);

    if (content == NULL) {
        return;
    }

    vsprintf(content, __format, __arg);

    fprintf(out,
        "%s:%s:%s: %s%s%s\n",
        vcs->module_name,
        log_type,
        time_string,
        color_enable ? color : "",
        content,
        color_enable ? COLOR_NONE : "");
    fflush(out);
    free(content);
}

static void
printw(struct verbose_control *vcs, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    print(vcs, "W", format, args, COLOR_RED_BOLD);
    va_end(args);
}

static void
printi(struct verbose_control *vcs, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    print(vcs, "I", format, args, NULL);
    va_end(args);
}

static void
printd(struct verbose_control *vcs, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    print(vcs, "D", format, args, NULL);
    va_end(args);
}

static void
printv(struct verbose_control *vcs, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    print(vcs, "V", format, args, NULL);
    va_end(args);
}

static void
apply_verbose_config_to(struct verbose_control *control, verbose_log_level_t log_level)
{
    control->printw = (log_level >= WARNING) ? printw : NULL;
    control->printi = (log_level >= INFO) ? printi : NULL;
    control->printd = (log_level >= DEBUG) ? printd : NULL;
    control->printv = (log_level >= VERBOSE) ? printv : NULL;
}

void
verbose_control_setup(const char *module_name, struct verbose_config *config, struct verbose_control *control)
{
    control->module_name = strdup(module_name);
    control->output_file = config->output_file;
    control->color_enable = isatty(fileno(config->output_file));
    verbose_log_level_t log_level = verbose_profile_for_module(module_name, &(config->profile));
    apply_verbose_config_to(control, log_level);
}

void
verbose_control_clean(struct verbose_control *control)
{
    if (control->module_name != NULL) {
        free((void *)control->module_name);
        control->module_name = NULL;
    }
}
