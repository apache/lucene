/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

#include "verbose_config.h"

#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>

static FILE *
mklog(const char *base_dir, const char *log_name)
{
    char *path = malloc(strlen(base_dir) + 256);
    FILE *result = NULL;

    if (path != NULL) {
        strcpy(path, base_dir);
        if (log_name) {
            strcat(path, "/");
            strcat(path, log_name);
        } else
            sprintf(path + strlen(base_dir), "/dpu-%u.log", getpid());
        result = fopen(path, "w");
        free(path);
    } else {
        (void)fprintf(stderr, "*** could not create temporary path name!\n");
    }
    return result;
}

static const char *standard_log_dir = "/var/log/dpu";
static const char *default_log_dir = ".";
#define __IS_WRITABLE_DIR(d) access(d, W_OK) == 0

static bool
guess_and_create_output_log_file(struct verbose_config *config,
    bool logstdout,
    char *verbose_dir,
    char *log_name,
    bool empty_upmem_verbose)
{
    if (logstdout) {
        config->output_file = stdout;
        config->manage_file_handle = false;
    } else if ((verbose_dir != NULL) && (__IS_WRITABLE_DIR(verbose_dir))) {
        config->output_file = mklog(verbose_dir, log_name);
        config->manage_file_handle = true;
    } else if (__IS_WRITABLE_DIR(standard_log_dir)) {
        config->output_file = mklog(standard_log_dir, log_name);
        config->manage_file_handle = true;
    } else if (!empty_upmem_verbose) {
        /*
         * Additional rule: do not want to pollute the customer's environment with
         * our internal cooking if she doesn't want to... Create the default file
         * if and only if at least one verbose output is required, i.e.
         * UPMEM_VERBOSE is defined.
         */
        config->output_file = mklog(default_log_dir, log_name);
        config->manage_file_handle = true;
    } else {
        // No risk of NPE: no trace will be produced at the end.
        config->output_file = stderr;
        config->manage_file_handle = false;
        return true;
    }
    if (config->output_file == NULL) {
        (void)fprintf(stderr, "*** could not create verbose output file\n");
    }
    return config->output_file != NULL;
}

bool
verbose_config_setup(struct verbose_config *config)
{
    const char *profile_string = getenv("UPMEM_VERBOSE");
    char *logdir;
    char *logname;
    bool logstdout;
    bool ret;
    bool empty_profile = profile_string == NULL;

    ret = verbose_profile_setup(&config->profile, empty_profile ? "" : profile_string, &logdir, &logname, &logstdout)
        && guess_and_create_output_log_file(config, logstdout, logdir, logname, empty_profile);

    if (logdir)
        free(logdir);
    if (logname)
        free(logname);

    return ret;
}

void
verbose_config_clean(struct verbose_config *config)
{
    if (config->manage_file_handle && (config->output_file != NULL)) {
        (void)fclose(config->output_file);
        config->output_file = NULL;
    }
    verbose_profile_clean(&config->profile);
}
