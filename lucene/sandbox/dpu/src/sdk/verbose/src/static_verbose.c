/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <dpu_attributes.h>

#include "static_verbose.h"
#include "verbose_config.h"

#define MAX_NR_CONTROLS 16

static pthread_mutex_t vcs_mutex = PTHREAD_MUTEX_INITIALIZER;
static struct verbose_config config;
static struct {
    unsigned int nr_controls;
    unsigned int max_nr_controls;
    struct verbose_control **controls;
} vcs;

void __attribute__((constructor)) __setup_verbose_config()
{
    verbose_config_setup(&config);
    vcs.nr_controls = 0;
    vcs.max_nr_controls = MAX_NR_CONTROLS;
    vcs.controls = calloc(vcs.max_nr_controls, sizeof(*(vcs.controls)));
}

void __attribute__((destructor)) __cleanup_verbose_config()
{
    if (vcs.controls != NULL) {
        for (unsigned int each_control = 0; each_control < vcs.nr_controls; each_control++) {
            verbose_control_clean(vcs.controls[each_control]);
            free(vcs.controls[each_control]);
        }
    }
    free(vcs.controls);
    vcs.controls = NULL;
    vcs.nr_controls = 0;

    verbose_config_clean(&config);
}

/*
 * Function to enable dpu-lldb to print all output in the client shell instead of the server (which is most of the time not
 * visible)
 */
void __API_SYMBOL__
set_verbose_output_file(FILE *output_file)
{
    pthread_mutex_lock(&vcs_mutex);
    config.output_file = output_file;
    config.manage_file_handle = false;
    for (unsigned int each_control = 0; each_control < vcs.nr_controls; ++each_control) {
        vcs.controls[each_control]->output_file = output_file;
    }
    pthread_mutex_unlock(&vcs_mutex);
}

__API_SYMBOL__ struct verbose_control *
get_verbose_control_for(const char *module_name)
{
    unsigned int nr_controls = vcs.nr_controls;

    for (unsigned int each_control = 0; each_control < nr_controls; ++each_control) {
        if (strcmp(module_name, vcs.controls[each_control]->module_name) == 0) {
            return vcs.controls[each_control];
        }
    }

    pthread_mutex_lock(&vcs_mutex);

    for (unsigned int each_control = nr_controls; each_control < vcs.nr_controls; ++each_control) {
        if (strcmp(module_name, vcs.controls[each_control]->module_name) == 0) {
            pthread_mutex_unlock(&vcs_mutex);
            return vcs.controls[each_control];
        }
    }

    if (vcs.nr_controls == vcs.max_nr_controls) {
        fprintf(stderr, "[ERROR] maximum number of vcs reached!\n");
        pthread_mutex_unlock(&vcs_mutex);
        return NULL;
    }

    vcs.controls[vcs.nr_controls] = (struct verbose_control *)malloc(sizeof(struct verbose_control));
    struct verbose_control *result = vcs.controls[vcs.nr_controls];
    verbose_control_setup(module_name, &config, result);
    vcs.nr_controls++;

    pthread_mutex_unlock(&vcs_mutex);

    return result;
}
