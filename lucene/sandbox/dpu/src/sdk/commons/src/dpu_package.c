/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

#define _GNU_SOURCE
#include <dlfcn.h>
#include <libgen.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

char *
fetch_package_library_directory()
{
    /* Searching the package directory:
     * We are using dladdr to locate the current shared library; which should be in something like PACKAGE_PATH/lib/.
     */
    Dl_info info;
    if ((dladdr(fetch_package_library_directory, &info) == 0) || (info.dli_fname == NULL)) {
        return NULL;
    }
    char *file = strdup(info.dli_fname);
    char *dir = dirname(file);
    char *result = strdup(dir);

    free(file);

    return result;
}
