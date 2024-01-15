/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

#include "dpu_predef_programs.h"
#include "dpu_api_log.h"
#include "dpu_package.h"

#include <limits.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

static dpuinstruction_t *
fetch_program(const char *name, iram_size_t *size);

dpuinstruction_t *
fetch_core_dump_program(iram_size_t *size)
{
    return fetch_program("coreDump", size);
}

dpuinstruction_t *
fetch_restore_registers_program(iram_size_t *size)
{
    return fetch_program("restoreRegisters", size);
}

dpuinstruction_t *
fetch_mram_access_program(iram_size_t *size)
{
    return fetch_program("accessMramFromDpu", size);
}

dpuinstruction_t *
fetch_internal_reset_program(iram_size_t *size)
{
    return fetch_program("internalStateReset", size);
}

#define RELATIVE_PATH_TO_PROGRAM_DIR "/../share/upmem/include/misc/"
#define PROGRAM_EXT ".bin"

static dpuinstruction_t *
fetch_program(const char *name, iram_size_t *size)
{
    LOG_FN(DEBUG, "%s", name);

    char buffer[PATH_MAX];
    const char *dir = fetch_package_library_directory();
    if (dir == NULL) {
        LOG_FN(WARNING, "ERROR: cannot locate shared library");
        return NULL;
    }

    if ((strlen(dir) + strlen(RELATIVE_PATH_TO_PROGRAM_DIR) + strlen(name) + strlen(PROGRAM_EXT) + 1) > PATH_MAX) {
        LOG_FN(WARNING, "ERROR: path is too long");
        free((void *)dir);
        return NULL;
    }
    sprintf(buffer, "%s" RELATIVE_PATH_TO_PROGRAM_DIR "%s" PROGRAM_EXT, dir, name);
    LOG_FN(VERBOSE, "looking for file: %s", buffer);
    free((void *)dir);

    FILE *file = fopen(buffer, "rb");
    if (file == NULL) {
        LOG_FN(WARNING, "ERROR: cannot find file");
        return NULL;
    }
    LOG_FN(VERBOSE, "found file: %s", buffer);
    fseek(file, 0L, SEEK_END);
    size_t file_size = ftell(file);
    rewind(file);

    dpuinstruction_t *instructions;
    if ((file_size % sizeof(*instructions)) != 0) {
        LOG_FN(WARNING, "ERROR: file size is invalid");
        fclose(file);
        return NULL;
    }
    *size = file_size / sizeof(*instructions);

    if ((instructions = malloc(file_size)) == NULL) {
        fclose(file);
        return NULL;
    }

    iram_size_t read_size = (iram_size_t)fread(instructions, sizeof(*instructions), *size, file);
    if (read_size != *size) {
        LOG_FN(WARNING, "ERROR: did not read expected size of instructions");
        fclose(file);
        return NULL;
    }

    fclose(file);
    return instructions;
}
