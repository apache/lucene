/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

#include <stdlib.h>

#include <dpu_description.h>
#include <dpu_attributes.h>

static void
dpu_description_free(dpu_description_t description);

__API_SYMBOL__ void
dpu_acquire_description(dpu_description_t description)
{
    __sync_add_and_fetch(&description->refcount, 1);
}

__API_SYMBOL__ void
dpu_free_description(dpu_description_t description)
{
    if (__sync_sub_and_fetch(&description->refcount, 1) == 0)
        dpu_description_free(description);
}

static void
dpu_description_free(dpu_description_t description)
{
    description->_internals.free(description->_internals.data);
    free(description);
}
