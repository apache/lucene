/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Style convention:
 * Java objects are named using camelCase
 * C objects are named using snake_case
 */

#include <stddef.h>
#define _GNU_SOURCE
#include <dpu.h>
#include <jni.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

static struct dpu_set_t
build_native_set(JNIEnv *env, jobject object)
{
    struct dpu_set_t set;
    memset(&set, 0, sizeof(set));

    jclass cls = (*env)->GetObjectClass(env, object);
    jfieldID isSingleDpuID = (*env)->GetFieldID(env, cls, "isSingleDpu", "Z");
    jfieldID nrRanksID = (*env)->GetFieldID(env, cls, "nrRanks", "I");
    jfieldID nativePointerID = (*env)->GetFieldID(env, cls, "nativePointer", "J");

    jlong nativePointer = (*env)->GetLongField(env, object, nativePointerID);
    jboolean isSingleDpu = (*env)->GetBooleanField(env, object, isSingleDpuID);

    if (isSingleDpu) {
        set.kind = DPU_SET_DPU;
        set.dpu = (struct dpu_t *)nativePointer;
    } else {
        jint nrRanks = (*env)->GetIntField(env, object, nrRanksID);

        set.kind = DPU_SET_RANKS;
        set.list.nr_ranks = nrRanks;
        set.list.ranks = (struct dpu_rank_t **)nativePointer;
    }

    return set;
}

static inline jobject
get_object_field(JNIEnv *env, jobject object, const char *fieldName, const char *fieldSignature)
{
    jclass cls = (*env)->GetObjectClass(env, object);
    jfieldID fieldID = (*env)->GetFieldID(env, cls, fieldName, fieldSignature);
    return (*env)->GetObjectField(env, object, fieldID);
}

static jint
throw_dpu_exception(JNIEnv *env, const char *message)
{
    jclass exClass = (*env)->FindClass(env, "com/upmem/dpu/DpuException");
    return (*env)->ThrowNew(env, exClass, message);
}

#define THROW_ON_ERROR_X(s, after)                                                                                               \
    do {                                                                                                                         \
        dpu_error_t _status = (s);                                                                                               \
        if (_status != DPU_OK) {                                                                                                 \
            const char *_msg = dpu_error_to_string(_status);                                                                     \
            throw_dpu_exception(env, _msg);                                                                                      \
            free((void *)_msg);                                                                                                  \
            after;                                                                                                               \
        }                                                                                                                        \
    } while (0)

#define THROW_ON_ERROR(s) THROW_ON_ERROR_X(s, {})
#define THROW_ON_ERROR_L(s, l) THROW_ON_ERROR_X(s, goto(l))

JNIEXPORT jint JNICALL
Java_org_apache_lucene_sandbox_pim_TestPimNativeInterface_getNrOfDpus(JNIEnv *env,
    __attribute__((unused)) jclass cls,
    jobject dpuSystem)
{
    jobject nativeDpuSet = get_object_field(env, dpuSystem, "set", "Lcom/upmem/dpu/NativeDpuSet;");
    struct dpu_set_t set = build_native_set(env, nativeDpuSet);

    uint32_t nr_dpus = 0;
    THROW_ON_ERROR(dpu_get_nr_dpus(set, &nr_dpus));

    return (jint)nr_dpus;
}

typedef uint32_t index_t;
typedef uint8_t result_t;

// TODO(sbrocard): use compound literals
typedef struct sg_xfer_context {
    index_t *metadata;
    result_t **queries_addresses;
    jint nr_queries;
} sg_xfer_context;

#define MAX(a, b)                                                                                                                \
    ({                                                                                                                           \
        __typeof__(a) _a = (a);                                                                                                  \
        __typeof__(b) _b = (b);                                                                                                  \
        _a > _b ? _a : _b;                                                                                                       \
    })

static index_t *
get_metadata(JNIEnv *env,
    struct dpu_set_t set,
    uint32_t nr_dpus,
    jint nr_queries,
    size_t *max_nr_results,
    size_t *total_nr_results)
{
    index_t(*metadata)[nr_dpus][nr_queries] = malloc(sizeof(index_t[nr_dpus][nr_queries]));

    struct dpu_set_t dpu;
    uint32_t each_dpu = 0;
    DPU_FOREACH (set, dpu, each_dpu) {
        THROW_ON_ERROR(dpu_prepare_xfer(dpu, metadata[each_dpu]));
    }
    THROW_ON_ERROR(dpu_push_xfer(set, DPU_XFER_FROM_DPU, "results_index", 0, sizeof(index_t[nr_queries]), DPU_XFER_DEFAULT));

    *max_nr_results = 0;
    *total_nr_results = 0;
    for (uint32_t i_dpu = 0; i_dpu < nr_dpus; ++i_dpu) {
        *max_nr_results = MAX(*max_nr_results, (*metadata)[i_dpu][nr_queries - 1]);
        *total_nr_results += (*metadata)[i_dpu][nr_queries - 1];
    }

    return (index_t *)metadata;
}

static void
compute_block_addresses(const struct sg_xfer_context *sc_args, const jbyte *dpu_results, uint32_t nr_dpus)
{
    jint nr_queries = sc_args->nr_queries;
    result_t *(*queries_addresses)[nr_dpus][nr_queries] = (result_t * (*)[nr_dpus][nr_queries]) sc_args->queries_addresses;
    index_t(*metadata)[nr_dpus][nr_queries] = (index_t(*)[nr_dpus][nr_queries])sc_args->metadata;

    result_t *current_query_address = (result_t *)dpu_results;
    for (jint i_query = 0; i_query < nr_queries; ++i_query) {
        (*queries_addresses)[0][i_query] = current_query_address;
        for (uint32_t i_dpu = 1; i_dpu < nr_dpus; ++i_dpu) {
            (*queries_addresses)[i_dpu][i_query] = current_query_address + (*metadata)[i_dpu - 1][i_query];
        }
        current_query_address = current_query_address + (*metadata)[nr_dpus - 1][i_query];
    }
}

static bool
get_block(struct sg_block_info *out, uint32_t i_dpu, uint32_t i_query, void *args)
{
    /* Unpack the arguments */
    sg_xfer_context *sc_args = (sg_xfer_context *)args;
    uint32_t nr_queries = sc_args->nr_queries;
    if (i_query >= nr_queries) {
        return false;
    }

    index_t(*metadata)[nr_queries] = (index_t(*)[nr_queries])sc_args->metadata;
    size_t nr_elements = metadata[i_dpu][i_query];
    result_t *(*queries_addresses)[nr_queries] = (result_t * (*)[nr_queries]) sc_args->queries_addresses;

    /* Set the output block */
    out->length = nr_elements * sizeof(result_t);
    out->addr = (uint8_t *)queries_addresses[i_dpu][i_query];

    return true;
}

JNIEXPORT jobject JNICALL
Java_org_apache_lucene_sandbox_pim_DpuSystemExecutor_sgXferResults(JNIEnv *env, jobject this, jint nr_queries)
{
    jobject dpuSystem = get_object_field(env, this, "dpuSystem", "Lcom/upmem/dpu/DpuSystem;");
    jobject nativeDpuSet = get_object_field(env, dpuSystem, "set", "Lcom/upmem/dpu/NativeDpuSet;");
    struct dpu_set_t set = build_native_set(env, nativeDpuSet);

    uint32_t nr_dpus = 0;
    dpu_get_nr_dpus(set, &nr_dpus);
    size_t total_nr_results = 0;
    size_t max_nr_results = 0;
    index_t *metadata = get_metadata(env, set, nr_dpus, nr_queries, &max_nr_results, &total_nr_results);
    size_t transfer_size = get_transfer_size(metadata);

    // TODO(sbrocard): check if we can create a ByteBuffer here and return it
    // ANSWER: yes we can but the underlying memory can't be freed by the GC so we pass a ByteBuffer from the Java side as an out
    // parameter instead
    // CORRECTION: actually we can just call ByteBuffer.allocateDirect from here so let's just get a ByteBuffer from the Java side

    // allocate direct buffer

    // Allocate a Java direct buffer
    jclass byteBufferClass = (*env)->FindClass(env, "java/nio/ByteBuffer");
    jmethodID allocateDirectMethod
        = (*env)->GetStaticMethodID(env, byteBufferClass, "allocateDirect", "(I)Ljava/nio/ByteBuffer;");
    jobject byteBuffer
        = (*env)->CallStaticObjectMethod(env, byteBufferClass, allocateDirectMethod, sizeof(result_t[total_nr_results]));

    // Get the address of the direct buffer
    jbyte *dpu_results = (*env)->GetDirectBufferAddress(env, byteBuffer);

    result_t **queries_addresses = malloc(sizeof(result_t *[nr_dpus][nr_queries]));

    sg_xfer_context sc_args = { .metadata = metadata, .queries_addresses = queries_addresses };
    get_block_t get_block_info = { .f = &get_block, .args = &sc_args, .args_size = sizeof(sc_args) };

    compute_block_addresses(&sc_args, dpu_results, nr_dpus);

    // TODO(sbrocard): use callback to make transfer per rank to not share the max transfer size
    THROW_ON_ERROR(
        dpu_push_sg_xfer(set, DPU_XFER_FROM_DPU, "results_batch", 0, sizeof(result_t[max_nr_results]), &get_block_info, DPU_SG_XFER_DISABLE_LENGTH_CHECK));

    free(metadata);
    free(queries_addresses);

    return byteBuffer;
}
