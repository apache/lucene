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

// TODO(sbrocard): increase version if needed
#define JNI_VERSION JNI_VERSION_1_1

// cached JNI lookups
static JNIEnv *env;

static jclass exClass;
static jclass nativeDpuSetClass;
static jclass dpuSystemClass;
static jclass dpuSystemExecutorClass;

static jfieldID nativeDpuSetField;
static jfieldID dpuSystemField;

static jclass byteBufferClass;
static jmethodID allocateDirectMethod;

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

static jint
throw_dpu_exception(JNIEnv *env, const char *message)
{
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
    jobject nativeDpuSet = (*env)->GetObjectField(env, dpuSystem, nativeDpuSetField);
    struct dpu_set_t set = build_native_set(env, nativeDpuSet);

    uint32_t nr_dpus = 0;
    THROW_ON_ERROR(dpu_get_nr_dpus(set, &nr_dpus));

    return (jint)nr_dpus;
}

typedef uint32_t index_t;
typedef uint8_t result_t;

#define MAX(a, b)                                                                                                                \
    ({                                                                                                                           \
        __typeof__(a) _a = (a);                                                                                                  \
        __typeof__(b) _b = (b);                                                                                                  \
        _a > _b ? _a : _b;                                                                                                       \
    })

/**
 * Retrieves metadata information.
 *
 * @param env The JNI environment.
 * @param set The DPU set.
 * @param nr_dpus The number of DPUs.
 * @param nr_queries The number of queries.
 * @param nr_segments The number of segments.
 *
 * @return The metadata information.
 */
struct metadata_t {
    index_t *results_size;
    index_t *results_size_lucene_segments;
    size_t max_nr_results;
    size_t total_nr_results;
} static get_metadata(JNIEnv *env, struct dpu_set_t set, uint32_t nr_dpus, jint nr_queries, jint nr_segments)
{
    /* Transfer and postprocess the results_index from the DPU */
    index_t(*results_size)[nr_dpus][nr_queries] = malloc(sizeof(index_t[nr_dpus][nr_queries]));

    struct dpu_set_t dpu;
    uint32_t each_dpu = 0;
    DPU_FOREACH (set, dpu, each_dpu) {
        THROW_ON_ERROR(dpu_prepare_xfer(dpu, results_size[each_dpu]));
    }
    THROW_ON_ERROR(dpu_push_xfer(set, DPU_XFER_FROM_DPU, "results_size", 0, sizeof(index_t[nr_queries]), DPU_XFER_DEFAULT));

    size_t max_nr_results = 0;
    size_t total_nr_results = 0;
    for (uint32_t i_dpu = 0; i_dpu < nr_dpus; ++i_dpu) {
        size_t nr_results = 0;
        for (jint i_query = 0; i_query < nr_queries; ++i_query) {
            nr_results += (*results_size)[i_dpu][i_query];
        }
        max_nr_results = MAX(max_nr_results, nr_results);
        total_nr_results += nr_results;
    }

    /* Transfer the results_size_lucene_segments from the DPU */
    index_t(*results_size_lucene_segments)[nr_dpus][nr_queries][nr_segments]
        = malloc(sizeof(index_t[nr_dpus][nr_queries][nr_segments]));

    DPU_FOREACH (set, dpu, each_dpu) {
        THROW_ON_ERROR(dpu_prepare_xfer(dpu, results_size_lucene_segments[each_dpu]));
    }
    THROW_ON_ERROR(dpu_push_xfer(
        set, DPU_XFER_FROM_DPU, "results_size_lucene_segments", 0, sizeof(index_t[nr_queries][nr_segments]), DPU_XFER_DEFAULT));

    return (struct metadata_t) { .results_size = (index_t *)results_size,
        .results_size_lucene_segments = (index_t *)results_size_lucene_segments,
        .max_nr_results = max_nr_results,
        .total_nr_results = total_nr_results };
}

// TODO(sbrocard): use compound literals
typedef struct sg_xfer_context {
    index_t *results_size;
    index_t *results_size_lucene_segments;
    result_t **block_addresses;
    jint nr_queries;
    jint nr_segments;
} sg_xfer_context;

/**
 * Computes the block addresses where the results will be transferred.
 *
 * @param sc_args The scatter-gather transfer context.
 * @param dpu_results The base address of the DPU results.
 * @param nr_dpus The number of DPUs.
 */
static void
compute_block_addresses(struct sg_xfer_context *sc_args, const jbyte *dpu_results, uint32_t nr_dpus)
{
    /* Unpack the arguments */
    const jint nr_queries = sc_args->nr_queries;
    const jint nr_segments = sc_args->nr_segments;
    result_t *(*block_addresses)[nr_dpus][nr_queries][nr_segments]
        = (result_t * (*)[nr_dpus][nr_queries][nr_segments]) sc_args->block_addresses;
    index_t(*results_size_lucene_segments)[nr_dpus][nr_queries][nr_segments]
        = (index_t(*)[nr_dpus][nr_queries][nr_segments])sc_args->results_size_lucene_segments;

    /* Compute the block addresses */
    result_t *curr_blk_addr = (result_t *)dpu_results;
    for (jint i_qu = 0; i_qu < nr_queries; ++i_qu) {
        for (jint i_seg = 0; i_seg < nr_segments; ++i_seg) {
            for (uint32_t i_dpu = 0; i_dpu < nr_dpus; ++i_dpu) {
                (*block_addresses)[i_dpu][i_qu][i_seg] = curr_blk_addr;
                curr_blk_addr += (*results_size_lucene_segments)[i_dpu][i_qu][i_seg];
            }
        }
    }
}

/**
 * Callback function to retrieve a block from a DPU.
 *
 * @param out Pointer to the sg_block_info struct showing where the retrieved block will be stored.
 * @param i_dpu The index of the DPU to retrieve the block from.
 * @param i_query The index of the query to retrieve the block for.
 * @param args The scatter-gather transfer context.
 *
 * @return true if the block exists, false otherwise.
 */
static bool
get_block(struct sg_block_info *out, uint32_t i_dpu, uint32_t i_block, void *args)
{
    /* Unpack the arguments */
    sg_xfer_context *sc_args = (sg_xfer_context *)args;
    const uint32_t nr_queries = sc_args->nr_queries;
    const uint32_t nr_segments = sc_args->nr_segments;
    if (i_block >= nr_queries * nr_segments) {
        return false;
    }

    index_t(*results_size_lucene_segments)[nr_queries * nr_segments]
        = (index_t(*)[nr_queries * nr_segments]) sc_args->results_size_lucene_segments;
    result_t *(*block_addresses)[nr_queries * nr_segments] = (result_t * (*)[nr_queries * nr_segments]) sc_args->block_addresses;

    /* Set the output block */
    out->length = results_size_lucene_segments[i_dpu][i_block] * sizeof(result_t);
    out->addr = (uint8_t *)block_addresses[i_dpu][i_block];

    return true;
}

JNIEXPORT jobject JNICALL
Java_org_apache_lucene_sandbox_pim_DpuSystemExecutor_sgXferResults(JNIEnv *env, jobject this, jint nr_queries, jint nr_segments)
{
    jobject dpuSystem = (*env)->GetObjectField(env, this, dpuSystemField);
    jobject nativeDpuSet = (*env)->GetObjectField(env, dpuSystem, nativeDpuSetField);
    struct dpu_set_t set = build_native_set(env, nativeDpuSet);

    uint32_t nr_dpus = 0;
    dpu_get_nr_dpus(set, &nr_dpus);
    size_t total_nr_results = 0;
    size_t max_nr_results = 0;
    struct metadata_t metadata = get_metadata(env, set, nr_dpus, nr_queries, nr_segments);

    // Allocate a Java direct buffer
    jobject byteBuffer
        = (*env)->CallStaticObjectMethod(env, byteBufferClass, allocateDirectMethod, sizeof(result_t[total_nr_results]));

    // Get the address of the direct buffer
    jbyte *dpu_results = (*env)->GetDirectBufferAddress(env, byteBuffer);

    result_t **block_addresses = malloc(sizeof(result_t *[nr_dpus][nr_queries][nr_segments]));

    sg_xfer_context sc_args = { .results_size = metadata.results_size, .block_addresses = block_addresses };
    get_block_t get_block_info = { .f = &get_block, .args = &sc_args, .args_size = sizeof(sc_args) };

    compute_block_addresses(&sc_args, dpu_results, nr_dpus);

    // TODO(sbrocard): use callback to make transfer per rank to not share the max transfer size
    THROW_ON_ERROR(dpu_push_sg_xfer(set,
        DPU_XFER_FROM_DPU,
        "results_batch",
        0,
        sizeof(result_t[max_nr_results]),
        &get_block_info,
        DPU_SG_XFER_DISABLE_LENGTH_CHECK));

    free(metadata.results_size);
    free(metadata.results_size_lucene_segments);
    free(block_addresses);

    return byteBuffer;
}

/**
 * @brief Cache callback to avoid repeated JNI lookups
 */
void
cache_callback(JNIEnv *env)
{
    exClass = (*env)->FindClass(env, "com/upmem/dpu/DpuException");
    nativeDpuSetClass = (*env)->FindClass(env, "com/upmem/dpu/NativeDpuSet");
    dpuSystemClass = (*env)->FindClass(env, "com/upmem/dpu/DpuSystem");
    dpuSystemExecutorClass = (*env)->FindClass(env, "com/upmem/dpu/DpuSystemExecutor");

    dpuSystemField = (*env)->GetFieldID(env, dpuSystemExecutorClass, "dpuSystem", "Lcom/upmem/dpu/DpuSystem;");
    nativeDpuSetField = (*env)->GetFieldID(env, dpuSystemClass, "set", "Lcom/upmem/dpu/NativeDpuSet;");

    byteBufferClass = (*env)->FindClass(env, "java/nio/ByteBuffer");
    allocateDirectMethod = (*env)->GetStaticMethodID(env, byteBufferClass, "allocateDirect", "(I)Ljava/nio/ByteBuffer;");
}

/**
 * @brief Release callback to avoid memory leaks
 */
void
release_callback(JNIEnv *env)
{
    // Delete global references
    (*env)->DeleteGlobalRef(env, exClass);
    exClass = NULL;
    (*env)->DeleteGlobalRef(env, nativeDpuSetClass);
    nativeDpuSetClass = NULL;
    (*env)->DeleteGlobalRef(env, dpuSystemClass);
    dpuSystemClass = NULL;
    (*env)->DeleteGlobalRef(env, dpuSystemExecutorClass);
    dpuSystemExecutorClass = NULL;

    (*env)->DeleteGlobalRef(env, byteBufferClass);
    byteBufferClass = NULL;

    // jmethodIDs are safe to keep without an explicit global reference, for this reason, we don't need to delete the reference
    // either.
    dpuSystemField = NULL;
    nativeDpuSetField = NULL;
    allocateDirectMethod = NULL;
}

JNIEXPORT jint
JNI_OnLoad(JavaVM *vm, __attribute__((unused)) void *reserved)
{
    if ((*vm)->GetEnv(vm, (void **)&env, JNI_VERSION) != JNI_OK) {
        return JNI_ERR;
    }

    cache_callback(env);

    return JNI_VERSION;
}

JNIEXPORT void
JNI_OnUnload(__attribute__((unused)) JavaVM *vm, __attribute__((unused)) void *reserved)
{
    release_callback(env);

    env = NULL;
}