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

#include "topdocs_sync.h"

// TODO(sbrocard): increase version if needed
#define JNI_VERSION JNI_VERSION_1_1

// cached JNI lookups
// NOLINTBEGIN(cppcoreguidelines-avoid-non-const-global-variables)
static JNIEnv *env;

static jclass exClass;
static jclass nativeDpuSetClass;
static jclass dpuSystemClass;
static jclass dpuSystemExecutorClass;

static jfieldID nativeDpuSetField;
static jfieldID dpuSystemField;

static jclass byteBufferClass;
static jmethodID allocateDirectMethod;

static jfieldID SGReturnByteBufferField;
static jfieldID SGReturnQueriesIndicesField;
static jfieldID SGReturnSegmentsIndicesField;
// NOLINTEND(cppcoreguidelines-avoid-non-const-global-variables)

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
#define DPU_PROPERTY_OR_THROW(t, f, set)                                                                                         \
    ({                                                                                                                           \
        t _val;                                                                                                                  \
        THROW_ON_ERROR(f(set, &_val));                                                                                           \
        _val;                                                                                                                    \
    })

JNIEXPORT jint JNICALL
Java_org_apache_lucene_sandbox_pim_TestPimNativeInterface_getNrOfDpus(JNIEnv *env,
    __attribute__((unused)) jclass cls,
    jobject dpuSystem)
{
    jobject nativeDpuSet = (*env)->GetObjectField(env, dpuSystem, nativeDpuSetField);
    struct dpu_set_t set = build_native_set(env, nativeDpuSet);

    uint32_t nr_dpus = DPU_PROPERTY_OR_THROW(uint32_t, dpu_get_nr_dpus, set);

    return (jint)nr_dpus;
}

typedef uint32_t index_t;
typedef uint64_t result_t;

#define MAX(a, b)                                                                                                                \
    ({                                                                                                                           \
        __typeof__(a) _a = (a);                                                                                                  \
        __typeof__(b) _b = (b);                                                                                                  \
        _a > _b ? _a : _b;                                                                                                       \
    })

#define UB(x) ((((x) + 1U) >> 1U) << 1U)

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
    index_t *results_index;
    index_t *results_size_lucene_segments;
    size_t max_nr_results;
    size_t total_nr_results;
} static get_metadata(JNIEnv *env, struct dpu_set_t set, const uint32_t nr_dpus, const jint nr_queries, const jint nr_segments)
{
    /* Transfer and postprocess the results_index from the DPU
     * Need to align the transfer to 8 bytes, so align the number of queries to an even number
     */
    uint32_t nr_queries_ub = UB(nr_queries);
    index_t(*results_index)[nr_dpus][nr_queries_ub] = malloc(sizeof(index_t[nr_dpus][nr_queries_ub]));

    struct dpu_set_t dpu;
    uint32_t each_dpu = 0;
    DPU_FOREACH (set, dpu, each_dpu) {
        THROW_ON_ERROR(dpu_prepare_xfer(dpu, &((*results_index)[each_dpu][0])));
    }

    // TODO(jlegriel): the fact that we compute the total number of results immediately after this call prevents to
    // do it asynchronously
    dpu_push_xfer(set, DPU_XFER_FROM_DPU, "results_index", 0, sizeof(index_t[nr_queries_ub]), DPU_XFER_DEFAULT);

    size_t max_nr_results = 0;
    size_t total_nr_results = 0;
    for (uint32_t i_dpu = 0; i_dpu < nr_dpus; ++i_dpu) {
        size_t nr_results = 0;
        nr_results += (*results_index)[i_dpu][nr_queries - 1];
        max_nr_results = MAX(max_nr_results, nr_results);
        total_nr_results += nr_results;
    }

    /* Transfer the results_size_lucene_segments from the DPU */
    index_t(*results_size_lucene_segments)[nr_dpus][nr_queries_ub][nr_segments]
        = malloc(sizeof(index_t[nr_dpus][nr_queries_ub][nr_segments]));

    DPU_FOREACH (set, dpu, each_dpu) {
        THROW_ON_ERROR(dpu_prepare_xfer(dpu, &((*results_size_lucene_segments)[each_dpu][0][0])));
    }

    THROW_ON_ERROR(dpu_push_xfer(set,
        DPU_XFER_FROM_DPU,
        "results_index_lucene_segments",
        0,
        sizeof(index_t[nr_queries_ub][nr_segments]),
        DPU_XFER_DEFAULT));

    return (struct metadata_t) { .results_index = (index_t *)results_index,
        .results_size_lucene_segments = (index_t *)results_size_lucene_segments,
        .max_nr_results = max_nr_results,
        .total_nr_results = total_nr_results };
}

// TODO(sbrocard): use compound literals
typedef struct sg_xfer_context {
    index_t *results_index;
    index_t *results_size_lucene_segments;
    result_t **block_addresses;
    jbyte *queries_indices;
    jint nr_queries;
    jint nr_segments;
} sg_xfer_context;

struct compute_block_addresses_context {
    struct sg_xfer_context *sc_args;
    const jbyte *dpu_results;
    const uint32_t nr_dpus;
    uint32_t nr_ranks;
    jbyte *segments_indices;
};

/**
 * Computes the block addresses where the results will be transferred.
 * The block addresses are computed in parallel for different queries.
 * We use the callback mechanism of UPMEM's SDK, which uses one thread for each rank in the
 * DPU system (40 ranks on a full system), and share the different queries id equally among threads.
 */
static dpu_error_t
compute_block_addresses(__attribute__((unused)) struct dpu_set_t set, uint32_t rank_id, void *args)
{

    struct compute_block_addresses_context *ctx = (struct compute_block_addresses_context *)args;
    struct sg_xfer_context *sc_args = ctx->sc_args;

    /* Unpack the arguments */
    const jint nr_queries = sc_args->nr_queries;
    const uint32_t nr_queries_ub = UB(nr_queries);
    const jint nr_segments = sc_args->nr_segments;
    const uint32_t nr_dpus = ctx->nr_dpus;
    result_t *(*block_addresses)[nr_dpus][nr_queries][nr_segments]
        = (result_t * (*)[nr_dpus][nr_queries][nr_segments]) sc_args->block_addresses;
    index_t(*results_size_lucene_segments)[nr_dpus][nr_queries_ub][nr_segments]
        = (index_t(*)[nr_dpus][nr_queries_ub][nr_segments])sc_args->results_size_lucene_segments;

    jint(*queries_indices_table)[nr_queries] = (jint(*)[nr_queries])(sc_args->queries_indices);
    jint(*segments_indices_table)[nr_queries][nr_segments] = (jint(*)[nr_queries][nr_segments])(ctx->segments_indices);

    const jbyte *dpu_results = ctx->dpu_results;

    uint32_t nr_ranks = ctx->nr_ranks;
    uint32_t nr_queries_for_call = nr_queries / nr_ranks;
    uint32_t remaining = nr_queries - nr_queries_for_call * nr_ranks;
    uint32_t query_id_start = (rank_id < remaining)
        ? rank_id * (++nr_queries_for_call + 1)
        : remaining * (nr_queries_for_call + 1) + (rank_id - remaining) * nr_queries_for_call;
    uint32_t query_id_end = query_id_start + nr_queries_for_call;

    /* Compute the block addresses, queries indices and segments indices */
    for (jint i_qu = (jint)query_id_start; i_qu < query_id_end; ++i_qu) {
        result_t *curr_blk_addr = (result_t *)dpu_results;
        for (jint i_seg = 0; i_seg < nr_segments; ++i_seg) {
            for (uint32_t i_dpu = 0; i_dpu < nr_dpus; ++i_dpu) {
                (*block_addresses)[i_dpu][i_qu][i_seg] = curr_blk_addr;
                curr_blk_addr += (*results_size_lucene_segments)[i_dpu][i_qu][i_seg];
            }
            (*segments_indices_table)[i_qu][i_seg] = (jint)(curr_blk_addr - (result_t *)dpu_results);
        }
        (*queries_indices_table)[i_qu] = (jint)(curr_blk_addr - (result_t *)dpu_results);
    }

    return DPU_OK;
}

/**
 * Prefix sum of the queries/segments indices computed in parallel separately for each query
 */
static void
prefix_sum_indices(struct sg_xfer_context *sc_args,
    const jbyte *dpu_results,
    const uint32_t nr_dpus,
    jbyte *queries_indices,
    jbyte *segments_indices)
{

    /* Unpack the arguments */
    const jint nr_queries = sc_args->nr_queries;
    const jint nr_segments = sc_args->nr_segments;
    jint(*queries_indices_table)[nr_queries] = (jint(*)[nr_queries])queries_indices;
    jint(*segments_indices_table)[nr_queries][nr_segments] = (jint(*)[nr_queries][nr_segments])segments_indices;

    for (jint i_qu = 1; i_qu < nr_queries; ++i_qu) {
        (*queries_indices_table)[i_qu] += (*queries_indices_table)[i_qu - 1];
        for (jint i_seg = 0; i_seg < nr_segments; ++i_seg) {
            (*segments_indices_table)[i_qu][i_seg] += (*queries_indices_table)[i_qu - 1];
        }
    }
}

/**
 * Callback function to retrieve a block from a DPU.
 *
 * @param[out] out Pointer to the sg_block_info struct showing where the retrieved block will be stored.
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
    const uint32_t nr_queries_ub = UB(nr_queries);
    const uint32_t nr_segments = sc_args->nr_segments;

    if (i_block >= nr_queries * nr_segments) {
        return false;
    }

    index_t(*results_size_lucene_segments)[nr_queries_ub * nr_segments]
        = (index_t(*)[nr_queries_ub * nr_segments]) sc_args->results_size_lucene_segments;
    result_t *(*block_addresses)[nr_queries * nr_segments] = (result_t * (*)[nr_queries * nr_segments]) sc_args->block_addresses;
    jint(*queries_indices_table)[nr_queries] = (jint(*)[nr_queries])(sc_args->queries_indices);

    /* Set the output block */
    out->length = results_size_lucene_segments[i_dpu][i_block] * sizeof(result_t);
    uint32_t query_id = i_block / nr_segments;
    uint32_t offset = (query_id) ? (*queries_indices_table)[query_id - 1] * sizeof(result_t) : 0;
    out->addr = (uint8_t *)block_addresses[i_dpu][i_block] + offset;

    return true;
}

JNIEXPORT jint JNICALL
/**
 * Transfers the results of a set of queries executed on a set of segments from the DPU to the host.
 *
 * @param env The JNI environment.
 * @param this The Java object calling this native method.
 * @param nr_queries The number of queries executed.
 * @param nr_segments The number of segments queried.
 * @param sgReturn the structure to store the results.
 * @return The results of the queries.
 */
Java_org_apache_lucene_sandbox_pim_DpuSystemExecutor_sgXferResults(JNIEnv *env,
    jobject this,
    jint nr_queries,
    jint nr_segments,
    jintArray nr_hits,
    jintArray quant_factors,
    jobjectArray scorers,
    jobject sgReturn)
{
    jint res = 0;
    jobject dpuSystem = (*env)->GetObjectField(env, this, dpuSystemField);
    jobject nativeDpuSet = (*env)->GetObjectField(env, dpuSystem, nativeDpuSetField);
    struct dpu_set_t set = build_native_set(env, nativeDpuSet);

    uint32_t nr_dpus = DPU_PROPERTY_OR_THROW(uint32_t, dpu_get_nr_dpus, set);
    uint32_t nr_ranks = DPU_PROPERTY_OR_THROW(uint32_t, dpu_get_nr_ranks, set);

    jint *nr_hits_arr = (*env)->GetIntArrayElements(env, nr_hits, 0);
    jint *quant_factors_arr = (*env)->GetIntArrayElements(env, quant_factors, 0);

    // create norm inverse array using scorers
    float(*norm_inverse)[nr_queries][NORM_INVERSE_CACHE_SIZE] = malloc(sizeof(float[nr_queries][NORM_INVERSE_CACHE_SIZE]));
    for (int i = 0; i < nr_queries; ++i) {
        // access scorers[i].scorer.cache (should be BM25Scorer)
        jobject object = (*env)->GetObjectArrayElement(env, scorers, i);
        jclass cls = (*env)->GetObjectClass(env, object);
        jfieldID scorerID
            = (*env)->GetFieldID(env, cls, "scorer", "Lorg/apache/lucene/search/similarities/Similarity$SimScorer;");
        jobject scorer = (*env)->GetObjectField(env, object, scorerID);
        cls = (*env)->GetObjectClass(env, scorer);
        // TODO assert that the class is BM25Scorer
        jfieldID cacheID = (*env)->GetFieldID(env, cls, "cache", "[F");
        jfloatArray cache = (*env)->GetObjectField(env, scorer, cacheID);
        jfloat *cache_arr = (*env)->GetFloatArrayElements(env, cache, 0);
        for (int j = 0; j < NORM_INVERSE_CACHE_SIZE; ++j) {
            (*norm_inverse)[i][j] = cache_arr[j];
        }
        (*env)->ReleaseFloatArrayElements(env, cache, cache_arr, JNI_ABORT);
    }

    // Perform the intermediate synchronizations for the topdocs lower bound
    THROW_ON_ERROR(topdocs_lower_bound_sync(set, nr_hits_arr, (float *)norm_inverse, quant_factors_arr, nr_queries));

    // Retrieve the metadata information
    struct metadata_t metadata = get_metadata(env, set, nr_dpus, nr_queries, nr_segments);
    size_t total_nr_results = metadata.total_nr_results;
    size_t max_nr_results = metadata.max_nr_results;

    // Retrieve direct buffers where to store the results
    jobject byteBuffer = (*env)->GetObjectField(env, sgReturn, SGReturnByteBufferField);
    jobject byteBufferQueriesIndices = (*env)->GetObjectField(env, sgReturn, SGReturnQueriesIndicesField);
    jobject byteBufferSegmentsIndices = (*env)->GetObjectField(env, sgReturn, SGReturnSegmentsIndicesField);

    // Get the address of the direct buffer
    jbyte *dpu_results = (*env)->GetDirectBufferAddress(env, byteBuffer);
    jbyte *queries_indices = (*env)->GetDirectBufferAddress(env, byteBufferQueriesIndices);
    jbyte *segments_indices = (*env)->GetDirectBufferAddress(env, byteBufferSegmentsIndices);

    // check that the byteBuffer received is large enough to hold all results
    // if not return the size needed
    jlong cap = (*env)->GetDirectBufferCapacity(env, byteBuffer);
    if (cap < total_nr_results * sizeof(result_t)) {
        res = (jint)(total_nr_results * sizeof(result_t));
        goto end;
    }

    result_t **block_addresses = malloc(sizeof(result_t *[nr_dpus][nr_queries][nr_segments]));

    sg_xfer_context sc_args = { .nr_queries = nr_queries,
        .nr_segments = nr_segments,
        .results_index = metadata.results_index,
        .results_size_lucene_segments = metadata.results_size_lucene_segments,
        .block_addresses = block_addresses,
        .queries_indices = queries_indices };
    get_block_t get_block_info = { .f = &get_block, .args = &sc_args, .args_size = sizeof(sc_args) };

    struct compute_block_addresses_context addr_ctx = { .sc_args = &sc_args,
        .dpu_results = dpu_results,
        .nr_dpus = nr_dpus,
        .nr_ranks = nr_ranks,
        .segments_indices = segments_indices };

    THROW_ON_ERROR(dpu_callback(set, compute_block_addresses, &addr_ctx, DPU_CALLBACK_DEFAULT));
    prefix_sum_indices(&sc_args, dpu_results, nr_dpus, queries_indices, segments_indices);

    // TODO(sbrocard): use callback to make transfer per rank to not share the max transfer size
    if (max_nr_results != 0) {
        THROW_ON_ERROR(dpu_push_sg_xfer(set,
            DPU_XFER_FROM_DPU,
            "results_batch_sorted",
            0,
            sizeof(result_t[max_nr_results]),
            &get_block_info,
            DPU_SG_XFER_DISABLE_LENGTH_CHECK));
    }

    free(block_addresses);
end:
    (*env)->ReleaseIntArrayElements(env, nr_hits, nr_hits_arr, JNI_ABORT);
    (*env)->ReleaseIntArrayElements(env, quant_factors, quant_factors_arr, JNI_ABORT);
    free(norm_inverse);
    free(metadata.results_index);
    free(metadata.results_size_lucene_segments);
    return res;
}

/**
 * @brief Cache callback to avoid repeated JNI lookups
 */
void
cache_callback(JNIEnv *env)
{
    exClass = (*env)->FindClass(env, "org/apache/lucene/sandbox/sdk/DpuException");
    nativeDpuSetClass = (*env)->FindClass(env, "org/apache/lucene/sandbox/sdk/NativeDpuSet");
    dpuSystemClass = (*env)->FindClass(env, "org/apache/lucene/sandbox/sdk/DpuSystem");
    dpuSystemExecutorClass = (*env)->FindClass(env, "org/apache/lucene/sandbox/pim/DpuSystemExecutor");
    dpuSystemField = (*env)->GetFieldID(env, dpuSystemExecutorClass, "dpuSystem", "Lorg/apache/lucene/sandbox/sdk/DpuSystem;");
    nativeDpuSetField = (*env)->GetFieldID(env, dpuSystemClass, "set", "Lorg/apache/lucene/sandbox/sdk/NativeDpuSet;");

    byteBufferClass = (*env)->FindClass(env, "java/nio/ByteBuffer");
    allocateDirectMethod = (*env)->GetStaticMethodID(env, byteBufferClass, "allocateDirect", "(I)Ljava/nio/ByteBuffer;");

    jclass SGReturnClass = (*env)->FindClass(env, "org/apache/lucene/sandbox/pim/SGReturnPool$SGReturn");
    SGReturnByteBufferField = (*env)->GetFieldID(env, SGReturnClass, "byteBuffer", "Ljava/nio/ByteBuffer;");
    SGReturnQueriesIndicesField = (*env)->GetFieldID(env, SGReturnClass, "queriesIndices", "Ljava/nio/ByteBuffer;");
    SGReturnSegmentsIndicesField = (*env)->GetFieldID(env, SGReturnClass, "segmentsIndices", "Ljava/nio/ByteBuffer;");
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
    SGReturnByteBufferField = NULL;
    SGReturnQueriesIndicesField = NULL;
    SGReturnSegmentsIndicesField = NULL;
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
    free_topdocs_sync();
    release_callback(env);

    env = NULL;
}