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

#define _GNU_SOURCE
#include <dpu.h>           // for dpu_get_nr_dpus, dpu_prepare_xfer, dpu_pus...
#include <errno.h>         // for errno
#include <jni.h>           // for JNINativeInterface_, _jobject, JNIEnv, job...
#include <stdbool.h>       // for bool, false, true
#include <stddef.h>        // for NULL, size_t
#include <stdint.h>        // for uint32_t, uint64_t, uint8_t
#include <stdio.h>         // for asprintf, fprintf, stderr
#include <stdlib.h>        // for free, malloc, exit
#include <string.h>        // for strerror_r, memset

#include "topdocs_sync.h"  // for NORM_INVERSE_CACHE_SIZE, free_topdocs_sync
// IWYU pragma: no_include "jni_md.h"

// TODO(sbrocard): increase version if needed
#define JNI_VERSION JNI_VERSION_1_1

typedef uint32_t index_t;
typedef uint64_t result_t;

// TODO(sbrocard): use compound literals
typedef struct {
    void *results_size_lucene_segments; // index_t[nr_dpus][nr_queries_ub][nr_segments]
    void *block_addresses; // result_t *[nr_dpus][nr_queries][nr_segments]
    void *queries_indices; // jint[nr_queries]
    jint nr_queries;
    jint nr_segments;
} sg_xfer_context;

typedef struct {
    sg_xfer_context *sc_args;
    void *dpu_results; // result_t[total_nr_results]
    uint32_t nr_dpus;
    uint32_t nr_ranks;
    void *segments_indices; // jint[nr_queries][nr_segments]
} compute_block_addresses_context;

typedef struct {
    JNIEnv *env;
    jint *nr_hits_arr; // jint[nr_queries]
    jintArray nr_hits;
    jint *quant_factors_arr; // jint[nr_queries]
    jintArray quant_factors;
} release_int_arrays_ctx;

typedef struct {
    void *results_size_lucene_segments; // index_t[nr_dpus][nr_queries_ub][nr_segments]
    size_t max_nr_results;
    size_t total_nr_results;
} metadata_t;

// cached JNI lookups
// NOLINTBEGIN(cppcoreguidelines-avoid-non-const-global-variables)
static JNIEnv *cached_env;

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

static inline jint
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

#define MSG_LEN 100

#define CHECK_MALLOC(ptr)                                                                                                        \
    do {                                                                                                                         \
        if ((ptr) == NULL) {                                                                                                     \
            char msg_buff[MSG_LEN];                                                                                              \
            char *error_str = strerror_r(errno, msg_buff, MSG_LEN);                                                              \
            const char *_msg;                                                                                                    \
            int dummy __attribute__((unused));                                                                                   \
            dummy = asprintf((char **)&_msg, "malloc failed: %s\n", error_str);                                                    \
            throw_dpu_exception(env, _msg);                                                                                      \
            free((void *)_msg);                                                                                                  \
        }                                                                                                                        \
    } while (0)

#define SAFE_MALLOC(size)                                                                                                        \
    ({                                                                                                                           \
        void *_ptr = malloc(size);                                                                                               \
        CHECK_MALLOC(_ptr);                                                                                                      \
        _ptr;                                                                                                                    \
    })

#define CLEANUP(f) __attribute__((cleanup(f)))

#define MAX(a, b)                                                                                                                \
    ({                                                                                                                           \
        __typeof__(a) _a = (a);                                                                                                  \
        __typeof__(b) _b = (b);                                                                                                  \
        _a > _b ? _a : _b;                                                                                                       \
    })

#define UB(x) ((((size_t)(x) + 1U) & ~0x1U))

/* Init functions */

/**
 * @brief Cache callback to avoid repeated JNI lookups
 */
static inline void
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

JNIEXPORT jint
JNI_OnLoad(JavaVM *vm, __attribute__((unused)) void *reserved)
{
    if ((*vm)->GetEnv(vm, (void **)&cached_env, JNI_VERSION) != JNI_OK) {
        return JNI_ERR;
    }

    cache_callback(cached_env);

    return JNI_VERSION;
}

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
        set.list.nr_ranks = (uint32_t)nrRanks;
        set.list.ranks = (struct dpu_rank_t **)nativePointer;
    }

    return set;
}

static inline struct dpu_set_t
get_dpu_set(JNIEnv *env, jobject dpuSystemExecutor)
{
    jobject dpuSystem = (*env)->GetObjectField(env, dpuSystemExecutor, dpuSystemField);
    jobject nativeDpuSet = (*env)->GetObjectField(env, dpuSystem, nativeDpuSetField);
    return build_native_set(env, nativeDpuSet);
}

/* Cleanup functions */

/**
 * @brief Release callback to avoid memory leaks
 */
static inline void
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

JNIEXPORT void
JNI_OnUnload(__attribute__((unused)) JavaVM *vm, __attribute__((unused)) void *reserved)
{
    free_topdocs_sync();
    release_callback(cached_env);

    cached_env = NULL;
}

static inline void
cleanup_free(void *ptr)
{
    free(*(void **)ptr);
}

static inline void
cleanup_metadata(const metadata_t *metadata)
{
    free(metadata->results_size_lucene_segments);
}

static inline void
release_int_arrays(const release_int_arrays_ctx *ctx)
{
    (*ctx->env)->ReleaseIntArrayElements(ctx->env, ctx->nr_hits, ctx->nr_hits_arr, JNI_ABORT);
    (*ctx->env)->ReleaseIntArrayElements(ctx->env, ctx->quant_factors, ctx->quant_factors_arr, JNI_ABORT);
}

/* Test functions */

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

/* Main functions */

static inline size_t
compute_max_nr_results(uint32_t nr_dpus, jint nr_queries, index_t results_index[nr_dpus][UB(nr_queries)])
{
    size_t max_nr_results = 0;
    for (uint32_t i_dpu = 0; i_dpu < nr_dpus; ++i_dpu) {
        const size_t nr_results = results_index[i_dpu][nr_queries - 1];
        max_nr_results = MAX(max_nr_results, nr_results);
    }

    return max_nr_results;
}

static inline size_t
compute_total_nr_results(uint32_t nr_dpus, jint nr_queries, index_t results_index[nr_dpus][UB(nr_queries)])
{
    size_t total_nr_results = 0;
    for (uint32_t i_dpu = 0; i_dpu < nr_dpus; ++i_dpu) {
        const size_t nr_results = results_index[i_dpu][nr_queries - 1];
        total_nr_results += nr_results;
    }

    return total_nr_results;
}

static inline void
xfer_results_index(JNIEnv *env,
    struct dpu_set_t set,
    uint32_t nr_dpus,
    uint32_t nr_queries_ub,
    index_t results_index[nr_dpus][nr_queries_ub])
{
    struct dpu_set_t dpu;
    uint32_t each_dpu = 0;
    DPU_FOREACH (set, dpu, each_dpu) {
        THROW_ON_ERROR(dpu_prepare_xfer(dpu, &(results_index[each_dpu][0])));
    }

    // TODO(jlegriel): the fact that we compute the total number of results immediately after this call prevents to
    // do it asynchronously
    dpu_push_xfer(set, DPU_XFER_FROM_DPU, "results_index", 0, sizeof(index_t[nr_queries_ub]), DPU_XFER_DEFAULT);
}

static inline void
xfer_result_index_lucene_segments(JNIEnv *env,
    struct dpu_set_t set,
    uint32_t nr_dpus,
    uint32_t nr_queries_ub,
    jint nr_segments,
    index_t results_size_lucene_segments[nr_dpus][nr_queries_ub][nr_segments])
{
    struct dpu_set_t dpu;
    uint32_t each_dpu = 0;
    DPU_FOREACH (set, dpu, each_dpu) {
        THROW_ON_ERROR(dpu_prepare_xfer(dpu, &(results_size_lucene_segments[each_dpu][0][0])));
    }

    THROW_ON_ERROR(dpu_push_xfer(set,
        DPU_XFER_FROM_DPU,
        "results_index_lucene_segments",
        0,
        sizeof(index_t[nr_queries_ub][nr_segments]),
        DPU_XFER_DEFAULT));
}

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
static inline metadata_t
get_metadata(JNIEnv *env, struct dpu_set_t set, uint32_t nr_dpus, jint nr_queries, jint nr_segments)
{
    /* Transfer and postprocess the results_index from the DPU
     * Need to align the transfer to 8 bytes, so align the number of queries to an even number
     */
    uint32_t nr_queries_ub = UB(nr_queries);
    index_t(*results_index)[nr_dpus][nr_queries_ub] = SAFE_MALLOC(sizeof(*results_index));

    xfer_results_index(env, set, nr_dpus, nr_queries_ub, *results_index);

    size_t max_nr_results = compute_max_nr_results(nr_dpus, nr_queries, *results_index);
    size_t total_nr_results = compute_total_nr_results(nr_dpus, nr_queries, *results_index);

    /* Transfer the results_size_lucene_segments from the DPU */
    index_t(*results_size_lucene_segments)[nr_dpus][nr_queries_ub][nr_segments]
        = SAFE_MALLOC(sizeof(*results_size_lucene_segments));

    xfer_result_index_lucene_segments(env, set, nr_dpus, nr_queries_ub, nr_segments, *results_size_lucene_segments);

    return (metadata_t) { .results_size_lucene_segments = results_size_lucene_segments,
        .max_nr_results = max_nr_results,
        .total_nr_results = total_nr_results };
}

/**
 * Computes the block addresses where the results will be transferred.
 * The block addresses are computed in parallel for different queries.
 * We use the callback mechanism of UPMEM's SDK, which uses one thread for each rank in the
 * DPU system (40 ranks on a full system), and share the different queries id equally among threads.
 */
static dpu_error_t
compute_block_addresses(__attribute__((unused)) struct dpu_set_t set, uint32_t rank_id, void *args)
{

    compute_block_addresses_context *ctx = (compute_block_addresses_context *)args;
    sg_xfer_context *sc_args = ctx->sc_args;

    /* Unpack the arguments */
    const uint32_t nr_queries = (uint32_t)sc_args->nr_queries;
    const uint32_t nr_queries_ub = UB(nr_queries);
    const jint nr_segments = sc_args->nr_segments;
    const uint32_t nr_dpus = ctx->nr_dpus;
    result_t *(*block_addresses)[nr_dpus][nr_queries][nr_segments] = sc_args->block_addresses;
    index_t(*results_size_lucene_segments)[nr_dpus][nr_queries_ub][nr_segments] = sc_args->results_size_lucene_segments;

    jint(*queries_indices_table)[nr_queries] = sc_args->queries_indices;
    jint(*segments_indices_table)[nr_queries][nr_segments] = ctx->segments_indices;

    result_t *const dpu_results = ctx->dpu_results;

    uint32_t nr_ranks = ctx->nr_ranks;
    uint32_t nr_queries_for_call = nr_queries / nr_ranks;
    uint32_t remaining = nr_queries - nr_queries_for_call * nr_ranks;
    uint32_t query_id_start = (rank_id < remaining) ? rank_id * ++nr_queries_for_call : remaining + rank_id * nr_queries_for_call;
    uint32_t query_id_end = query_id_start + nr_queries_for_call;

    /* Compute the block addresses, queries indices and segments indices */
    for (uint32_t i_qu = query_id_start; i_qu < query_id_end; ++i_qu) {
        result_t *curr_blk_addr = dpu_results;
        for (jint i_seg = 0; i_seg < nr_segments; ++i_seg) {
            for (uint32_t i_dpu = 0; i_dpu < nr_dpus; ++i_dpu) {
                (*block_addresses)[i_dpu][i_qu][i_seg] = curr_blk_addr;
                curr_blk_addr += (*results_size_lucene_segments)[i_dpu][i_qu][i_seg];
            }
            (*segments_indices_table)[i_qu][i_seg] = (jint)(curr_blk_addr - dpu_results);
        }
        (*queries_indices_table)[i_qu] = (jint)(curr_blk_addr - dpu_results);
    }

    return DPU_OK;
}

/**
 * Prefix sum of the queries/segments indices computed in parallel separately for each query
 */
static inline void
prefix_sum_indices(compute_block_addresses_context *addr_ctx)
{
    /* Unpack the arguments */
    const jint nr_queries = addr_ctx->sc_args->nr_queries;
    const jint nr_segments = addr_ctx->sc_args->nr_segments;
    jint(*queries_indices_table)[nr_queries] = addr_ctx->sc_args->queries_indices;
    jint(*segments_indices_table)[nr_queries][nr_segments] = addr_ctx->segments_indices;

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
    const uint32_t nr_queries = (uint32_t)sc_args->nr_queries;
    const uint32_t nr_queries_ub = UB(nr_queries);
    const uint32_t nr_segments = (uint32_t)sc_args->nr_segments;

    if (i_block >= nr_queries * nr_segments) {
        return false;
    }

    index_t(*results_size_lucene_segments)[/*nr_dpus*/][nr_queries_ub][nr_segments] = sc_args->results_size_lucene_segments;
    result_t *(*block_addresses)[/*nr_dpus*/][nr_queries][nr_segments] = sc_args->block_addresses;
    jint(*queries_indices_table)[nr_queries] = sc_args->queries_indices;

    /* Set the output block */
    uint32_t i_qu = i_block / nr_segments;
    uint32_t i_seg = i_block % nr_segments;
    out->length = (uint32_t)((*results_size_lucene_segments)[i_dpu][i_qu][i_seg] * sizeof(result_t));
    jint offset = (i_qu) ? (*queries_indices_table)[i_qu - 1] : 0;
    out->addr = (uint8_t *)((*block_addresses)[i_dpu][i_qu][i_seg] + offset);

#ifndef NDEBUG
    if (out->addr == NULL) {
        (void)fprintf(
            stderr, "Block address is NULL, i_dpu=%u, i_block=%u, i_qu=%u, i_seg=%u, offset=%i\n", i_dpu, i_block, i_qu, i_seg, offset);
        (void)fprintf(stderr, "nr_queries=%u, nr_segments=%u\n", nr_queries, nr_segments);
        exit(1);
    }
#endif

    return true;
}

static inline void *
create_norm_inverse_array(JNIEnv *env, jint nr_queries, jobjectArray scorers)
{
    float(*norm_inverse)[nr_queries][NORM_INVERSE_CACHE_SIZE] = SAFE_MALLOC(sizeof(*norm_inverse));
    for (int i = 0; i < nr_queries; ++i) {
        // access scorers[i].cache (should be BM25Scorer)
        jobject object = (*env)->GetObjectArrayElement(env, scorers, i);
        jclass cls = (*env)->GetObjectClass(env, object);
        // TODO(jlegriel): assert that the class is BM25Scorer
        jfieldID cacheID = (*env)->GetFieldID(env, cls, "cache", "[F");
        jfloatArray cache = (*env)->GetObjectField(env, object, cacheID);
        jfloat *cache_arr = (*env)->GetFloatArrayElements(env, cache, 0);
        for (size_t j = 0; j < NORM_INVERSE_CACHE_SIZE; ++j) {
            (*norm_inverse)[i][j] = cache_arr[j];
        }
        (*env)->ReleaseFloatArrayElements(env, cache, cache_arr, JNI_ABORT);
    }

    return norm_inverse;
}

static inline jint
retrieve_return_buffers(JNIEnv *env,
    jobject sgReturn,
    size_t total_nr_results,
    jbyte **dpu_results,
    jbyte **queries_indices,
    jbyte **segments_indices)
{
    // Retrieve direct buffers where to store the results
    jobject byteBuffer = (*env)->GetObjectField(env, sgReturn, SGReturnByteBufferField);
    jobject byteBufferQueriesIndices = (*env)->GetObjectField(env, sgReturn, SGReturnQueriesIndicesField);
    jobject byteBufferSegmentsIndices = (*env)->GetObjectField(env, sgReturn, SGReturnSegmentsIndicesField);

    // check that the byteBuffer received is large enough to hold all results
    // if not return the size needed
    jlong cap = (*env)->GetDirectBufferCapacity(env, byteBuffer);
    if ((size_t)cap < total_nr_results * sizeof(result_t)) {
        return (jint)(total_nr_results * sizeof(result_t));
    }

    // Get the address of the direct buffer
    *dpu_results = (*env)->GetDirectBufferAddress(env, byteBuffer);
    *queries_indices = (*env)->GetDirectBufferAddress(env, byteBufferQueriesIndices);
    *segments_indices = (*env)->GetDirectBufferAddress(env, byteBufferSegmentsIndices);

    return 0;
}

static inline void
perform_topdocs_lower_bound_sync(JNIEnv *env,
    jint nr_queries,
    jintArray nr_hits,
    jintArray quant_factors,
    jobjectArray scorers,
    struct dpu_set_t set)
{
    jint *nr_hits_arr = (*env)->GetIntArrayElements(env, nr_hits, 0);
    jint *quant_factors_arr = (*env)->GetIntArrayElements(env, quant_factors, 0);
    CLEANUP(release_int_arrays)
    const release_int_arrays_ctx release_trigger = { env, nr_hits_arr, nr_hits, quant_factors_arr, quant_factors };

    // create norm inverse array using scorers
    CLEANUP(cleanup_free)
    const float(*norm_inverse)[nr_queries][NORM_INVERSE_CACHE_SIZE] = create_norm_inverse_array(env, nr_queries, scorers);

    // Perform the intermediate synchronizations for the topdocs lower bound
    THROW_ON_ERROR(topdocs_lower_bound_sync(set, nr_queries, nr_hits_arr, *norm_inverse, quant_factors_arr));
}

#ifndef NDEBUG
static inline void
check_block_addresses(JNIEnv *env,
    uint32_t nr_dpus,
    uint32_t nr_queries,
    uint32_t nr_segments,
    result_t *block_addresses[nr_dpus][nr_queries][nr_segments])
{
    for (uint32_t i_dpu = 0; i_dpu < nr_dpus; ++i_dpu) {
        for (uint32_t i_qu = 0; i_qu < nr_queries; ++i_qu) {
            for (uint32_t i_seg = 0; i_seg < nr_segments; ++i_seg) {
                if (block_addresses[i_dpu][i_qu][i_seg] == NULL) {
                    (void)fprintf(stderr, "Block address is NULL, i_dpu=%u, i_qu=%u, i_seg=%u\n", i_dpu, i_qu, i_seg);
                    (void)fprintf(stderr, "nr_queries=%u, nr_segments=%u\n", nr_queries, nr_segments);
                    THROW_ON_ERROR(DPU_ERR_SYSTEM);
                }
            }
        }
    }
}
#endif

static inline jint
build_block_info(JNIEnv *env,
    struct dpu_set_t set,
    jobject sgReturn,
    jint nr_queries,
    jint nr_segments,
    uint32_t nr_dpus,
    uint32_t nr_ranks,
    const metadata_t *metadata,
    result_t *block_addresses[nr_dpus][nr_queries][nr_segments],
    sg_xfer_context *sc_args,
    compute_block_addresses_context *addr_ctx,
    get_block_t *get_block_info)
{
    // Get the address of the direct buffer
    jbyte *dpu_results = NULL;
    jbyte *queries_indices = NULL;
    jbyte *segments_indices = NULL;
    jint size_needed
        = retrieve_return_buffers(env, sgReturn, metadata->total_nr_results, &dpu_results, &queries_indices, &segments_indices);
    if (size_needed != 0) {
        return size_needed;
    }

    *sc_args = (sg_xfer_context) { .nr_queries = nr_queries,
        .nr_segments = nr_segments,
        .results_size_lucene_segments = metadata->results_size_lucene_segments,
        .block_addresses = block_addresses,
        .queries_indices = queries_indices };

    *addr_ctx = (compute_block_addresses_context) { .sc_args = sc_args,
        .dpu_results = dpu_results,
        .nr_dpus = nr_dpus,
        .nr_ranks = nr_ranks,
        .segments_indices = segments_indices };

    THROW_ON_ERROR(dpu_callback(set, compute_block_addresses, addr_ctx, DPU_CALLBACK_DEFAULT));
    prefix_sum_indices(addr_ctx);

#ifndef NDEBUG
    check_block_addresses(env, nr_dpus, nr_queries, nr_segments, block_addresses);
#endif

    *get_block_info = (get_block_t) { .f = &get_block, .args = sc_args, .args_size = sizeof(*sc_args) };

    return size_needed;
}

static inline jint
perform_sg_xfer(JNIEnv *env, struct dpu_set_t set, jint nr_queries, jint nr_segments, jobject sgReturn)
{
    uint32_t nr_dpus = DPU_PROPERTY_OR_THROW(uint32_t, dpu_get_nr_dpus, set);
    uint32_t nr_ranks = DPU_PROPERTY_OR_THROW(uint32_t, dpu_get_nr_ranks, set);

    // Retrieve the metadata information
    CLEANUP(cleanup_metadata) const metadata_t metadata = get_metadata(env, set, nr_dpus, nr_queries, nr_segments);

    CLEANUP(cleanup_free)
    result_t *(*block_addresses)[nr_dpus][nr_queries][nr_segments] = SAFE_MALLOC(sizeof(*block_addresses));

    sg_xfer_context sc_args = {};
    compute_block_addresses_context addr_ctx = {};
    get_block_t get_block_info = {};
    jint size_needed = build_block_info(env,
        set,
        sgReturn,
        nr_queries,
        nr_segments,
        nr_dpus,
        nr_ranks,
        &metadata,
        *block_addresses,
        &sc_args,
        &addr_ctx,
        &get_block_info);
    if (size_needed != 0) {
        return size_needed;
    }

    size_t max_nr_results = metadata.max_nr_results;
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

    return size_needed;
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
    jobject sgReturn)
{
    struct dpu_set_t set = get_dpu_set(env, this);

    return perform_sg_xfer(env, set, nr_queries, nr_segments, sgReturn);
}

JNIEXPORT void JNICALL
/**
 * Run DPUs multiple times and computes a lower bound on score between each run.
 * This enables the DPUs to skip documents.
 *
 * @param env The JNI environment.
 * @param this The Java object calling this native method.
 * @param nr_queries The number of queries executed.
 * @param nr_segments The number of segments queried.
 * @param sgReturn the structure to store the results.
 * @return The results of the queries.
 */
Java_org_apache_lucene_sandbox_pim_DpuSystemExecutor_runDpusWithLowerBound(JNIEnv *env,
    jobject this,
    jint nr_queries,
    jintArray nr_hits,
    jintArray quant_factors,
    jobjectArray scorers)
{
    struct dpu_set_t set = get_dpu_set(env, this);

    perform_topdocs_lower_bound_sync(env, nr_queries, nr_hits, quant_factors, scorers, set);
}
