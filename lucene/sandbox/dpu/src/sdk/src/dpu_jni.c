/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

#define _GNU_SOURCE
#include "dpu_custom.h"
#include "dpu_rank.h"
#include "dpu_target.h"
#include "dpu_config.h"
#include "dpu_description.h"
#include "dpu_log_internals.h"
#include "dpu_management.h"
#include <dpu_error.h>
#include <dpu_program.h>
#include <dpu_types.h>
#include <dpu_elf.h>
#include <dpu.h>
#include <jni.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>

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

static jobject
build_java_set(JNIEnv *env, jclass cls, struct dpu_set_t set)
{
    struct dpu_set_t rank, dpu;

    jmethodID ctorID = (*env)->GetMethodID(env, cls, "<init>", "()V");
    jfieldID childrenID = (*env)->GetFieldID(env, cls, "children", "Ljava/util/List;");
    jfieldID isSingleDpuID = (*env)->GetFieldID(env, cls, "isSingleDpu", "Z");
    jfieldID nrRanksID = (*env)->GetFieldID(env, cls, "nrRanks", "I");
    jfieldID nativePointerID = (*env)->GetFieldID(env, cls, "nativePointer", "J");

    jclass listCls = (*env)->FindClass(env, "java/util/List");
    jmethodID listAdd = (*env)->GetMethodID(env, listCls, "add", "(Ljava/lang/Object;)Z");

    jobject jset = (*env)->NewObject(env, cls, ctorID);
    jobject jsetChildren = (*env)->GetObjectField(env, jset, childrenID);

    (*env)->SetBooleanField(env, jset, isSingleDpuID, false);
    (*env)->SetIntField(env, jset, nrRanksID, set.list.nr_ranks);
    (*env)->SetLongField(env, jset, nativePointerID, (jlong)set.list.ranks);

    DPU_RANK_FOREACH (set, rank) {
        jobject jrank = (*env)->NewObject(env, cls, ctorID);
        jobject jrankChildren = (*env)->GetObjectField(env, jrank, childrenID);

        (*env)->SetBooleanField(env, jrank, isSingleDpuID, false);
        (*env)->SetIntField(env, jrank, nrRanksID, rank.list.nr_ranks);
        (*env)->SetLongField(env, jrank, nativePointerID, (jlong)rank.list.ranks);

        DPU_FOREACH (rank, dpu) {
            jobject jdpu = (*env)->NewObject(env, cls, ctorID);
            (*env)->SetBooleanField(env, jdpu, isSingleDpuID, true);
            (*env)->SetLongField(env, jdpu, nativePointerID, (jlong)dpu.dpu);

            (*env)->CallObjectMethod(env, jrankChildren, listAdd, jdpu);
        }

        (*env)->CallObjectMethod(env, jsetChildren, listAdd, jrank);
    }

    return jset;
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
#define THROW_ON_ERROR_L(s, l) THROW_ON_ERROR_X(s, goto l)

JNIEXPORT jobject JNICALL
Java_com_upmem_dpu_NativeDpuSet_allocate(JNIEnv *env, jclass cls, jint nr_dpus, jstring profile)
{
    struct dpu_set_t set;

    const char *c_profile = (*env)->GetStringUTFChars(env, profile, 0);

    THROW_ON_ERROR_L(dpu_alloc(nr_dpus, c_profile, &set), error);

    (*env)->ReleaseStringUTFChars(env, profile, c_profile);

    return build_java_set(env, cls, set);

error:
    (*env)->ReleaseStringUTFChars(env, profile, c_profile);
    return NULL;
}

JNIEXPORT jobject JNICALL
Java_com_upmem_dpu_NativeDpuSet_allocateRanks(JNIEnv *env, jclass cls, jint nr_ranks, jstring profile)
{
    struct dpu_set_t set;

    const char *c_profile = (*env)->GetStringUTFChars(env, profile, 0);

    THROW_ON_ERROR_L(dpu_alloc_ranks(nr_ranks, c_profile, &set), error);

    (*env)->ReleaseStringUTFChars(env, profile, c_profile);

    return build_java_set(env, cls, set);

error:
    (*env)->ReleaseStringUTFChars(env, profile, c_profile);
    return NULL;
}

static jobject
build_java_description(JNIEnv *env, dpu_description_t description)
{
    jclass cls = (*env)->FindClass(env, "com/upmem/dpu/DpuDescription");

    jmethodID constructor = (*env)->GetMethodID(env, cls, "<init>", "(IIIIIIIIIII)V");

    jint chip_id = description->hw.signature.chip_id;
    jint backend_id = description->type;
    jint nr_of_cis = description->hw.topology.nr_of_control_interfaces;
    jint nr_of_dpus_per_ci = description->hw.topology.nr_of_dpus_per_control_interface;
    jint mram_size = description->hw.memories.mram_size;
    jint wram_size = description->hw.memories.wram_size;
    jint iram_size = description->hw.memories.iram_size;
    jint nr_of_threads = description->hw.dpu.nr_of_threads;
    jint nr_of_atomic_bits = description->hw.dpu.nr_of_atomic_bits;
    jint nr_of_notify_bits = description->hw.dpu.nr_of_notify_bits;
    jint nr_of_work_registers_per_thread = description->hw.dpu.nr_of_work_registers_per_thread;

    return (*env)->NewObject(env,
        cls,
        constructor,
        chip_id,
        backend_id,
        nr_of_cis,
        nr_of_dpus_per_ci,
        mram_size,
        wram_size,
        iram_size,
        nr_of_threads,
        nr_of_atomic_bits,
        nr_of_notify_bits,
        nr_of_work_registers_per_thread);
}

JNIEXPORT jobject JNICALL
Java_com_upmem_dpu_NativeDpuSet_descriptionFor(JNIEnv *env, __attribute__((unused)) jclass cls, jstring profile)
{
    const char *c_profile = (*env)->GetStringUTFChars(env, profile, 0);
    dpu_description_t description;

    THROW_ON_ERROR_L(dpu_get_profile_description(c_profile, &description), error);

    (*env)->ReleaseStringUTFChars(env, profile, c_profile);

    jobject jdescription = build_java_description(env, description);

    dpu_free_description(description);

    return jdescription;

error:
    (*env)->ReleaseStringUTFChars(env, profile, c_profile);
    return NULL;
}

JNIEXPORT jobject JNICALL
Java_com_upmem_dpu_NativeDpuSet_description(JNIEnv *env, jobject this)
{
    struct dpu_set_t set = build_native_set(env, this);
    struct dpu_rank_t *rank;

    if (set.kind == DPU_SET_DPU) {
        rank = set.dpu->rank;
    } else {
        rank = set.list.ranks[0];
    }

    dpu_description_t description = dpu_get_description(rank);
    return build_java_description(env, description);
}

JNIEXPORT void JNICALL
Java_com_upmem_dpu_NativeDpuSet_reset(JNIEnv *env, jobject this)
{
    struct dpu_set_t set = build_native_set(env, this);

    if (set.kind == DPU_SET_DPU) {
        THROW_ON_ERROR(dpu_reset_dpu(set.dpu));
    } else {
        for (uint32_t each_rank = 0; each_rank < set.list.nr_ranks; ++each_rank) {
            THROW_ON_ERROR_L(dpu_reset_rank(set.list.ranks[each_rank]), end);
        }
    }
end:
    return;
}

JNIEXPORT jobject JNICALL
Java_com_upmem_dpu_NativeDpuSet_load(JNIEnv *env, jobject this, jstring executable)
{
    jobject jprogram, jsymbols;
    struct dpu_program_t *program;
    struct dpu_set_t set = build_native_set(env, this);
    const char *c_exec = (*env)->GetStringUTFChars(env, executable, 0);

    THROW_ON_ERROR_L(dpu_load(set, c_exec, &program), error);

    (*env)->ReleaseStringUTFChars(env, executable, c_exec);

    jclass mapCls = (*env)->FindClass(env, "java/util/HashMap");
    jmethodID mapCtorId = (*env)->GetMethodID(env, mapCls, "<init>", "()V");
    jmethodID mapPutId = (*env)->GetMethodID(env, mapCls, "put", "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");
    jsymbols = (*env)->NewObject(env, mapCls, mapCtorId);

    jclass symbolCls = (*env)->FindClass(env, "com/upmem/dpu/DpuSymbol");
    jmethodID symbolCtorId = (*env)->GetMethodID(env, symbolCls, "<init>", "(IILjava/lang/String;)V");

    for (uint32_t each_symbol = 0; each_symbol < program->symbols->nr_symbols; ++each_symbol) {
        dpu_elf_symbol_t *symbol = &program->symbols->map[each_symbol];
        jstring jname = (*env)->NewStringUTF(env, symbol->name);
        jobject jsymbol = (*env)->NewObject(env, symbolCls, symbolCtorId, symbol->value, symbol->size, jname);
        (*env)->CallObjectMethod(env, jsymbols, mapPutId, jname, jsymbol);
    }

    jclass cls = (*env)->FindClass(env, "com/upmem/dpu/DpuProgramInfo");
    jmethodID ctorID = (*env)->GetMethodID(env, cls, "<init>", "(Ljava/util/Map;)V");
    jprogram = (*env)->NewObject(env, cls, ctorID, jsymbols);

    return jprogram;

error:
    (*env)->ReleaseStringUTFChars(env, executable, c_exec);
    return NULL;
}

static struct dpu_symbol_t
build_native_symbol(JNIEnv *env, jobject object)
{
    struct dpu_symbol_t symbol;

    jclass symbolCls = (*env)->FindClass(env, "com/upmem/dpu/DpuSymbol");
    jfieldID symbolAddressID = (*env)->GetFieldID(env, symbolCls, "address", "I");
    jfieldID symbolSizeID = (*env)->GetFieldID(env, symbolCls, "size", "I");

    symbol.address = (*env)->GetIntField(env, object, symbolAddressID);
    symbol.size = (*env)->GetIntField(env, object, symbolSizeID);

    return symbol;
}

struct jarray_release_ctx {
    JavaVM *jvm;
    uint32_t nr_arrays;
    jbyteArray *arrays;
    jbyte **buffers;
};

static void
free_release_ctx(struct jarray_release_ctx *ctx)
{
    if (ctx != NULL) {
        free(ctx->arrays);
        free(ctx->buffers);
        free(ctx);
    }
}

static dpu_error_t
callback_release_jarray(struct dpu_set_t __attribute__((unused)) set, uint32_t __attribute__((unused)) idx, void *ctx)
{
    struct jarray_release_ctx *ctxt = ctx;
    bool detach = false;
    JavaVM *jvm = ctxt->jvm;
    JNIEnv *env;

    if ((*jvm)->GetEnv(jvm, (void **)&env, JNI_VERSION_1_8) == JNI_EDETACHED) {
        detach = true;
        (*jvm)->AttachCurrentThread(jvm, (void **)&env, NULL);
    }

    for (uint32_t each_array = 0; each_array < ctxt->nr_arrays; ++each_array) {
        (*env)->ReleaseByteArrayElements(env, ctxt->arrays[each_array], ctxt->buffers[each_array], 0);
        (*env)->DeleteGlobalRef(env, ctxt->arrays[each_array]);
    }

    free_release_ctx(ctxt);

    if (detach) {
        (*jvm)->DetachCurrentThread(jvm);
    }

    return DPU_OK;
}

static dpu_error_t
prepare_release_callback(JNIEnv *env, jbyteArray *arrays, jbyte **buffers, uint32_t nr_arrays, struct jarray_release_ctx **ctxt)
{
    dpu_error_t status = DPU_OK;
    struct jarray_release_ctx *ctx = malloc(sizeof(*ctx));
    if (ctx == NULL) {
        status = DPU_ERR_SYSTEM;
        goto end;
    }

    (*env)->GetJavaVM(env, &ctx->jvm);
    ctx->nr_arrays = nr_arrays;
    ctx->arrays = calloc(nr_arrays, sizeof(*(ctx->arrays)));
    if (ctx->arrays == NULL) {
        status = DPU_ERR_SYSTEM;
        goto free_ctx;
    }

    ctx->buffers = calloc(nr_arrays, sizeof(*(ctx->buffers)));
    if (ctx->buffers == NULL) {
        status = DPU_ERR_SYSTEM;
        goto free_arrays;
    }

    for (uint32_t each_array = 0; each_array < nr_arrays; ++each_array) {
        jobject ref = (*env)->NewGlobalRef(env, arrays[each_array]);
        ctx->arrays[each_array] = ref;
        ctx->buffers[each_array] = buffers[each_array];
    }

    *ctxt = ctx;
    return DPU_OK;

free_arrays:
    free(ctx->arrays);
free_ctx:
    free(ctx);
end:
    return status;
}

JNIEXPORT void JNICALL
Java_com_upmem_dpu_NativeDpuSet_broadcast__Ljava_lang_String_2_3BZ(JNIEnv *env,
    jobject this,
    jstring jsymbol,
    jbyteArray jarray,
    jboolean async)
{
    struct dpu_set_t set = build_native_set(env, this);
    const char *symbol = (*env)->GetStringUTFChars(env, jsymbol, 0);
    dpu_xfer_flags_t flags = async ? DPU_XFER_ASYNC : DPU_XFER_DEFAULT;
    struct jarray_release_ctx *ctx = NULL;

    jsize length = (*env)->GetArrayLength(env, jarray);
    jbyte *jbuffer = (*env)->GetByteArrayElements(env, jarray, 0);
    THROW_ON_ERROR_L(dpu_broadcast_to(set, symbol, 0, jbuffer, length, flags), error);

    (*env)->ReleaseStringUTFChars(env, jsymbol, symbol);

    if (async) {
        THROW_ON_ERROR_L(prepare_release_callback(env, &jarray, &jbuffer, 1, &ctx), free_ctx);
        THROW_ON_ERROR_L(
            dpu_callback(
                set, callback_release_jarray, ctx, DPU_CALLBACK_ASYNC | DPU_CALLBACK_NONBLOCKING | DPU_CALLBACK_SINGLE_CALL),
            free_ctx);
    } else {
        (*env)->ReleaseByteArrayElements(env, jarray, jbuffer, 0);
    }

    return;

error:
    (*env)->ReleaseByteArrayElements(env, jarray, jbuffer, 0);
free_ctx:
    free_release_ctx(ctx);
    (*env)->ReleaseStringUTFChars(env, jsymbol, symbol);
}

JNIEXPORT void JNICALL
Java_com_upmem_dpu_NativeDpuSet_broadcast__Lcom_upmem_dpu_DpuSymbol_2_3BZ(JNIEnv *env,
    jobject this,
    jobject jsymbol,
    jbyteArray jarray,
    jboolean async)
{
    struct dpu_set_t set = build_native_set(env, this);
    struct dpu_symbol_t symbol = build_native_symbol(env, jsymbol);
    dpu_xfer_flags_t flags = async ? DPU_XFER_ASYNC : DPU_XFER_DEFAULT;
    struct jarray_release_ctx *ctx = NULL;

    jsize length = (*env)->GetArrayLength(env, jarray);
    jbyte *jbuffer = (*env)->GetByteArrayElements(env, jarray, 0);
    THROW_ON_ERROR_L(dpu_broadcast_to_symbol(set, symbol, 0, jbuffer, length, flags), error);

    if (async) {
        THROW_ON_ERROR_L(prepare_release_callback(env, &jarray, &jbuffer, 1, &ctx), free_ctx);
        THROW_ON_ERROR_L(
            dpu_callback(
                set, callback_release_jarray, ctx, DPU_CALLBACK_ASYNC | DPU_CALLBACK_NONBLOCKING | DPU_CALLBACK_SINGLE_CALL),
            free_ctx);
    } else {
        (*env)->ReleaseByteArrayElements(env, jarray, jbuffer, 0);
    }

    return;

error:
    (*env)->ReleaseByteArrayElements(env, jarray, jbuffer, 0);
free_ctx:
    free_release_ctx(ctx);
}

JNIEXPORT void JNICALL 
Java_com_upmem_dpu_NativeDpuSet_broadcast__Ljava_lang_String_2_3BIIIZ (JNIEnv * env, 
    jobject this, 
    jstring jsymbol, 
    jbyteArray jarray, 
    jint jstart, 
    jint length, 
    jint joffset, 
    jboolean async) {

    struct dpu_set_t set = build_native_set(env, this);
    const char *symbol = (*env)->GetStringUTFChars(env, jsymbol, 0);
    dpu_xfer_flags_t flags = async ? DPU_XFER_ASYNC : DPU_XFER_DEFAULT;
    struct jarray_release_ctx *ctx = NULL;

    jbyte *jbuffer = (*env)->GetByteArrayElements(env, jarray, 0);
    THROW_ON_ERROR_L(dpu_broadcast_to(set, symbol, joffset, jbuffer + jstart, length, flags), error);

    (*env)->ReleaseStringUTFChars(env, jsymbol, symbol);

    if (async) {
        THROW_ON_ERROR_L(prepare_release_callback(env, &jarray, &jbuffer, 1, &ctx), free_ctx);
        THROW_ON_ERROR_L(
            dpu_callback(
                set, callback_release_jarray, ctx, DPU_CALLBACK_ASYNC | DPU_CALLBACK_NONBLOCKING | DPU_CALLBACK_SINGLE_CALL),
            free_ctx);
    } else {
        (*env)->ReleaseByteArrayElements(env, jarray, jbuffer, 0);
    }

    return;

error:
    (*env)->ReleaseByteArrayElements(env, jarray, jbuffer, 0);
free_ctx:
    free_release_ctx(ctx);
    (*env)->ReleaseStringUTFChars(env, jsymbol, symbol);
}

JNIEXPORT void JNICALL
Java_com_upmem_dpu_NativeDpuSet_copy__Ljava_lang_String_2_3_3BIZZ(JNIEnv *env,
    jobject this,
    jstring jsymbol,
    jobjectArray jarrays,
    jint size,
    jboolean to_dpu,
    jboolean async)
{
    struct dpu_set_t set = build_native_set(env, this);
    dpu_xfer_t xfer = to_dpu ? DPU_XFER_TO_DPU : DPU_XFER_FROM_DPU;
    dpu_xfer_flags_t flags = async ? DPU_XFER_ASYNC : DPU_XFER_DEFAULT;
    struct jarray_release_ctx *ctx = NULL;
    uint32_t nr_buffers = (*env)->GetArrayLength(env, jarrays);
    jbyteArray arrays[nr_buffers];
    jbyte *buffers[nr_buffers];
    uint32_t nr_dpus;

    const char *symbol = (*env)->GetStringUTFChars(env, jsymbol, 0);
    THROW_ON_ERROR_L(dpu_get_nr_dpus(set, &nr_dpus), end);

    if (nr_buffers != nr_dpus) {
        throw_dpu_exception(env, "the number of buffers should match the numer of DPUs");
        goto end;
    }

    for (uint32_t each_buffer = 0; each_buffer < nr_buffers; ++each_buffer) {
        jbyteArray jarray = (*env)->GetObjectArrayElement(env, jarrays, each_buffer);
        jboolean iscopy;
        jbyte *jbuffer = (*env)->GetByteArrayElements(env, jarray,&iscopy);
        arrays[each_buffer] = jarray;
        buffers[each_buffer] = jbuffer;
    }

    bool valid_size = (size > 0);
    bool length_initialized = false;
    jsize length;

    struct dpu_set_t dpu;
    uint32_t each_dpu;
    DPU_FOREACH (set, dpu, each_dpu) {
        jbyteArray jarray = arrays[each_dpu];
        jbyte *jbuffer = buffers[each_dpu];
        jsize buffer_length = (*env)->GetArrayLength(env, jarray);

        if(valid_size) {
          if (size > buffer_length) {
            throw_dpu_exception(env, "all buffers should have length larger than the size specified");
            return;
          }
        }
        else if (!length_initialized) {
            length_initialized = true;
            length = buffer_length;
        } else if (length != buffer_length) {
          throw_dpu_exception(env, "all buffers should have the same length");
          return;
        }

        THROW_ON_ERROR_L(dpu_prepare_xfer(dpu, jbuffer), error);
    }

    if (length_initialized) {
        THROW_ON_ERROR_L(dpu_push_xfer(set, xfer, symbol, 0, length, flags), error);
    }
    else if(valid_size) {
        THROW_ON_ERROR_L(dpu_push_xfer(set, xfer, symbol, 0, size, flags), error);
    }

    if (async) {
        THROW_ON_ERROR_L(prepare_release_callback(env, arrays, buffers, nr_buffers, &ctx), end);
        if(to_dpu)
          THROW_ON_ERROR_L(
              dpu_callback(
                set, callback_release_jarray, ctx, DPU_CALLBACK_ASYNC | DPU_CALLBACK_NONBLOCKING | DPU_CALLBACK_SINGLE_CALL),
              end);
        else  {
          THROW_ON_ERROR_L(
              dpu_callback(
                set, callback_release_jarray, ctx, DPU_CALLBACK_ASYNC | DPU_CALLBACK_SINGLE_CALL),
              end);
        }
    } else {
    error:
        for (uint32_t each_buffer = 0; each_buffer < nr_buffers; ++each_buffer) {
            (*env)->ReleaseByteArrayElements(env, arrays[each_buffer], buffers[each_buffer], 0);
        }
    }

end:
    (*env)->ReleaseStringUTFChars(env, jsymbol, symbol);
}

JNIEXPORT void JNICALL Java_com_upmem_dpu_NativeDpuSet_copy__Ljava_lang_String_2_3Ljava_nio_ByteBuffer_2IZZ
  (JNIEnv * env, jobject this, jstring jsymbol, jobjectArray jarrays, jint size, jboolean to_dpu, jboolean async) {

    struct dpu_set_t set = build_native_set(env, this);
    dpu_xfer_t xfer = to_dpu ? DPU_XFER_TO_DPU : DPU_XFER_FROM_DPU;
    dpu_xfer_flags_t flags = async ? DPU_XFER_ASYNC : DPU_XFER_DEFAULT;
    uint32_t nr_buffers = (*env)->GetArrayLength(env, jarrays);
    jbyteArray arrays[nr_buffers];
    char *buffers[nr_buffers];
    uint32_t nr_dpus;

    const char *symbol = (*env)->GetStringUTFChars(env, jsymbol, 0);
    THROW_ON_ERROR_L(dpu_get_nr_dpus(set, &nr_dpus), end);

    if (nr_buffers != nr_dpus) {
        throw_dpu_exception(env, "the number of buffers should match the numer of DPUs");
        goto end;
    }

    for (uint32_t each_buffer = 0; each_buffer < nr_buffers; ++each_buffer) {
        jobject jarray = (*env)->GetObjectArrayElement(env, jarrays, each_buffer);
        if(jarray == NULL) {
          buffers[each_buffer] = NULL;
        }
        else {
          char* jbuffer = (*env)->GetDirectBufferAddress(env, jarray);
          buffers[each_buffer] = jbuffer;
        }
        arrays[each_buffer] = jarray;
    }

    bool valid_size = (size > 0);
    bool length_initialized = false;
    jsize length;

    struct dpu_set_t dpu;
    uint32_t each_dpu;
    DPU_FOREACH (set, dpu, each_dpu) {
        jobject jarray = arrays[each_dpu];
        char *jbuffer = buffers[each_dpu];
        if(jbuffer == NULL) continue;
        jsize buffer_length = (*env)->GetDirectBufferCapacity(env, jarray);

        if(valid_size) {
          if (size > buffer_length) {
            throw_dpu_exception(env, "all buffers should have length larger than the size specified");
            return;
          }
        }
        else if (!length_initialized) {
            length_initialized = true;
            length = buffer_length;
        } else if (length != buffer_length) {
          throw_dpu_exception(env, "all buffers should have the same length");
          return;
        }

        THROW_ON_ERROR_L(dpu_prepare_xfer(dpu, jbuffer), end);
    }

    if (length_initialized) {
        THROW_ON_ERROR_L(dpu_push_xfer(set, xfer, symbol, 0, length, flags), end);
    }
    else if(valid_size) {
        THROW_ON_ERROR_L(dpu_push_xfer(set, xfer, symbol, 0, size, flags), end);
    }

end:
    (*env)->ReleaseStringUTFChars(env, jsymbol, symbol);
  }

JNIEXPORT void JNICALL
Java_com_upmem_dpu_NativeDpuSet_copy__Lcom_upmem_dpu_DpuSymbol_2_3_3BIZZ(JNIEnv *env,
    jobject this,
    jobject jsymbol,
    jobjectArray jarrays,
    jint size,
    jboolean to_dpu,
    jboolean async)
{
    struct dpu_set_t set = build_native_set(env, this);
    struct dpu_symbol_t symbol = build_native_symbol(env, jsymbol);
    dpu_xfer_t xfer = to_dpu ? DPU_XFER_TO_DPU : DPU_XFER_FROM_DPU;
    dpu_xfer_flags_t flags = async ? DPU_XFER_ASYNC : DPU_XFER_DEFAULT;
    struct jarray_release_ctx *ctx = NULL;
    uint32_t nr_buffers = (*env)->GetArrayLength(env, jarrays);
    jbyteArray arrays[nr_buffers];
    jbyte *buffers[nr_buffers];
    uint32_t nr_dpus;

    THROW_ON_ERROR_L(dpu_get_nr_dpus(set, &nr_dpus), end);

    if (nr_buffers != nr_dpus) {
        throw_dpu_exception(env, "the number of buffers should match the numer of DPUs");
        goto end;
    }

    for (uint32_t each_buffer = 0; each_buffer < nr_buffers; ++each_buffer) {
        jbyteArray jarray = (*env)->GetObjectArrayElement(env, jarrays, each_buffer);
        jbyte *jbuffer = (*env)->GetByteArrayElements(env, jarray, 0);
        arrays[each_buffer] = jarray;
        buffers[each_buffer] = jbuffer;
    }

    bool valid_size = (size > 0);
    bool length_initialized = false;
    jsize length;

    struct dpu_set_t dpu;
    uint32_t each_dpu;
    DPU_FOREACH (set, dpu, each_dpu) {
        jbyteArray jarray = arrays[each_dpu];
        jbyte *jbuffer = buffers[each_dpu];
        jsize buffer_length = (*env)->GetArrayLength(env, jarray);

        if(valid_size) {
          if (size > buffer_length) {
            throw_dpu_exception(env, "all buffers should have length larger than the size specified");
            return;
          }
        }
        else if (!length_initialized) {
            length_initialized = true;
            length = buffer_length;
        } else if (length != buffer_length) {
          throw_dpu_exception(env, "all buffers should have the same length");
          return;
        }

        THROW_ON_ERROR_L(dpu_prepare_xfer(dpu, jbuffer), error);
    }

    if (length_initialized) {
        THROW_ON_ERROR_L(dpu_push_xfer_symbol(set, xfer, symbol, 0, length, flags), error);
    }
    else if(valid_size) {
        THROW_ON_ERROR_L(dpu_push_xfer_symbol(set, xfer, symbol, 0, size, flags), error);
    }

    if (async) {
        THROW_ON_ERROR_L(prepare_release_callback(env, arrays, buffers, nr_buffers, &ctx), end);
        if(to_dpu)
          THROW_ON_ERROR_L(
              dpu_callback(
                set, callback_release_jarray, ctx, DPU_CALLBACK_ASYNC | DPU_CALLBACK_NONBLOCKING | DPU_CALLBACK_SINGLE_CALL),
              end);
        else
          THROW_ON_ERROR_L(
              dpu_callback(
                set, callback_release_jarray, ctx, DPU_CALLBACK_ASYNC | DPU_CALLBACK_SINGLE_CALL),
              end);
    } else {
    error:
        for (uint32_t each_buffer = 0; each_buffer < nr_buffers; ++each_buffer) {
            (*env)->ReleaseByteArrayElements(env, arrays[each_buffer], buffers[each_buffer], 0);
        }
    }

end:
    if(!async)
      free_release_ctx(ctx);
    return;
}

JNIEXPORT void JNICALL Java_com_upmem_dpu_NativeDpuSet_copy__Lcom_upmem_dpu_DpuSymbol_2_3Ljava_nio_ByteBuffer_2IZZ
  (JNIEnv *env, jobject this, jobject jsymbol, jobjectArray jarrays, jint size, jboolean to_dpu, jboolean async) {

    struct dpu_set_t set = build_native_set(env, this);
    struct dpu_symbol_t symbol = build_native_symbol(env, jsymbol);
    dpu_xfer_t xfer = to_dpu ? DPU_XFER_TO_DPU : DPU_XFER_FROM_DPU;
    dpu_xfer_flags_t flags = async ? DPU_XFER_ASYNC : DPU_XFER_DEFAULT;
    uint32_t nr_buffers = (*env)->GetArrayLength(env, jarrays);
    jobject arrays[nr_buffers];
    char *buffers[nr_buffers];
    uint32_t nr_dpus;

    THROW_ON_ERROR_L(dpu_get_nr_dpus(set, &nr_dpus), end);

    if (nr_buffers != nr_dpus) {
        throw_dpu_exception(env, "the number of buffers should match the numer of DPUs");
        goto end;
    }

    for (uint32_t each_buffer = 0; each_buffer < nr_buffers; ++each_buffer) {
        jobject jarray = (*env)->GetObjectArrayElement(env, jarrays, each_buffer);
        if(jarray == NULL)
          buffers[each_buffer] = NULL;
        else {
          char *jbuffer = (*env)->GetDirectBufferAddress(env, jarray);
          buffers[each_buffer] = jbuffer;
        }
        arrays[each_buffer] = jarray;
    }

    bool valid_size = (size > 0);
    bool length_initialized = false;
    jsize length;

    struct dpu_set_t dpu;
    uint32_t each_dpu;
    DPU_FOREACH (set, dpu, each_dpu) {
        jobject jarray = arrays[each_dpu];
        char *jbuffer = buffers[each_dpu];
        if(jbuffer == NULL) continue;
        jsize buffer_length = (*env)->GetDirectBufferCapacity(env, jarray);

        if(valid_size) {
          if (size > buffer_length) {
            throw_dpu_exception(env, "all buffers should have length larger than the size specified");
            return;
          }
        }
        else if (!length_initialized) {
            length_initialized = true;
            length = buffer_length;
        } else if (length != buffer_length) {
          throw_dpu_exception(env, "all buffers should have the same length");
          return;
        }

        THROW_ON_ERROR_L(dpu_prepare_xfer(dpu, jbuffer), end);
    }

    if (length_initialized) {
        THROW_ON_ERROR_L(dpu_push_xfer_symbol(set, xfer, symbol, 0, length, flags), end);
    }
    else if(valid_size) {
        THROW_ON_ERROR_L(dpu_push_xfer_symbol(set, xfer, symbol, 0, size, flags), end);
    }

end:
    return;
}

JNIEXPORT void JNICALL
Java_com_upmem_dpu_NativeDpuSet_launch(JNIEnv *env, jobject this, jboolean async)
{
    struct dpu_set_t set = build_native_set(env, this);
    dpu_launch_policy_t policy = async ? DPU_ASYNCHRONOUS : DPU_SYNCHRONOUS;
    THROW_ON_ERROR(dpu_launch(set, policy));
}

struct java_print_ctxt_t {
    JNIEnv *env;
    jobject stream;
};

static dpu_error_t
java_print_fct(void *arg, const char *fmt, ...)
{
    char *str;
    va_list ap;
    va_start(ap, fmt);
    if (vasprintf(&str, fmt, ap) == -1) {
        va_end(ap);
        return DPU_ERR_SYSTEM;
    }
    struct java_print_ctxt_t *ctxt = arg;
    JNIEnv *env = ctxt->env;
    jobject stream = ctxt->stream;
    jclass cls = (*env)->GetObjectClass(env, stream);
    jmethodID streamPrintID = (*env)->GetMethodID(env, cls, "print", "(Ljava/lang/String;)V");

    jstring jstr = (*env)->NewStringUTF(env, str);
    (*env)->CallObjectMethod(env, stream, streamPrintID, jstr);

    va_end(ap);
    return DPU_OK;
}

JNIEXPORT void JNICALL
Java_com_upmem_dpu_NativeDpuSet_log(JNIEnv *env, jobject this, jobject stream)
{
    struct dpu_set_t set = build_native_set(env, this);

    struct dpu_t *dpu = dpu_from_set(set);

    if (dpu == NULL) {
        throw_dpu_exception(env, "invalid action for this DPU set");
        return;
    }

    struct java_print_ctxt_t ctxt = { .env = env, .stream = stream };
    THROW_ON_ERROR_L(java_print_fct(&ctxt, DPU_LOG_FORMAT_HEADER, dpu_get_id(dpu)), end);
    THROW_ON_ERROR(dpulog_read_for_dpu_(dpu, java_print_fct, &ctxt));

end:
    return;
}

struct callback_ctx {
    JavaVM *jvm;
    jobject set;
    jobject callback;
    bool is_single_call;
    uint32_t ref_count;
};

static dpu_error_t
callback_wrapper(struct dpu_set_t set, uint32_t idx, void *args)
{
    struct callback_ctx *ctxt = args;
    bool detach = false;
    JavaVM *jvm = ctxt->jvm;
    JNIEnv *env;

    if ((*jvm)->GetEnv(jvm, (void **)&env, JNI_VERSION_1_8) == JNI_EDETACHED) {
        detach = true;
        (*jvm)->AttachCurrentThread(jvm, (void **)&env, NULL);
    }

    jobject jset = NULL;
    if (ctxt->is_single_call || (set.kind == DPU_SET_DPU)) {
        jset = ctxt->set;
    } else {
        jclass cls = (*env)->GetObjectClass(env, ctxt->set);
        jfieldID childrenID = (*env)->GetFieldID(env, cls, "children", "Ljava/util/List;");
        jfieldID nativePointerID = (*env)->GetFieldID(env, cls, "nativePointer", "J");
        jobject children = (*env)->GetObjectField(env, ctxt->set, childrenID);
        jclass listCls = (*env)->GetObjectClass(env, children);
        jmethodID sizeID = (*env)->GetMethodID(env, listCls, "size", "()I");
        jmethodID getID = (*env)->GetMethodID(env, listCls, "get", "(I)Ljava/lang/Object;");
        uint32_t size = (*env)->CallIntMethod(env, children, sizeID);
        for (uint32_t each_rank = 0; each_rank < size; ++each_rank) {
            jobject jrank = (*env)->CallObjectMethod(env, children, getID, each_rank);
            jlong nativePointer = (*env)->GetLongField(env, jrank, nativePointerID);
            if ((struct dpu_rank_t **)(nativePointer) == &set.list.ranks[0]) {
                jset = jrank;
                break;
            }
        }
    }

    jclass cls = (*env)->GetObjectClass(env, ctxt->callback);
    jmethodID callID = (*env)->GetMethodID(env, cls, "call", "(Lcom/upmem/dpu/DpuSet;I)V");

    jclass setCls = (*env)->GetObjectClass(env, jset);
    jfieldID wrapperID = (*env)->GetFieldID(env, setCls, "wrapper", "Lcom/upmem/dpu/DpuSet;");
    jobject wrapper = (*env)->GetObjectField(env, jset, wrapperID);
    (*env)->CallVoidMethod(env, ctxt->callback, callID, wrapper, idx);

    if (__sync_sub_and_fetch(&ctxt->ref_count, 1) == 0) {
        (*env)->DeleteGlobalRef(env, ctxt->set);
        (*env)->DeleteGlobalRef(env, ctxt->callback);
        free(ctxt);
    }

    if (detach) {
        (*jvm)->DetachCurrentThread(jvm);
    }

    return DPU_OK;
}

JNIEXPORT void JNICALL
Java_com_upmem_dpu_NativeDpuSet_call(JNIEnv *env, jobject this, jobject callback, jboolean blocking, jboolean single_call)
{
    struct dpu_set_t set = build_native_set(env, this);
    dpu_callback_flags_t flags = DPU_CALLBACK_ASYNC;
    if (!blocking) {
        flags |= DPU_CALLBACK_NONBLOCKING;
    }
    if (single_call) {
        flags |= DPU_CALLBACK_SINGLE_CALL;
    }

    jobject set_ref = (*env)->NewGlobalRef(env, this);
    jobject cb_ref = (*env)->NewGlobalRef(env, callback);
    struct callback_ctx *args = malloc(sizeof(*args));
    if (args == NULL) {
        THROW_ON_ERROR(DPU_ERR_SYSTEM);
    } else {
        (*env)->GetJavaVM(env, &args->jvm);
        args->callback = cb_ref;
        args->set = set_ref;
        args->ref_count = (set.kind == DPU_SET_DPU) ? 1 : set.list.nr_ranks;
        args->is_single_call = single_call;
        THROW_ON_ERROR(dpu_callback(set, callback_wrapper, args, flags));
    }
}

JNIEXPORT void JNICALL
Java_com_upmem_dpu_NativeDpuSet_sync(JNIEnv *env, jobject this)
{
    struct dpu_set_t set = build_native_set(env, this);
    THROW_ON_ERROR(dpu_sync(set));
}

JNIEXPORT jobject JNICALL
Java_com_upmem_dpu_NativeDpuSet_debugInit(JNIEnv *env, jobject this)
{
    struct dpu_set_t set = build_native_set(env, this);

    if (set.kind != DPU_SET_DPU) {
        THROW_ON_ERROR(DPU_ERR_INVALID_DPU_SET);
        return NULL;
    }

    struct dpu_rank_t *rank = set.dpu->rank;
    struct dpu_context_t *context = malloc(sizeof(*context));
    uint32_t nr_of_atomic_bits;
    uint8_t nr_of_dpu_threads, nr_of_work_registers_per_thread;
    struct _dpu_description_t *description = dpu_get_description(rank);

    nr_of_dpu_threads = description->hw.dpu.nr_of_threads;
    nr_of_work_registers_per_thread = description->hw.dpu.nr_of_work_registers_per_thread;
    nr_of_atomic_bits = description->hw.dpu.nr_of_atomic_bits;

    context->registers = malloc(nr_of_dpu_threads * nr_of_work_registers_per_thread * sizeof(*(context->registers)));
    context->scheduling = malloc(nr_of_dpu_threads * sizeof(*(context->scheduling)));
    context->pcs = malloc(nr_of_dpu_threads * sizeof(*(context->pcs)));
    context->zero_flags = malloc(nr_of_dpu_threads * sizeof(*(context->zero_flags)));
    context->carry_flags = malloc(nr_of_dpu_threads * sizeof(*(context->carry_flags)));
    context->atomic_register = malloc(nr_of_atomic_bits * sizeof(*(context->atomic_register)));

    for (dpu_thread_t each_thread = 0; each_thread < nr_of_dpu_threads; ++each_thread) {
        context->scheduling[each_thread] = 0xFF;
    }

    context->nr_of_running_threads = 0;
    context->bkp_fault = false;
    context->dma_fault = false;
    context->mem_fault = false;

    THROW_ON_ERROR_L(dpu_initialize_fault_process_for_dpu(set.dpu, context, (mram_addr_t)0 /*nullptr*/), error);

    jlong contextPtr = (jlong)context;
    jboolean bkpFault = (jboolean)context->bkp_fault;
    jboolean dmaFault = (jboolean)context->dma_fault;
    jboolean memFault = (jboolean)context->mem_fault;
    jint bkpIndex = (jint)context->bkp_fault_thread_index;
    jint dmaIndex = (jint)context->dma_fault_thread_index;
    jint memIndex = (jint)context->mem_fault_thread_index;
    jint bkpFaultId = (jint)context->bkp_fault_id;
    jbyteArray scheduling = (*env)->NewByteArray(env, nr_of_dpu_threads);
    jshortArray pcs = (*env)->NewShortArray(env, nr_of_dpu_threads);

    (*env)->SetByteArrayRegion(env, scheduling, 0, nr_of_dpu_threads, (const jbyte *)context->scheduling);
    (*env)->SetShortArrayRegion(env, pcs, 0, nr_of_dpu_threads, (const jshort *)context->pcs);

    jobject information;

    jclass cls = (*env)->FindClass(env, "com/upmem/dpu/DpuFaultDump");

    jmethodID constructor = (*env)->GetMethodID(env, cls, "<init>", "(JZZZIIII[B[S)V");

    information = (*env)->NewObject(env,
        cls,
        constructor,
        contextPtr,
        bkpFault,
        dmaFault,
        memFault,
        bkpIndex,
        dmaIndex,
        memIndex,
        bkpFaultId,
        scheduling,
        pcs);

    return information;

error:
    free(context->registers);
    free(context->scheduling);
    free(context->pcs);
    free(context->zero_flags);
    free(context->carry_flags);
    free(context->atomic_register);
    free(context);
    return NULL;
}

static jobject
build_java_dump(JNIEnv *env,
    struct dpu_context_t *dpu_context,
    uint8_t nr_of_dpu_threads,
    uint8_t nr_of_work_registers_per_thread,
    uint32_t nr_of_atomic_bits)
{
    jbooleanArray atomicBits = (*env)->NewBooleanArray(env, nr_of_atomic_bits);
    jintArray registers = (*env)->NewIntArray(env, nr_of_work_registers_per_thread * nr_of_dpu_threads);
    jshortArray pcs = (*env)->NewShortArray(env, nr_of_dpu_threads);
    jbooleanArray zeroFlags = (*env)->NewBooleanArray(env, nr_of_dpu_threads);
    jbooleanArray carryFlags = (*env)->NewBooleanArray(env, nr_of_dpu_threads);

    (*env)->SetBooleanArrayRegion(env, atomicBits, 0, nr_of_atomic_bits, (const jboolean *)dpu_context->atomic_register);
    (*env)->SetIntArrayRegion(
        env, registers, 0, nr_of_work_registers_per_thread * nr_of_dpu_threads, (const jint *)dpu_context->registers);
    (*env)->SetShortArrayRegion(env, pcs, 0, nr_of_dpu_threads, (const jshort *)dpu_context->pcs);
    (*env)->SetBooleanArrayRegion(env, zeroFlags, 0, nr_of_dpu_threads, (const jboolean *)dpu_context->zero_flags);
    (*env)->SetBooleanArrayRegion(env, carryFlags, 0, nr_of_dpu_threads, (const jboolean *)dpu_context->carry_flags);

    jclass cls = (*env)->FindClass(env, "com/upmem/dpu/DpuContextDump");
    jmethodID constructor = (*env)->GetMethodID(env, cls, "<init>", "([Z[I[S[Z[Z)V");

    return (*env)->NewObject(env, cls, constructor, atomicBits, registers, pcs, zeroFlags, carryFlags);
}

static jobject
dump_whole_context(JNIEnv *env, struct dpu_t *dpu)
{
    struct dpu_rank_t *rank = dpu->rank;
    struct dpu_context_t dpu_context;
    uint32_t nr_of_atomic_bits;
    uint8_t nr_of_dpu_threads, nr_of_work_registers_per_thread;
    struct _dpu_description_t *description = dpu_get_description(rank);

    nr_of_dpu_threads = description->hw.dpu.nr_of_threads;
    nr_of_work_registers_per_thread = description->hw.dpu.nr_of_work_registers_per_thread;
    nr_of_atomic_bits = description->hw.dpu.nr_of_atomic_bits;

    THROW_ON_ERROR_L(dpu_context_fill_from_rank(&dpu_context, rank), end);
    THROW_ON_ERROR_L(dpu_extract_pcs_for_dpu(dpu, &dpu_context), error);
    THROW_ON_ERROR_L(dpu_extract_context_for_dpu(dpu, &dpu_context), error);

    jobject information
        = build_java_dump(env, &dpu_context, nr_of_dpu_threads, nr_of_work_registers_per_thread, nr_of_atomic_bits);
    dpu_free_dpu_context(&dpu_context);

    return information;

error:
    dpu_free_dpu_context(&dpu_context);
end:
    return NULL;
}

static jobject
dump_debug_context(JNIEnv *env, struct dpu_t *dpu, jobject faultDump)
{
    jclass dumpCls = (*env)->GetObjectClass(env, faultDump);
    jfieldID nativePointerID = (*env)->GetFieldID(env, dumpCls, "faultContextPointer", "J");
    struct dpu_context_t *dpu_context = (struct dpu_context_t *)(*env)->GetLongField(env, faultDump, nativePointerID);
    struct dpu_rank_t *rank = dpu->rank;
    uint32_t nr_of_atomic_bits;
    uint8_t nr_of_dpu_threads, nr_of_work_registers_per_thread;
    struct _dpu_description_t *description = dpu_get_description(rank);

    nr_of_dpu_threads = description->hw.dpu.nr_of_threads;
    nr_of_work_registers_per_thread = description->hw.dpu.nr_of_work_registers_per_thread;
    nr_of_atomic_bits = description->hw.dpu.nr_of_atomic_bits;

    THROW_ON_ERROR_X(dpu_extract_context_for_dpu(dpu, dpu_context), return NULL);

    return build_java_dump(env, dpu_context, nr_of_dpu_threads, nr_of_work_registers_per_thread, nr_of_atomic_bits);
}

JNIEXPORT jobject JNICALL
Java_com_upmem_dpu_NativeDpuSet_dump(JNIEnv *env, jobject this, jobject faultDump)
{
    struct dpu_set_t set = build_native_set(env, this);

    if (set.kind != DPU_SET_DPU) {
        THROW_ON_ERROR(DPU_ERR_INVALID_DPU_SET);
        return NULL;
    }

    struct dpu_t *dpu = set.dpu;

    if (faultDump == NULL) {
        return dump_whole_context(env, dpu);
    } else {
        return dump_debug_context(env, dpu, faultDump);
    }
}

JNIEXPORT void JNICALL
Java_com_upmem_dpu_NativeDpuSet_systemReport(JNIEnv *env, jobject this, jstring output)
{
    struct dpu_set_t set = build_native_set(env, this);

    if (set.kind != DPU_SET_DPU) {
        THROW_ON_ERROR(DPU_ERR_INVALID_DPU_SET);
        return;
    }

    const char *c_output = (*env)->GetStringUTFChars(env, output, 0);
    THROW_ON_ERROR(dpu_custom_for_dpu(set.dpu, DPU_COMMAND_SYSTEM_REPORT, (dpu_custom_command_args_t)c_output));
    (*env)->ReleaseStringUTFChars(env, output, c_output);
}

JNIEXPORT void JNICALL
Java_com_upmem_dpu_NativeDpuSet_free(JNIEnv *env, jobject this)
{
    struct dpu_set_t set = build_native_set(env, this);
    THROW_ON_ERROR(dpu_free(set));
}
