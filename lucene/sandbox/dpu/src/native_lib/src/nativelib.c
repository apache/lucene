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

    uint32_t nrDpus = 0;
    THROW_ON_ERROR(dpu_get_nr_dpus(set, &nrDpus));

    return (jint)nrDpus;
}
