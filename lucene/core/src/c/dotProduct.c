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
#include <stdint.h>
#include <stdio.h>
#include "dotProduct.h"

#ifdef __ARM_ACLE
#include <arm_acle.h>
#endif

#if (defined(__ARM_FEATURE_SVE))
#include <arm_sve.h>
/*
 * Unrolled and vectorized int8 dotProduct implementation using SVE instructions
 * NOTE: Clang 15.0 compiler on Apple M3 Max compiles the code below successfully
 * with '-march=native+sve' option but throws "Illegal Hardware Instruction" error
 * Looks like Apple M3 does not implement SVE and Apple's official documentation
 * is not explicit about this or at least I could not find it. 
 * 
 */
int32_t vdot8s_sve(int8_t vec1[], int8_t vec2[], int32_t limit) {
    int32_t result = 0;
    int32_t i = 0;
    // Vectors of 8-bit signed integers
    svint8_t va1, va2, va3, va4;
    svint8_t vb1, vb2, vb3, vb4;
    // Init accumulators
    svint32_t acc1 = svdup_n_s32(0);
    svint32_t acc2 = svdup_n_s32(0);
    svint32_t acc3 = svdup_n_s32(0);
    svint32_t acc4 = svdup_n_s32(0);

    // Number of 8-bits elements in the SVE vector
    int32_t vec_length = svcntb();

    // Manually unroll the loop
    for (i = 0; i + 4 * vec_length <= limit; i += 4 * vec_length) {
	// Load vectors into the Z registers which can range from 128-bit to 2048-bit wide
	// The predicate register - P determines which bytes are active
	// svptrue_b8() returns a predictae in which every element is true
	//
        va1 = svld1_s8(svptrue_b8(), &vec1[i]);
        vb1 = svld1_s8(svptrue_b8(), &vec2[i]);

        va2 = svld1_s8(svptrue_b8(), &vec1[i + vec_length]);
        vb2 = svld1_s8(svptrue_b8(), &vec2[i + vec_length]);

        va3 = svld1_s8(svptrue_b8(), &vec1[i + 2 * vec_length]);
        vb3 = svld1_s8(svptrue_b8(), &vec2[i + 2 * vec_length]);

        va4 = svld1_s8(svptrue_b8(), &vec1[i + 3 * vec_length]);
        vb4 = svld1_s8(svptrue_b8(), &vec2[i + 3 * vec_length]);

	    // Dot product using SDOT instruction on Z vectors
	    acc1 = svdot_s32(acc1, va1, vb1);
	    acc2 = svdot_s32(acc2, va2, vb2);
	    acc3 = svdot_s32(acc3, va3, vb3);
	    acc4 = svdot_s32(acc4, va4, vb4);
    }

    // Vector tail: less scalar computations for unaligned sizes, esp with big vector sizes
    for (; i < limit; i+= vec_length) {
        va1 = svld1_s8(svptrue_b8(), &vec1[i]);
        vb1 = svld1_s8(svptrue_b8(), &vec2[i]);
        acc1 = svdot_s32(acc1, va1, vb1);
    }

    // Add correspponding active elements in each of the vectors
    acc1 = svadd_s32_x(svptrue_b8() , acc1, acc2);
    acc3 = svadd_s32_x(svptrue_b8() , acc3, acc4);
    acc1 = svadd_s32_x(svptrue_b8(), acc1, acc3);

    // REDUCE: Add every vector element in target and write result to scalar
    result = svaddv_s32(svptrue_b8(), acc1);
    return result;
}
#endif /* SVE code */

#if defined(__ARM_NEON)
#include <arm_neon.h>
// https://developer.arm.com/architectures/instruction-sets/intrinsics/
int32_t vdot8s_neon(int8_t vec1[], int8_t vec2[], int32_t limit) {
    int32_t result = 0;
    int32x4_t acc1 = vdupq_n_s32(0);
    int32x4_t acc2 = vdupq_n_s32(0);
    int32x4_t acc3 = vdupq_n_s32(0);
    int32x4_t acc4 = vdupq_n_s32(0);
    int32_t i = 0;
    int8x16_t va1, va2, va3, va4;
    int8x16_t vb1, vb2, vb3, vb4;
    
    for (; i + 64 <= limit; i += 64) {
        // Read into 8 (bit) x 16 (values) vector
        va1 = vld1q_s8(&vec1[i]);
        vb1 = vld1q_s8(&vec2[i]);
        
        va2 = vld1q_s8(&vec1[i + 16]);
        vb2 = vld1q_s8(&vec2[i + 16]);

        va3 = vld1q_s8(&vec1[i + 32]);
        vb3 = vld1q_s8(&vec2[i + 32]);
  	
        va4 = vld1q_s8(&vec1[i + 48]);
        vb4 = vld1q_s8(&vec2[i + 48]);
	
	    // Dot product using SDOT instruction
	    // GCC 7.3 does not define the intrinsic below so we get compile time error.
	    acc1 = vdotq_s32(acc1, va1, vb1);
	    acc2 = vdotq_s32(acc2, va2, vb2);
	    acc3 = vdotq_s32(acc3, va3, vb3);
	    acc4 = vdotq_s32(acc4, va4, vb4);
    }

    // Vector tail: less scalar computations for unaligned sizes, esp with big vector sizes
    for (; i < limit; i += 16) {
        va1 = vld1q_s8(&vec1[i]);
        vb1 = vld1q_s8(&vec2[i]);
        acc1 = vdotq_s32(acc1, va1, vb1);
    }

    // Add corresponding elements in each vectors
    acc1 = vaddq_s32(acc1, acc2);
    acc3 = vaddq_s32(acc3, acc4);
    acc1 = vaddq_s32(acc1, acc3);

    // REDUCE:  Add every vector element in target and write result to scalar
    result += vaddvq_s32(acc1);

    return result;
}
#endif /* __ARM_NEON */

int32_t dot8s(int8_t vec1[], int8_t vec2[], int32_t limit) {
    int32_t result = 0;
    #ifdef __clang__
    #pragma clang loop vectorize(assume_safety) unroll(enable)
    #endif
    for (int32_t i = 0; i < limit; i++) {
        result += vec1[i] * vec2[i];
    }
    return result;
}


int main(int argc, const char* arrgs[]) {
    int s = 157;
    int8_t a[s];
    int8_t b[s];
    for (int i = 0; i < s; i++) {
	    a[i] = 2;
	    b[i] = 3;
    }
  //  printf("Sum (Vectorized - SVE) = %d\n", vdot8s_sve(a, b, s));
    printf("Dimension: [%d]; Sum (Vectorized - NEON) = %d\n", s, vdot8s_neon(a, b, s));
    printf("Dimension: [%d]; Sum (Scalar) = %d\n", s, dot8s(a, b, s));
}

