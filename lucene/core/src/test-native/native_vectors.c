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

// Scalar test fixtures for the real FFM calling convention. This intentionally implements only
// six symbols, so the same library also exercises fallback and strict-mode failures.
#if UNPREFIXED
#define lucene_dotProduct dotProduct
#define lucene_cosine cosine
#define lucene_dotProductFloat dotProductFloat
#define lucene_squareDistanceFloat squareDistanceFloat
#define lucene_cosineFloat cosineFloat
#define lucene_l2normalize l2normalize
#endif
#include "dotProduct.h"
#include <math.h>
#include <stddef.h>
#include <string.h>

// Keep these indices in sync with TestNativeVectorLibrary.Checks.
enum { BYTE_DOT, BYTE_COSINE, FLOAT_DOT, FLOAT_DISTANCE, FLOAT_COSINE, NORMALIZE, COUNT };
static int32_t calls[COUNT];
static int32_t lengths[COUNT];
static int32_t expected_length;
static int32_t last_throw_on_zero;

// Observation functions are deliberately independent of the production symbol prefix variant.
void lucene_test_reset(int32_t length) {
  memset(calls, 0, sizeof(calls));
  memset(lengths, 0, sizeof(lengths));
  expected_length = length;
  last_throw_on_zero = -1;
}

int32_t lucene_test_calls(int32_t operation) {
  return calls[operation];
}

int32_t lucene_test_length(int32_t operation) {
  return lengths[operation];
}

int32_t lucene_test_throw_on_zero(void) {
  return last_throw_on_zero;
}

static int record_call(int operation, int32_t length) {
  calls[operation]++;
  lengths[operation] = length;
  // Reject a bad Java length before touching memory, so a regression fails an assertion rather
  // than reading beyond the end of a heap array or crashing the test JVM.
  return length == expected_length;
}

int32_t lucene_dotProduct(int8_t a[], int8_t b[], int32_t n) {
  if (!record_call(BYTE_DOT, n)) return 0;
  int32_t result = 0;
  for (int32_t i = 0; i < n; i++) result += (int32_t) a[i] * b[i];
  return result;
}

float lucene_cosine(int8_t a[], int8_t b[], int32_t n) {
  if (!record_call(BYTE_COSINE, n)) return NAN;
  int32_t dot = 0, norm_a = 0, norm_b = 0;
  for (int32_t i = 0; i < n; i++) {
    dot += (int32_t) a[i] * b[i];
    norm_a += (int32_t) a[i] * a[i];
    norm_b += (int32_t) b[i] * b[i];
  }
  return (float) (dot / sqrt((double) norm_a * norm_b));
}

float lucene_dotProductFloat(float a[], float b[], int32_t n) {
  if (!record_call(FLOAT_DOT, n)) return NAN;
  float result = 0;
  for (int32_t i = 0; i < n; i++) result += a[i] * b[i];
  return result;
}

float lucene_squareDistanceFloat(float a[], float b[], int32_t n) {
  if (!record_call(FLOAT_DISTANCE, n)) return NAN;
  float result = 0;
  for (int32_t i = 0; i < n; i++) {
    float delta = a[i] - b[i];
    result += delta * delta;
  }
  return result;
}

float lucene_cosineFloat(float a[], float b[], int32_t n) {
  if (!record_call(FLOAT_COSINE, n)) return NAN;
  float dot = 0, norm_a = 0, norm_b = 0;
  for (int32_t i = 0; i < n; i++) {
    dot += a[i] * b[i];
    norm_a += a[i] * a[i];
    norm_b += b[i] * b[i];
  }
  return (float) (dot / sqrt((double) norm_a * norm_b));
}

float *lucene_l2normalize(float v[], int8_t throwOnZero, int32_t n) {
  last_throw_on_zero = throwOnZero; // Java interprets NULL according to this flag.
  if (!record_call(NORMALIZE, n)) return NULL;
  float squared_norm = 0;
  for (int32_t i = 0; i < n; i++) squared_norm += v[i] * v[i];
  if (squared_norm == 0) return NULL;
  float norm = (float) sqrt(squared_norm);
  for (int32_t i = 0; i < n; i++) v[i] /= norm;
  return v;
}
