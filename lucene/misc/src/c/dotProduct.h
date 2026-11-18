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

// Functions that NativeVectorUtilSupport binds from the "dotProduct" library. A library may
// implement any subset; Lucene uses its Java implementation for the others. Each function must
// return what the VectorUtilSupport method it replaces returns.
//
// Where a function takes a single length argument `n`, it is the number of elements of its FIRST
// array argument. The other array has the same number of elements unless stated otherwise.

#include <stdint.h>

// Signed bytes.
int32_t lucene_dotProduct(int8_t a[], int8_t b[], int32_t n);
int32_t lucene_squareDistance(int8_t a[], int8_t b[], int32_t n);
float lucene_cosine(int8_t a[], int8_t b[], int32_t n);

// Unsigned bytes.
int32_t lucene_uint8DotProduct(uint8_t a[], uint8_t b[], int32_t n);
int32_t lucene_uint8SquareDistance(uint8_t a[], uint8_t b[], int32_t n);

// Unsigned half-bytes, one value per byte.
int32_t lucene_int4DotProduct(int8_t a[], int8_t b[], int32_t n);
int32_t lucene_int4SquareDistance(int8_t a[], int8_t b[], int32_t n);

// `packed` holds n / 2 bytes: the high nibble of packed[i] pairs with unpacked[i], the low nibble
// with unpacked[n / 2 + i].
int32_t lucene_int4DotProductSinglePacked(int8_t unpacked[], uint8_t packed[], int32_t n);
int32_t lucene_int4SquareDistanceSinglePacked(int8_t unpacked[], uint8_t packed[], int32_t n);

// Both arrays packed as above; n is their length in bytes.
int32_t lucene_int4DotProductBothPacked(uint8_t a[], uint8_t b[], int32_t n);
int32_t lucene_int4SquareDistanceBothPacked(uint8_t a[], uint8_t b[], int32_t n);

// `q` is a transposed int4 vector of n bytes (4 bit planes); `d` holds n / 4 bytes (1 bit plane)
// for int4BitDotProduct and n / 2 bytes (2 bit planes, lower bits first) for int4DibitDotProduct.
int64_t lucene_int4BitDotProduct(uint8_t q[], uint8_t d[], int32_t n);
int64_t lucene_int4DibitDotProduct(uint8_t q[], uint8_t d[], int32_t n);

// Floats; n is the number of floats, not bytes.
float lucene_dotProductFloat(float a[], float b[], int32_t n);
float lucene_squareDistanceFloat(float a[], float b[], int32_t n);
float lucene_cosineFloat(float a[], float b[], int32_t n);

// Writes n quantized values to dest and returns the corrective offset.
float lucene_minMaxScalarQuantize(float vector[], int8_t dest[], float scale, float alpha,
    float minQuantile, float maxQuantile, int32_t n);
float lucene_recalculateScalarQuantizationOffset(int8_t vector[], float oldAlpha,
    float oldMinQuantile, float scale, float alpha, float minQuantile, float maxQuantile,
    int32_t n);

// Compacts the first upTo entries of both buffers in place; returns the number kept.
int32_t lucene_filterByScore(int32_t docBuffer[], double scoreBuffer[], double minScoreInclusive,
    int32_t upTo);

// Normalizes v in place and returns v. For a vector with zero norm it must leave v unchanged and
// return NULL; Lucene then throws IllegalArgumentException if throwOnZero is non-zero.
float *lucene_l2normalize(float v[], int8_t throwOnZero, int32_t n);

// n is the array length, which is 256.
void lucene_expand8(int32_t arr[], int32_t n);

// Returns the first index in [from, to) whose value is >= target, or `to`.
int32_t lucene_findNextGEQ(int32_t buffer[], int32_t target, int32_t from, int32_t to);
