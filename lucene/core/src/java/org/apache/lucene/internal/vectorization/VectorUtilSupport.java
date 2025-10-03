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

package org.apache.lucene.internal.vectorization;

import java.lang.foreign.MemorySegment;

/**
 * Interface for implementations of VectorUtil support.
 *
 * @lucene.internal
 */
public interface VectorUtilSupport<IByteVector, IFloatVector> {
  IByteVector bytesFromArray(byte[] array);

  IByteVector bytesFromMemorySegment(MemorySegment segment);

  IFloatVector floatsFromArray(float[] array);

  IFloatVector floatsFromMemorySegment(MemorySegment segment);

  default float dotProductFloats(float[] a, float[] b) {
    return dotProductFloats(floatsFromArray(a), floatsFromArray(b));
  }

  default float dotProductFloats(float[] a, MemorySegment b) {
    return dotProductFloats(floatsFromArray(a), floatsFromMemorySegment(b));
  }

  default float dotProductFloats(MemorySegment a, MemorySegment b) {
    return dotProductFloats(floatsFromMemorySegment(a), floatsFromMemorySegment(b));
  }

  /** Calculates the dot product of the given float arrays. */
  float dotProductFloats(IFloatVector a, IFloatVector b);

  default float cosineFloats(float[] a, float[] b) {
    return cosineFloats(floatsFromArray(a), floatsFromArray(b));
  }

  /** Returns the cosine similarity between the two vectors. */
  float cosineFloats(IFloatVector v1, IFloatVector v2);

  default float squareDistanceFloats(float[] a, float[] b) {
    return squareDistanceFloats(floatsFromArray(a), floatsFromArray(b));
  }

  default float squareDistanceFloats(float[] a, MemorySegment b) {
    return squareDistanceFloats(floatsFromArray(a), floatsFromMemorySegment(b));
  }

  default float squareDistanceFloats(MemorySegment a, MemorySegment b) {
    return squareDistanceFloats(floatsFromMemorySegment(a), floatsFromMemorySegment(b));
  }

  /** Returns the sum of squared differences of the two vectors. */
  float squareDistanceFloats(IFloatVector a, IFloatVector b);

  default int dotProductBytes(byte[] a, byte[] b) {
    return dotProductBytes(bytesFromArray(a), bytesFromArray(b));
  }

  default int dotProductBytes(byte[] a, MemorySegment b) {
    return dotProductBytes(bytesFromArray(a), bytesFromMemorySegment(b));
  }

  default int dotProductBytes(MemorySegment a, MemorySegment b) {
    return dotProductBytes(bytesFromMemorySegment(a), bytesFromMemorySegment(b));
  }

  /** Returns the dot product computed over signed bytes. */
  int dotProductBytes(IByteVector a, IByteVector b);

  default int int4DotProduct(byte[] a, byte[] b) {
    return int4DotProduct(bytesFromArray(a), bytesFromArray(b));
  }

  default int int4DotProduct(byte[] a, MemorySegment b) {
    return int4DotProduct(bytesFromArray(a), bytesFromMemorySegment(b));
  }

  default int int4DotProduct(MemorySegment a, MemorySegment b) {
    return int4DotProduct(bytesFromMemorySegment(a), bytesFromMemorySegment(b));
  }

  /** Returns the dot product computed over unsigned half-bytes, both uncompressed. */
  int int4DotProduct(IByteVector a, IByteVector b);

  default int int4DotProductSinglePacked(byte[] a, byte[] b) {
    return int4DotProductSinglePacked(bytesFromArray(a), bytesFromArray(b));
  }

  default int int4DotProductSinglePacked(byte[] a, MemorySegment b) {
    return int4DotProductSinglePacked(bytesFromArray(a), bytesFromMemorySegment(b));
  }

  /** Returns the dot product computed over unsigned half-bytes, one compressed. */
  int int4DotProductSinglePacked(IByteVector unpacked, IByteVector packed);

  default int int4DotProductBothPacked(byte[] a, byte[] b) {
    return int4DotProductBothPacked(bytesFromArray(a), bytesFromArray(b));
  }

  default int int4DotProductBothPacked(MemorySegment a, MemorySegment b) {
    return int4DotProductBothPacked(bytesFromMemorySegment(a), bytesFromMemorySegment(b));
  }

  /** Returns the dot product computed over unsigned half-bytes, both compressed. */
  int int4DotProductBothPacked(IByteVector a, IByteVector b);

  default int uint8DotProduct(byte[] a, byte[] b) {
    return uint8DotProduct(bytesFromArray(a), bytesFromArray(b));
  }

  default int uint8DotProduct(byte[] a, MemorySegment b) {
    return uint8DotProduct(bytesFromArray(a), bytesFromMemorySegment(b));
  }

  default int uint8DotProduct(MemorySegment a, MemorySegment b) {
    return uint8DotProduct(bytesFromMemorySegment(a), bytesFromMemorySegment(b));
  }

  /** Returns the dot product computed as though the bytes were unsigned. */
  int uint8DotProduct(IByteVector a, IByteVector b);

  default float cosineBytes(byte[] a, byte[] b) {
    return cosineBytes(bytesFromArray(a), bytesFromArray(b));
  }

  default float cosineBytes(byte[] a, MemorySegment b) {
    return cosineBytes(bytesFromArray(a), bytesFromMemorySegment(b));
  }

  default float cosineBytes(MemorySegment a, MemorySegment b) {
    return cosineBytes(bytesFromMemorySegment(a), bytesFromMemorySegment(b));
  }

  /** Returns the cosine similarity between the two byte vectors. */
  float cosineBytes(IByteVector a, IByteVector b);

  default int squareDistanceBytes(byte[] a, byte[] b) {
    return squareDistanceBytes(bytesFromArray(a), bytesFromArray(b));
  }

  default int squareDistanceBytes(byte[] a, MemorySegment b) {
    return squareDistanceBytes(bytesFromArray(a), bytesFromMemorySegment(b));
  }

  default int squareDistanceBytes(MemorySegment a, MemorySegment b) {
    return squareDistanceBytes(bytesFromMemorySegment(a), bytesFromMemorySegment(b));
  }

  /** Returns the sum of squared differences of the two byte vectors. */
  int squareDistanceBytes(IByteVector a, IByteVector b);

  default int int4SquareDistance(byte[] a, byte[] b) {
    return int4SquareDistance(bytesFromArray(a), bytesFromArray(b));
  }

  default int int4SquareDistance(byte[] a, MemorySegment b) {
    return int4SquareDistance(bytesFromArray(a), bytesFromMemorySegment(b));
  }

  default int int4SquareDistance(MemorySegment a, MemorySegment b) {
    return int4SquareDistance(bytesFromMemorySegment(a), bytesFromMemorySegment(b));
  }

  /**
   * Returns the sum of squared differences between two unsigned half-byte vectors, both
   * uncompressed.
   */
  int int4SquareDistance(IByteVector a, IByteVector b);

  default int int4SquareDistanceSinglePacked(byte[] a, byte[] b) {
    return int4SquareDistanceSinglePacked(bytesFromArray(a), bytesFromArray(b));
  }

  default int int4SquareDistanceSinglePacked(byte[] a, MemorySegment b) {
    return int4SquareDistanceSinglePacked(bytesFromArray(a), bytesFromMemorySegment(b));
  }

  /**
   * Returns the sum of squared differences between two unsigned half-byte vectors, one compressed.
   */
  int int4SquareDistanceSinglePacked(IByteVector unpacked, IByteVector packed);

  default int int4SquareDistanceBothPacked(byte[] a, byte[] b) {
    return int4SquareDistanceBothPacked(bytesFromArray(a), bytesFromArray(b));
  }

  default int int4SquareDistanceBothPacked(MemorySegment a, MemorySegment b) {
    return int4SquareDistanceBothPacked(bytesFromMemorySegment(a), bytesFromMemorySegment(b));
  }

  /**
   * Returns the sum of squared differences between two unsigned half-byte vectors, both compressed.
   */
  int int4SquareDistanceBothPacked(IByteVector a, IByteVector b);

  default int uint8SquareDistance(byte[] a, byte[] b) {
    return uint8SquareDistance(bytesFromArray(a), bytesFromArray(b));
  }

  default int uint8SquareDistance(byte[] a, MemorySegment b) {
    return uint8SquareDistance(bytesFromArray(a), bytesFromMemorySegment(b));
  }

  default int uint8SquareDistance(MemorySegment a, MemorySegment b) {
    return uint8SquareDistance(bytesFromMemorySegment(a), bytesFromMemorySegment(b));
  }

  /** Returns the sum of squared differences of the two unsigned byte vectors. */
  int uint8SquareDistance(IByteVector a, IByteVector b);

  /**
   * Given an array {@code buffer} that is sorted between indexes {@code 0} inclusive and {@code to}
   * exclusive, find the first array index whose value is greater than or equal to {@code target}.
   * This index is guaranteed to be at least {@code from}. If there is no such array index, {@code
   * to} is returned.
   */
  int findNextGEQ(int[] buffer, int target, int from, int to);

  default long int4BitDotProduct(byte[] a, byte[] b) {
    return int4BitDotProduct(bytesFromArray(a), bytesFromArray(b));
  }

  /**
   * Compute the dot product between a quantized int4 vector and a binary quantized vector. It is
   * assumed that the int4 quantized bits are packed in the byte array in the same way as the {@link
   * org.apache.lucene.util.quantization.OptimizedScalarQuantizer#transposeHalfByte(byte[], byte[])}
   * and that the binary bits are packed the same way as {@link
   * org.apache.lucene.util.quantization.OptimizedScalarQuantizer#packAsBinary(byte[], byte[])}.
   *
   * @param int4Quantized half byte packed int4 quantized vector
   * @param binaryQuantized byte packed binary quantized vector
   * @return the dot product
   */
  long int4BitDotProduct(IByteVector int4Quantized, IByteVector binaryQuantized);

  /**
   * Quantizes {@code vector}, putting the result into {@code dest}.
   *
   * @param vector the vector to quantize
   * @param dest the destination vector
   * @param scale the scaling factor
   * @param alpha the alpha value
   * @param minQuantile the lower quantile of the distribution
   * @param maxQuantile the upper quantile of the distribution
   * @return the corrective offset that needs to be applied to the score
   */
  float minMaxScalarQuantize(
      float[] vector, byte[] dest, float scale, float alpha, float minQuantile, float maxQuantile);

  /**
   * Recalculates the offset for {@code vector}.
   *
   * @param vector the vector to quantize
   * @param oldAlpha the previous alpha value
   * @param oldMinQuantile the previous lower quantile
   * @param scale the scaling factor
   * @param alpha the alpha value
   * @param minQuantile the lower quantile of the distribution
   * @param maxQuantile the upper quantile of the distribution
   * @return the new corrective offset
   */
  float recalculateScalarQuantizationOffset(
      byte[] vector,
      float oldAlpha,
      float oldMinQuantile,
      float scale,
      float alpha,
      float minQuantile,
      float maxQuantile);

  /**
   * filter both {@code docBuffer} and {@code scoreBuffer} with {@code minScoreInclusive}, each
   * {@code docBuffer} and {@code scoreBuffer} of the same index forms a pair, pairs with score not
   * greater than or equal to {@code minScoreInclusive} will be filtered out from the array.
   *
   * @param docBuffer doc buffer contains docs (or some other value forms a pair with {@code
   *     scoreBuffer})
   * @param scoreBuffer score buffer contains scores to be compared with {@code minScoreInclusive}
   * @param minScoreInclusive minimal required score to not be filtered out
   * @param upTo where the filter should end
   * @return how many pairs left after filter
   */
  int filterByScore(int[] docBuffer, double[] scoreBuffer, double minScoreInclusive, int upTo);

  float[] l2normalize(float[] v, boolean throwOnZero);

  /**
   * Expands a 64-element integer array into a 256-element array by extracting individual bytes.
   * Each 32-bit integer is split into 4 bytes, expanding the array from 64 to 256 elements. Only
   * works on arrays with exactly 256 items (64 integers expanded to 256 bytes). Vectorization is
   * beneficial here because the block size is 256.
   */
  void expand8(int[] arr);
}
