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
package org.apache.lucene.util;

import java.util.List;

/** Stores a float numeric multi-vector value */
public class FloatMultiVectorValue {
  private float[] packedValue;
  private int vectorCount;
  private final int dimension;

  public FloatMultiVectorValue(float[] value, int dimension) {
    if (dimension <= 0) {
      throw new IllegalArgumentException("Dimension for composing vectors should be > 0");
    }
    if (value == null || value.length == 0) {
      throw new IllegalArgumentException("Empty multi-vector value not allowed");
    }
    this.vectorCount = value.length / dimension;
    if (dimension * vectorCount != value.length) {
      throw new IllegalArgumentException(
          "Each composing vector should have the same dimension = " + dimension);
    }
    this.dimension = dimension;
    this.packedValue = ArrayUtil.copyOfSubArray(value, 0, vectorCount * this.dimension);
  }

  public FloatMultiVectorValue(List<float[]> vectorValues, int dimension) {
    if (dimension <= 0) {
      throw new IllegalArgumentException("Dimension for composing vectors should be > 0");
    }
    if (vectorValues == null || vectorValues.isEmpty()) {
      throw new IllegalArgumentException("Empty vector values list not allowed");
    }
    vectorCount = vectorValues.size();
    this.dimension = dimension;
    packedValue = new float[vectorCount * dimension];
    int targetPtr = 0;
    for (float[] vector : vectorValues) {
      if (vector.length != dimension) {
        throw new IllegalArgumentException(
            "Found vector of dimension = "
                + vector.length
                + ". Each composing vector should have the same dimension = "
                + dimension);
      }
      System.arraycopy(vector, 0, packedValue, targetPtr, dimension);
      targetPtr += dimension;
    }
  }

  //  // TODO: implement if needed
  //  public void addValues(List<float []> vectorValues) {
  //    if (vectorValues == null || vectorValues.isEmpty()) {
  //      return;
  //    }
  //    for (float[] vector: vectorValues) {
  //      if (vector.length != dimension) {
  //        throw new IllegalArgumentException("Dimension for provided vector value [" +
  // vector.length + "] " +
  //            "does not match FloatMultiVectorValue dimension [" + dimension + "]");
  //        // TODO: add to existing running list of vectors
  //      }
  //    }
  //  }

  public float[] packedValue() {
    return packedValue;
  }

  public int dimension() {
    return dimension;
  }

  public int vectorCount() {
    return vectorCount;
  }

  public long ramBytesUsed() {
    return RamUsageEstimator.NUM_BYTES_ARRAY_HEADER
        + (long) RamUsageEstimator.primitiveSizes.get(float.class) * packedValue.length
        + (long) 2 * RamUsageEstimator.primitiveSizes.get(int.class);
  }
}
