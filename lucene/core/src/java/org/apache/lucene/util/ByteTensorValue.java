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

public class ByteTensorValue {
  private byte[] packedValue;
  private int vectorCount;
  final private int dimension;

  public ByteTensorValue(byte[] tensorValue, int dimension) {
    if (dimension <= 0) {
      throw new IllegalArgumentException("Dimension for composing vectors should be > 0");
    }
    if (tensorValue == null || tensorValue.length == 0) {
      throw new IllegalArgumentException("Empty tensor value not allowed");
    }
    this.vectorCount = tensorValue.length / dimension;
    if (dimension * vectorCount != tensorValue.length) {
      throw new IllegalArgumentException("Each composing vector should have the same dimension = " + dimension);
    }
    this.dimension = dimension;
    this.packedValue = ArrayUtil.copyOfSubArray(tensorValue, 0, vectorCount * this.dimension);
  }

  public ByteTensorValue(List<byte[]> vectorValues, int dimension) {
    if (dimension <= 0) {
      throw new IllegalArgumentException("Dimension for composing vectors should be > 0");
    }
    if (vectorValues == null || vectorValues.isEmpty()) {
      throw new IllegalArgumentException("Empty vector values list not allowed");
    }
    vectorCount = vectorValues.size();
    this.dimension = dimension;
    packedValue = new byte[vectorCount * dimension];
    int targetPtr = 0;
    for (byte[] vector: vectorValues) {
      if (vector.length != dimension) {
        throw new IllegalArgumentException("Found vector of dimension = " + vector.length +
            ". Each composing vector should have the same dimension = " + dimension);
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
//        throw new IllegalArgumentException("Dimension for provided vector value [" + vector.length + "] " +
//            "does not match FloatTensorValue dimension [" + dimension + "]");
//        // TODO: add to existing running list of vectors
//      }
//    }
//  }

  public byte[] packedValue() {
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
        + (long) RamUsageEstimator.primitiveSizes.get(byte.class) * packedValue.length
        + (long) 2 * RamUsageEstimator.primitiveSizes.get(int.class);
  }
}
