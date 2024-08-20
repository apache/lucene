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

package org.apache.lucene.util.bkd;

import org.apache.lucene.util.ArrayUtil;

/**
 * Basic parameters for indexing points on the BKD tree.
 *
 * @param numDims How many dimensions we are storing at the leaf (data) node
 * @param numIndexDims How many dimensions we are indexing in the internal nodes
 * @param bytesPerDim How many bytes each value in each dimension takes.
 * @param maxPointsInLeafNode max points allowed on a Leaf block
 */
public record BKDConfig(int numDims, int numIndexDims, int bytesPerDim, int maxPointsInLeafNode) {

  /** Default maximum number of point in each leaf block */
  public static final int DEFAULT_MAX_POINTS_IN_LEAF_NODE = 512;

  /** Maximum number of index dimensions (2 * max index dimensions) */
  public static final int MAX_DIMS = 16;

  /** Maximum number of index dimensions */
  public static final int MAX_INDEX_DIMS = 8;

  public BKDConfig {
    // Check inputs are on bounds
    if (numDims < 1 || numDims > MAX_DIMS) {
      throw new IllegalArgumentException(
          "numDims must be 1 .. " + MAX_DIMS + " (got: " + numDims + ")");
    }
    if (numIndexDims < 1 || numIndexDims > MAX_INDEX_DIMS) {
      throw new IllegalArgumentException(
          "numIndexDims must be 1 .. " + MAX_INDEX_DIMS + " (got: " + numIndexDims + ")");
    }
    if (numIndexDims > numDims) {
      throw new IllegalArgumentException(
          "numIndexDims cannot exceed numDims (" + numDims + ") (got: " + numIndexDims + ")");
    }
    if (bytesPerDim <= 0) {
      throw new IllegalArgumentException("bytesPerDim must be > 0; got " + bytesPerDim);
    }
    if (maxPointsInLeafNode <= 0) {
      throw new IllegalArgumentException(
          "maxPointsInLeafNode must be > 0; got " + maxPointsInLeafNode);
    }
    if (maxPointsInLeafNode > ArrayUtil.MAX_ARRAY_LENGTH) {
      throw new IllegalArgumentException(
          "maxPointsInLeafNode must be <= ArrayUtil.MAX_ARRAY_LENGTH (= "
              + ArrayUtil.MAX_ARRAY_LENGTH
              + "); got "
              + maxPointsInLeafNode);
    }
  }

  /** numDims * bytesPerDim */
  public int packedBytesLength() {
    return numDims * bytesPerDim;
  }

  /** numIndexDims * bytesPerDim */
  public int packedIndexBytesLength() {
    return numIndexDims * bytesPerDim;
  }

  /** (numDims * bytesPerDim) + Integer.BYTES (packedBytesLength plus docID size) */
  public int bytesPerDoc() {
    return packedBytesLength() + Integer.BYTES;
  }
}
