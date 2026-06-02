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

import java.util.List;
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

  private static final List<BKDConfig> DEFAULT_CONFIGS =
      List.of(
          // cover the most common types for 1 and 2 dimensions.
          new BKDConfig(1, 1, 2, DEFAULT_MAX_POINTS_IN_LEAF_NODE),
          new BKDConfig(1, 1, 4, DEFAULT_MAX_POINTS_IN_LEAF_NODE),
          new BKDConfig(1, 1, 8, DEFAULT_MAX_POINTS_IN_LEAF_NODE),
          new BKDConfig(1, 1, 16, DEFAULT_MAX_POINTS_IN_LEAF_NODE),
          new BKDConfig(2, 2, 2, DEFAULT_MAX_POINTS_IN_LEAF_NODE),
          new BKDConfig(2, 2, 4, DEFAULT_MAX_POINTS_IN_LEAF_NODE),
          new BKDConfig(2, 2, 8, DEFAULT_MAX_POINTS_IN_LEAF_NODE),
          new BKDConfig(2, 2, 16, DEFAULT_MAX_POINTS_IN_LEAF_NODE),
          // cover lucene shapes
          new BKDConfig(7, 4, 4, DEFAULT_MAX_POINTS_IN_LEAF_NODE));

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

  public static BKDConfig of(
      int numDims, int numIndexDims, int bytesPerDim, int maxPointsInLeafNode) {
    final BKDConfig config = new BKDConfig(numDims, numIndexDims, bytesPerDim, maxPointsInLeafNode);
    final int defaultConfigIndex = BKDConfig.DEFAULT_CONFIGS.indexOf(config);
    if (defaultConfigIndex != -1) {
      return BKDConfig.DEFAULT_CONFIGS.get(defaultConfigIndex);
    } else {
      return config;
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
