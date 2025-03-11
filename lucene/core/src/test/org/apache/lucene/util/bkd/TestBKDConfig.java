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

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.ArrayUtil;

public class TestBKDConfig extends LuceneTestCase {

  public void testInvalidNumDims() {
    IllegalArgumentException ex =
        expectThrows(
            IllegalArgumentException.class,
            () -> new BKDConfig(0, 0, 8, BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE));
    assertTrue(ex.getMessage().contains("numDims must be 1 .. " + BKDConfig.MAX_DIMS));
  }

  public void testInvalidNumIndexedDims() {
    {
      IllegalArgumentException ex =
          expectThrows(
              IllegalArgumentException.class,
              () -> new BKDConfig(1, 0, 8, BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE));
      assertTrue(ex.getMessage().contains("numIndexDims must be 1 .. " + BKDConfig.MAX_INDEX_DIMS));
    }
    {
      IllegalArgumentException ex =
          expectThrows(
              IllegalArgumentException.class,
              () -> new BKDConfig(1, 2, 8, BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE));
      assertTrue(ex.getMessage().contains("numIndexDims cannot exceed numDims"));
    }
  }

  public void testInvalidBytesPerDim() {
    IllegalArgumentException ex =
        expectThrows(
            IllegalArgumentException.class,
            () -> new BKDConfig(1, 1, 0, BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE));
    assertTrue(ex.getMessage().contains("bytesPerDim must be > 0"));
  }

  public void testInvalidMaxPointsPerLeafNode() {
    {
      IllegalArgumentException ex =
          expectThrows(IllegalArgumentException.class, () -> new BKDConfig(1, 1, 8, -1));
      assertTrue(ex.getMessage().contains("maxPointsInLeafNode must be > 0"));
    }
    {
      IllegalArgumentException ex =
          expectThrows(
              IllegalArgumentException.class,
              () -> new BKDConfig(1, 1, 8, ArrayUtil.MAX_ARRAY_LENGTH + 1));
      assertTrue(
          ex.getMessage().contains("maxPointsInLeafNode must be <= ArrayUtil.MAX_ARRAY_LENGTH"));
    }
  }

  public void testDeduplicate() {
    {
      for (int dims : new int[] {1, 2}) {
        for (int bytesPerDim : new int[] {2, 4, 8, 16}) {
          BKDConfig config =
              BKDConfig.of(dims, dims, bytesPerDim, BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE);
          BKDConfig deduped =
              BKDConfig.of(dims, dims, bytesPerDim, BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE);
          assertSame(deduped, config);
        }
      }
    }
    {
      BKDConfig config = BKDConfig.of(7, 4, 4, BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE);
      BKDConfig deduped = BKDConfig.of(7, 4, 4, BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE);
      assertSame(deduped, config);
    }
    {
      BKDConfig config = BKDConfig.of(1, 1, 6, BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE);
      BKDConfig deduped = BKDConfig.of(1, 1, 6, BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE);
      assertNotSame(config, deduped);
    }
  }
}
