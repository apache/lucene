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
package org.apache.lucene.sandbox.codecs.dedup;

import java.io.IOException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectReader;

/**
 * Resolves a HYBRID graph node ordinal to its group-view ordinal. Backed on-heap while writing and
 * off-heap while reading.
 */
sealed interface NodeToGroupOrd
    permits NodeToGroupOrd.NodeToGroupOrdArray, NodeToGroupOrd.NodeToGroupOrdOffHeap {

  /** The group-view ordinal backing the vector of {@code node}. */
  int get(int node);

  /** Number of hybrid nodes. */
  int size();

  /** On-heap map used while building the graph. */
  record NodeToGroupOrdArray(int[] nodeToGroupOrd) implements NodeToGroupOrd {

    @Override
    public int get(int node) {
      return nodeToGroupOrd[node];
    }

    @Override
    public int size() {
      return nodeToGroupOrd.length;
    }
  }

  /** Off-heap map used while reading, backed by a {@link DirectReader}. */
  record NodeToGroupOrdOffHeap(
      long nodeToGroupOrdOffset,
      long nodeToGroupOrdSize,
      int nodeToGroupOrdBitsPerValue,
      int size,
      LongValues values)
      implements NodeToGroupOrd {

    NodeToGroupOrdOffHeap(
        IndexInput graphData,
        long nodeToGroupOrdOffset,
        long nodeToGroupOrdSize,
        int nodeToGroupOrdBitsPerValue,
        int size)
        throws IOException {
      RandomAccessInput slice =
          graphData.randomAccessSlice(nodeToGroupOrdOffset, nodeToGroupOrdSize);
      LongValues values = DirectReader.getInstance(slice, nodeToGroupOrdBitsPerValue);
      this(nodeToGroupOrdOffset, nodeToGroupOrdSize, nodeToGroupOrdBitsPerValue, size, values);
    }

    @Override
    public int get(int node) {
      return (int) values.get(node);
    }

    @Override
    public int size() {
      return size;
    }
  }
}
