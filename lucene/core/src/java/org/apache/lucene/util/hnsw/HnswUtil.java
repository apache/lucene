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
package org.apache.lucene.util.hnsw;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import org.apache.lucene.codecs.hnsw.HnswGraphProvider;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.FixedBitSet;

/** Utilities for use in tests involving HNSW graphs */
public class HnswTestUtil {

  /**
   * Returns true iff level 0 of the graph is fully connected - that is every node is reachable from
   * any entry point.
   */
  public static boolean isFullyConnected(HnswGraph knnValues) throws IOException {
    return componentSizes(knnValues).size() < 2;
  }

  /**
   * Returns the sizes of the distinct graph components on level 0. If the graph is fully-connected
   * there will only be a single component. If the graph is empty, the returned list will be empty.
   */
  public static List<Integer> componentSizes(HnswGraph hnsw) throws IOException {
    List<Integer> sizes = new ArrayList<>();
    FixedBitSet connectedNodes = new FixedBitSet(hnsw.size());
    assert hnsw.size() == hnsw.getNodesOnLevel(0).size();
    int total = 0;
    while (total < connectedNodes.length()) {
      int componentSize = traverseConnectedNodes(hnsw, connectedNodes);
      assert componentSize > 0;
      sizes.add(componentSize);
      total += componentSize;
    }
    return sizes;
  }

  // count the nodes in a connected component of the graph and set the bits of its nodes in
  // connectedNodes bitset
  private static int traverseConnectedNodes(HnswGraph hnswGraph, FixedBitSet connectedNodes)
      throws IOException {
    // Start at entry point and search all nodes on this level
    int entryPoint = nextClearBit(connectedNodes, 0);
    if (entryPoint == NO_MORE_DOCS) {
      return 0;
    }
    Deque<Integer> stack = new ArrayDeque<>();
    stack.push(entryPoint);
    int count = 0;
    while (!stack.isEmpty()) {
      int node = stack.pop();
      if (connectedNodes.get(node)) {
        continue;
      }
      count++;
      connectedNodes.set(node);
      hnswGraph.seek(0, node);
      int friendOrd;
      while ((friendOrd = hnswGraph.nextNeighbor()) != NO_MORE_DOCS) {
        stack.push(friendOrd);
      }
    }
    return count;
  }

  private static int nextClearBit(FixedBitSet bits, int index) {
    // Does not depend on the ghost bits being clear!
    long[] barray = bits.getBits();
    assert index >= 0 && index < bits.length() : "index=" + index + ", numBits=" + bits.length();
    int i = index >> 6;
    long word = ~(barray[i] >> index); // skip all the bits to the right of index

    if (word != 0) {
      return index + Long.numberOfTrailingZeros(word);
    }

    while (++i < barray.length) {
      word = ~barray[i];
      if (word != 0) {
        int next = (i << 6) + Long.numberOfTrailingZeros(word);
        if (next >= bits.length()) {
          return NO_MORE_DOCS;
        } else {
          return next;
        }
      }
    }
    return NO_MORE_DOCS;
  }

  public static boolean graphIsConnected(IndexReader reader, String vectorField)
      throws IOException {
    for (LeafReaderContext ctx : reader.leaves()) {
      CodecReader codecReader = (CodecReader) FilterLeafReader.unwrap(ctx.reader());
      HnswGraph graph =
          ((HnswGraphProvider)
                  ((PerFieldKnnVectorsFormat.FieldsReader) codecReader.getVectorReader())
                      .getFieldReader(vectorField))
              .getGraph(vectorField);
      if (isFullyConnected(graph) == false) {
        return false;
      }
    }
    return true;
  }
}
