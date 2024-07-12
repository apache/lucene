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
public class HnswUtil {

  /**
   * Returns true iff level 0 of the graph is fully connected - that is every node is reachable from
   * any entry point.
   */
  public static boolean isFullyConnected(HnswGraph knnValues) throws IOException {
    for (int level = 0; level < knnValues.numLevels(); level++) {
      if (components(knnValues, level).size() > 1) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns the sizes of the distinct graph components on level 0. If the graph is fully-connected
   * there will only be a single component. If the graph is empty, the returned list will be empty.
   */
  public static List<Integer> componentSizes(HnswGraph hnsw) throws IOException {
    return componentSizes(hnsw, 0);
  }

  /**
   * Returns the sizes of the distinct graph components on the given level. If the graph is
   * fully-connected there will only be a single component. If the graph is empty, the returned list
   * will be empty.
   */
  public static List<Integer> componentSizes(HnswGraph hnsw, int level) throws IOException {
    return components(hnsw, level).stream().map(Component::size).toList();
  }

  // Finds all connected components of the graph
  static List<Component> components(HnswGraph hnsw, int level) throws IOException {
    List<Component> components = new ArrayList<>();
    FixedBitSet connectedNodes = new FixedBitSet(hnsw.size());
    assert hnsw.size() == hnsw.getNodesOnLevel(0).size();
    int total = 0;
    HnswGraph.NodesIterator nodesIterator = hnsw.getNodesOnLevel(level);
    int nextClear = nodesIterator.nextInt();
    outer:
    while (nextClear != NO_MORE_DOCS) {
      Component component = traverseConnectedNodes(hnsw, level, connectedNodes, nextClear);
      assert component.size() > 0;
      components.add(component);
      total += component.size();
      if (level == 0) {
        nextClear = nextClearBit(connectedNodes, component.start());
      } else {
        while (nodesIterator.hasNext()) {
          nextClear = nodesIterator.nextInt();
          if (connectedNodes.get(nextClear) == false) {
            continue outer;
          }
        }
        nextClear = NO_MORE_DOCS;
      }
    }
    assert total == hnsw.getNodesOnLevel(level).size()
        : "total="
            + total
            + " level nodes on level "
            + level
            + " = "
            + hnsw.getNodesOnLevel(level).size();
    return components;
  }

  // count the nodes in a connected component of the graph and set the bits of its nodes in
  // connectedNodes bitset
  private static Component traverseConnectedNodes(
      HnswGraph hnswGraph, int level, FixedBitSet connectedNodes, int entryPoint)
      throws IOException {
    // Start at entry point and search all nodes on this level
    assert connectedNodes.get(entryPoint) == false;
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
      hnswGraph.seek(level, node);
      int friendOrd;
      while ((friendOrd = hnswGraph.nextNeighbor()) != NO_MORE_DOCS) {
        stack.push(friendOrd);
      }
    }
    return new Component(entryPoint, count);
  }

  private static int nextClearBit(FixedBitSet bits, int index) {
    // Does not depend on the ghost bits being clear!
    long[] barray = bits.getBits();
    assert index >= 0 && index < bits.length() : "index=" + index + ", numBits=" + bits.length();
    int i = index >> 6;
    long word = ~(barray[i] >> index); // skip all the bits to the right of index

    int next = NO_MORE_DOCS;
    if (word != 0) {
      next = index + Long.numberOfTrailingZeros(word);
    } else {
      while (++i < barray.length) {
        word = ~barray[i];
        if (word != 0) {
          next = (i << 6) + Long.numberOfTrailingZeros(word);
          break;
        }
      }
    }
    if (next >= bits.length()) {
      return NO_MORE_DOCS;
    } else {
      return next;
    }
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

  record Component(int start, int size) {}
}
