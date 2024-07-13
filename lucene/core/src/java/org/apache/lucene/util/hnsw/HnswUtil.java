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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
   * Returns true iff every level of the graph is rooted - that is every node is reachable from the
   * entry node on each level.
   */
  public static boolean isRooted(HnswGraph knnValues) throws IOException {
    for (int level = 0; level < knnValues.numLevels(); level++) {
      if (components(knnValues, level, false).size() > 1) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns true iff every level of the graph is strongly connected - that is every node is
   * reachable from the every other node.
   */
  public static boolean isStronglyConnected(HnswGraph knnValues) throws IOException {
    for (int level = 0; level < knnValues.numLevels(); level++) {
      if (components(knnValues, level, true).size() > 1) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns the sizes of the distinct strongly-connected graph components on level 0. If the graph
   * is fully-connected there will only be a single component. If the graph is empty, the returned
   * list will be empty.
   */
  public static List<Integer> componentSizes(HnswGraph hnsw) throws IOException {
    return componentSizes(hnsw, 0);
  }

  /**
   * Returns the sizes of the distinct strongly-connected graph components on the given level. If
   * the graph is strongly-connected there will only be a single component. If the graph is empty,
   * the returned list will be empty.
   */
  public static List<Integer> componentSizes(HnswGraph hnsw, int level) throws IOException {
    return components(hnsw, level, true).stream().map(Component::size).toList();
  }

  // Finds connected components of the graph level. If strong==true, check for strongly-connected
  // (bidirectional links) otherwise check for "rooted" connection from the first node in the graph
  // level.
  static List<Component> components(HnswGraph hnsw, int level, boolean strong) throws IOException {
    List<Component> components = new ArrayList<>();
    FixedBitSet connectedNodes = new FixedBitSet(hnsw.size());
    assert hnsw.size() == hnsw.getNodesOnLevel(0).size();
    int total = 0;
    HnswGraph.NodesIterator nodesIterator = hnsw.getNodesOnLevel(level);
    int nextClear = nodesIterator.nextInt();
    outer:
    while (nextClear != NO_MORE_DOCS) {
      Component component = markComponent(hnsw, level, connectedNodes, nextClear, strong);
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

  /**
   * Count the nodes in a component of the graph and set the bits of its nodes in connectedNodes
   * bitset.
   *
   * @param level the level of the graph to check
   * @param connectedNodes a bitset the size of the entire graph with 1's indicating nodes that have
   *     been marked as connected. This method updates the bitset.
   * @param entryPoint a node id to start at
   * @param strong check for "strongly connected" vs "rooted"
   */
  private static Component markComponent(
      HnswGraph hnswGraph, int level, FixedBitSet connectedNodes, int entryPoint, boolean strong)
      throws IOException {
    if (strong) {
      return markStronglyConnected(hnswGraph, level, connectedNodes, entryPoint);
    } else {
      return markRooted(hnswGraph, level, connectedNodes, entryPoint);
    }
  }

  /**
   * Count the nodes in a rooted component of the graph and set the bits of its nodes in
   * connectedNodes bitset. Rooted means nodes that can be reached from a root node.
   *
   * @param hnswGraph the graph to check
   * @param level the level of the graph to check
   * @param connectedNodes a bitset the size of the entire graph with 1's indicating nodes that have
   *     been marked as connected. This method updates the bitset.
   * @param entryPoint a node id to start at
   */
  private static Component markRooted(
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

  /**
   * Count the nodes in a strongly-connected component of the graph and set the bits of its nodes in
   * connectedNodes bitset. Strongly-connected means nodes that can all reach other, including the
   * entry point.
   *
   * @param hnswGraph the graph to check
   * @param level the level of the graph to check
   * @param visited a bitset the size of the entire graph with 1's indicating nodes that have been
   *     marked as connected to some component. This method updates the bitset.
   * @param entryPoint a node id to start at
   */
  private static Component markStronglyConnected(
      HnswGraph hnswGraph, int level, FixedBitSet visited, int entryPoint) throws IOException {
    // Start at entry point and search all nodes on this level
    assert visited.get(entryPoint) == false;
    FixedBitSet componentNodes = new FixedBitSet(visited.length());
    Set<Integer> stack = new HashSet<>();
    Set<Integer> revisit = new HashSet<>();
    stack.add(entryPoint);
    int count = 0;
    int lastCount;
    do {
      lastCount = count;
      while (!stack.isEmpty()) {
        int node = stack.iterator().next();
        stack.remove(node);
        if (visited.get(node)) {
          continue;
        }
        // FIXME this stronglyConnected is using the global visited (connectedNodes) map
        // when it should only be considering nodes connected in *this component*
        if (node == entryPoint || stronglyConnected(hnswGraph, level, componentNodes, node)) {
          count++;
          visited.set(node);
          componentNodes.set(node);
        } else {
          // If this node doesn't link back to any connected nodes then we can't label it "strongly"
          // connected, at least not yet.  We still might find it is so after labeling more nodes.
          // Put it on a revisit stack for later evaluation
          // hmm we also need to prevent adding it to stack so we don't get in a cycle here
          revisit.add(node);
        }
        hnswGraph.seek(level, node);
        int friendOrd;
        while ((friendOrd = hnswGraph.nextNeighbor()) != NO_MORE_DOCS) {
          if (visited.get(friendOrd) == false && revisit.contains(friendOrd) == false) {
            stack.add(friendOrd);
          }
        }
      }
      // swap stack and revisit
      Set<Integer> swap = stack;
      stack = revisit;
      revisit = swap;
    } while (count > lastCount);
    return new Component(entryPoint, count);
  }

  private static boolean stronglyConnected(
      HnswGraph hnswGraph, int level, FixedBitSet connectedNodes, int node) throws IOException {
    hnswGraph.seek(level, node);
    int friendOrd;
    while ((friendOrd = hnswGraph.nextNeighbor()) != NO_MORE_DOCS) {
      if (connectedNodes.get(friendOrd)) {
        return true;
      }
    }
    return false;
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

  /**
   * In graph theory, "connected components" are really defined only for undirected (ie
   * bidirectional) graphs. Our graphs are directed, because of pruning, but they are *mostly*
   * undirected. In this case we compute components starting from a single node so what we are
   * really measuring is whether the graph is a "rooted graph". TODO: measure whether the graph is
   * "strongly connected" ie there is a path from every node to every other node.
   */
  public static boolean graphIsRooted(IndexReader reader, String vectorField) throws IOException {
    for (LeafReaderContext ctx : reader.leaves()) {
      CodecReader codecReader = (CodecReader) FilterLeafReader.unwrap(ctx.reader());
      HnswGraph graph =
          ((HnswGraphProvider)
                  ((PerFieldKnnVectorsFormat.FieldsReader) codecReader.getVectorReader())
                      .getFieldReader(vectorField))
              .getGraph(vectorField);
      if (isRooted(graph) == false) {
        return false;
      }
    }
    return true;
  }

  /**
   * A component (also "connected component") of an undirected graph is a collection of nodes that
   * are connected by neighbor links: every node in a connected component is reachable from every
   * other node in the component. See https://en.wikipedia.org/wiki/Component_(graph_theory). Such a
   * graph is said to be "fully connected" <i>iff</i> it has a single component, or it is empty.
   *
   * @param start the lowest-numbered node in the component
   * @param size the number of nodes in the component
   */
  record Component(int start, int size) {}
}
