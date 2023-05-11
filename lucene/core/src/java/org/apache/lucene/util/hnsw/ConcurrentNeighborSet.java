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

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.util.NumericUtils;

/**
 * A concurrent set of neighbors
 *
 * <p>Neighbors are stored in a concurrent navigable set by encoding ordinal and score together in a
 * long. This means we can quickly iterate either forwards, or backwards.
 *
 * <p>The maximum connection count is loosely maintained -- meaning, we tolerate temporarily
 * exceeding the max size by a number of elements up to the number of threads performing concurrent
 * inserts, but it will always be reduced back to the cap immediately afterwards. This avoids taking
 * out a Big Lock to impose a strict cap.
 */
public class ConcurrentNeighborSet {
  private final ConcurrentSkipListSet<Long> neighbors;
  private final int maxConnections;
  private final AtomicInteger size;

  public ConcurrentNeighborSet(int maxConnections) {
    this.maxConnections = maxConnections;
    neighbors = new ConcurrentSkipListSet<>(Comparator.<Long>naturalOrder().reversed());
    size = new AtomicInteger();
  }

  public Iterator<Integer> nodeIterator() {
    // don't use a stream here. stream's implementation of iterator buffers
    // very aggressively, which is a big waste for a lot of searches
    Iterator<Long> it = neighbors.iterator();
    return new Iterator<>() {
      public boolean hasNext() {
        return it.hasNext();
      }

      public Integer next() {
        return decodeNodeId(it.next());
      }
    };
  }

  public int size() {
    return size.get();
  }

  public int rawSize() {
    return neighbors.size();
  }

  public void forEach(ThrowingBiConsumer<Integer, Float> consumer) throws IOException {
    for (Long encoded : neighbors) {
      consumer.accept(decodeNodeId(encoded), decodeScore(encoded));
    }
  }

  public void forEachDescending(ThrowingBiConsumer<Integer, Float> consumer) throws IOException {
    for (Long encoded : neighbors.descendingSet()) {
      consumer.accept(decodeNodeId(encoded), decodeScore(encoded));
    }
  }

  /**
   * For each candidate (going from best to worst), select it only if it is closer to target than it
   * is to any of the already-selected neighbors. This is maintained whether those other neighbors
   * were selected by this method, or were added as a "backlink" to a node inserted concurrently
   * that chose this one as a neighbor.
   */
  public void insertDiverse(
      NeighborArray candidates, ThrowingBiFunction<Integer, Integer, Float> scoreBetween)
      throws IOException {
    for (int i = candidates.size() - 1; neighbors.size() < maxConnections && i >= 0; i--) {
      int cNode = candidates.node[i];
      float cScore = candidates.score[i];
      // TODO in the paper, the diversity requirement is only enforced when there are more than
      // maxConn
      if (isDiverse(cNode, cScore, scoreBetween)) {
        // raw inserts (invoked by other threads inserting neighbors) could happen concurrently,
        // so don't "cheat" and do a raw put()
        insert(cNode, cScore, scoreBetween);
      }
    }
    // TODO follow the paper's suggestion and fill up the rest of the neighbors with non-diverse
    // candidates?
  }

  /**
   * Insert a new neighbor, maintaining our size cap by removing the least diverse neighbor if
   * necessary.
   */
  public void insert(
      int node, float score, ThrowingBiFunction<Integer, Integer, Float> scoreBetween)
      throws IOException {
    // if two nodes are inserted concurrently, and see each other as neighbors,
    // we will try to add a duplicate entry to the set, so it is not correct to assume
    // that all calls will result in a new entry being added
    if (neighbors.add(encode(node, score))) {
      if (size.incrementAndGet() > maxConnections) {
        removeLeastDiverse(scoreBetween);
        size.decrementAndGet();
      }
    }
  }

  // is the candidate node with the given score closer to the base node than it is to any of the
  // existing neighbors
  private boolean isDiverse(
      int node, float score, ThrowingBiFunction<Integer, Integer, Float> scoreBetween)
      throws IOException {
    for (Long encoded : neighbors) {
      if (scoreBetween.apply(decodeNodeId(encoded), node) > score) {
        return false;
      }
    }
    return true;
  }

  /**
   * find the first node e1 starting with the last neighbor (i.e. least similar to the base node),
   * look at all nodes e2 that are closer to the base node than e1 is. if any e2 is closer to e1
   * than e1 is to the base node, remove e1.
   */
  private void removeLeastDiverse(ThrowingBiFunction<Integer, Integer, Float> scoreBetween)
      throws IOException {
    for (Long e1 : neighbors.descendingSet()) {
      int e1Id = decodeNodeId(e1);
      float baseScore = decodeScore(e1);

      Iterator<Long> e2Iterator = iteratorStartingAfter(neighbors, e1);
      while (e2Iterator.hasNext()) {
        Long e2 = e2Iterator.next();
        int e2Id = decodeNodeId(e2);
        Float e1e2Score = scoreBetween.apply(e1Id, e2Id);
        if (e1e2Score >= baseScore) {
          if (neighbors.remove(e1)) {
            return;
          }
          // else another thread already removed it, keep looking
        }
      }
    }
    // couldn't find any "non-diverse" neighbors, so remove the one farthest from the base node
    neighbors.remove(neighbors.last());
  }

  /**
   * Returns an iterator over the entries in the set, starting at the entry *after* the given key.
   * So iteratorStartingAfter(map, 2) invoked on a set with keys [1, 2, 3, 4] would return an
   * iterator over the entries [3, 4].
   */
  private static <K> Iterator<K> iteratorStartingAfter(NavigableSet<K> set, K key) {
    // this isn't ideal, since the iteration will be worst case O(N log N), but since the worst
    // scores will usually be the first ones we iterate through, the average case is much better
    return new Iterator<>() {
      private K nextItem = set.lower(key);

      @Override
      public boolean hasNext() {
        return nextItem != null;
      }

      @Override
      public K next() {
        K current = nextItem;
        nextItem = set.lower(nextItem);
        return current;
      }
    };
  }

  public boolean contains(int i) {
    for (Long e : neighbors) {
      if (decodeNodeId(e) == i) {
        return true;
      }
    }
    return false;
  }

  // as found in NeighborQueue
  static long encode(int node, float score) {
    return (((long) NumericUtils.floatToSortableInt(score)) << 32) | (0xFFFFFFFFL & ~node);
  }

  static float decodeScore(long heapValue) {
    return NumericUtils.sortableIntToFloat((int) (heapValue >> 32));
  }

  static int decodeNodeId(long heapValue) {
    return (int) ~(heapValue);
  }

  /** A BiFunction that can throw IOException. */
  @FunctionalInterface
  public interface ThrowingBiFunction<T, U, R> {
    R apply(T t, U u) throws IOException;
  }

  /** A BiConsumer that can throw IOException. */
  @FunctionalInterface
  public interface ThrowingBiConsumer<T, U> {
    void accept(T t, U u) throws IOException;
  }
}
