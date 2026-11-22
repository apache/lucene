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
package org.apache.lucene.index;

import java.io.IOException;
import org.apache.lucene.search.DocIdSetIterator;

/**
 * Merges the doc iterators of several doc-values layers (ordered newest first) into the union of
 * their docs and tracks which layer supplies the current value: the newest layer positioned on the
 * current doc. Backed by a binary min-heap keyed by (docID, layer index), so advancing only
 * re-positions the layers sitting on the current doc (the rest keep their place) instead of
 * scanning every layer per doc. Shared by {@link OverlayNumericDocValues} and {@link
 * OverlayBinaryDocValues}; layers are advanced with {@code advance} only (never {@code
 * advanceExact}) so iteration and random access can be interleaved on the same instance.
 */
final class DocValuesOverlayMerger {

  private final DocValuesIterator[] layers; // newest first
  private final int[] heap; // 1-based; holds layer indices, ordered by (docID, index)
  private final int size;
  private int docID = -1;
  private int winner = -1;

  DocValuesOverlayMerger(DocValuesIterator[] layers) {
    this.layers = layers;
    this.size = layers.length;
    this.heap = new int[size + 1];
    // all layers start at docID -1, so indices in natural order are already a valid min-heap
    for (int i = 0; i < size; i++) {
      heap[i + 1] = i;
    }
  }

  int docID() {
    return docID;
  }

  /** Index of the layer supplying the current value, or -1 if the current doc has no value. */
  int valueLayer() {
    return winner;
  }

  int nextDoc() throws IOException {
    return advance(docID + 1);
  }

  int advance(int target) throws IOException {
    while (layers[heap[1]].docID() < target) {
      layers[heap[1]].advance(target);
      siftDown();
    }
    docID = layers[heap[1]].docID();
    winner = docID == DocIdSetIterator.NO_MORE_DOCS ? -1 : heap[1];
    return docID;
  }

  boolean advanceExact(int target) throws IOException {
    while (layers[heap[1]].docID() < target) {
      layers[heap[1]].advance(target);
      siftDown();
    }
    docID = target;
    winner = layers[heap[1]].docID() == target ? heap[1] : -1;
    return winner != -1;
  }

  long cost() {
    // Over-estimates the union (a doc in several layers counts once per layer); cost() is only an
    // upper-bound hint.
    long cost = 0;
    for (DocValuesIterator layer : layers) {
      cost += layer.cost();
    }
    return cost;
  }

  /** Restore the heap after the root layer advanced (its docID only ever increases). */
  private void siftDown() {
    int i = 1;
    int node = heap[1];
    while (true) {
      int left = i << 1;
      if (left > size) {
        break;
      }
      int right = left + 1;
      int child = right <= size && less(heap[right], heap[left]) ? right : left;
      if (less(heap[child], node) == false) {
        break;
      }
      heap[i] = heap[child];
      i = child;
    }
    heap[i] = node;
  }

  private boolean less(int a, int b) {
    int da = layers[a].docID();
    int db = layers[b].docID();
    if (da != db) {
      return da < db;
    }
    return a < b; // same doc: the newer layer (smaller index) wins
  }
}
