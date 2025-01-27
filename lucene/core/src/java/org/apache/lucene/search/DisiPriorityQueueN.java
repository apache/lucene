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
package org.apache.lucene.search;

import java.util.Arrays;
import java.util.Iterator;

final class DisiPriorityQueueN extends DisiPriorityQueue {

  static int leftNode(int node) {
    return ((node + 1) << 1) - 1;
  }

  static int rightNode(int leftNode) {
    return leftNode + 1;
  }

  static int parentNode(int node) {
    return ((node + 1) >>> 1) - 1;
  }

  private final DisiWrapper[] heap;
  private int size;

  DisiPriorityQueueN(int maxSize) {
    heap = new DisiWrapper[maxSize];
    size = 0;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public DisiWrapper top() {
    return heap[0];
  }

  @Override
  public DisiWrapper top2() {
    switch (size()) {
      case 0:
      case 1:
        return null;
      case 2:
        return heap[1];
      default:
        if (heap[1].doc <= heap[2].doc) {
          return heap[1];
        } else {
          return heap[2];
        }
    }
  }

  @Override
  public DisiWrapper topList() {
    final DisiWrapper[] heap = this.heap;
    final int size = this.size;
    DisiWrapper list = heap[0];
    list.next = null;
    if (size >= 3) {
      list = topList(list, heap, size, 1);
      list = topList(list, heap, size, 2);
    } else if (size == 2 && heap[1].doc == list.doc) {
      list = prepend(heap[1], list);
    }
    return list;
  }

  // prepend w1 (iterator) to w2 (list)
  private DisiWrapper prepend(DisiWrapper w1, DisiWrapper w2) {
    w1.next = w2;
    return w1;
  }

  private DisiWrapper topList(DisiWrapper list, DisiWrapper[] heap, int size, int i) {
    final DisiWrapper w = heap[i];
    if (w.doc == list.doc) {
      list = prepend(w, list);
      final int left = leftNode(i);
      final int right = rightNode(left);
      if (right < size) {
        list = topList(list, heap, size, left);
        list = topList(list, heap, size, right);
      } else if (left < size && heap[left].doc == list.doc) {
        list = prepend(heap[left], list);
      }
    }
    return list;
  }

  @Override
  public DisiWrapper add(DisiWrapper entry) {
    final DisiWrapper[] heap = this.heap;
    final int size = this.size;
    heap[size] = entry;
    upHeap(size);
    this.size = size + 1;
    return heap[0];
  }

  @Override
  public void addAll(DisiWrapper[] entries, int offset, int len) {
    // Nothing to do if empty:
    if (len == 0) {
      return;
    }

    // Fail early if we're going to over-fill:
    if (size + len > heap.length) {
      throw new IndexOutOfBoundsException(
          "Cannot add "
              + len
              + " elements to a queue with remaining capacity "
              + (heap.length - size));
    }

    // Copy the entries over to our heap array:
    System.arraycopy(entries, offset, heap, size, len);
    size += len;

    // Heapify in bulk:
    final int firstLeafIndex = size >>> 1;
    for (int rootIndex = firstLeafIndex - 1; rootIndex >= 0; rootIndex--) {
      int parentIndex = rootIndex;
      DisiWrapper parent = heap[parentIndex];
      while (parentIndex < firstLeafIndex) {
        int childIndex = leftNode(parentIndex);
        int rightChildIndex = rightNode(childIndex);
        DisiWrapper child = heap[childIndex];
        if (rightChildIndex < size && heap[rightChildIndex].doc < child.doc) {
          child = heap[rightChildIndex];
          childIndex = rightChildIndex;
        }
        if (child.doc >= parent.doc) {
          break;
        }
        heap[parentIndex] = child;
        parentIndex = childIndex;
      }
      heap[parentIndex] = parent;
    }
  }

  @Override
  public DisiWrapper pop() {
    final DisiWrapper[] heap = this.heap;
    final DisiWrapper result = heap[0];
    final int i = --size;
    heap[0] = heap[i];
    heap[i] = null;
    downHeap(i);
    return result;
  }

  @Override
  public DisiWrapper updateTop() {
    downHeap(size);
    return heap[0];
  }

  @Override
  DisiWrapper updateTop(DisiWrapper topReplacement) {
    heap[0] = topReplacement;
    return updateTop();
  }

  @Override
  public void clear() {
    Arrays.fill(heap, null);
    size = 0;
  }

  void upHeap(int i) {
    final DisiWrapper node = heap[i];
    final int nodeDoc = node.doc;
    int j = parentNode(i);
    while (j >= 0 && nodeDoc < heap[j].doc) {
      heap[i] = heap[j];
      i = j;
      j = parentNode(j);
    }
    heap[i] = node;
  }

  void downHeap(int size) {
    int i = 0;
    final DisiWrapper node = heap[0];
    int j = leftNode(i);
    if (j < size) {
      int k = rightNode(j);
      if (k < size && heap[k].doc < heap[j].doc) {
        j = k;
      }
      if (heap[j].doc < node.doc) {
        do {
          heap[i] = heap[j];
          i = j;
          j = leftNode(i);
          k = rightNode(j);
          if (k < size && heap[k].doc < heap[j].doc) {
            j = k;
          }
        } while (j < size && heap[j].doc < node.doc);
        heap[i] = node;
      }
    }
  }

  @Override
  public Iterator<DisiWrapper> iterator() {
    return Arrays.asList(heap).subList(0, size).iterator();
  }
}
