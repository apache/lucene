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

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.util.Comparator;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.PriorityQueue;

/**
 * Holds in-place KNN vector updates of a single field, for a set of documents within one segment.
 * This is the vector analogue of {@link DocValuesFieldUpdates}: it accumulates (docID -&gt; new
 * vector) entries during resolution, sorts them by docID at {@link #finish()} (stable, so that the
 * last update to a docID within a packet wins), and exposes an {@link Iterator} that the write path
 * overlays onto the existing vectors.
 *
 * @lucene.experimental
 */
abstract class KnnVectorFieldUpdates {

  final String field;
  final VectorEncoding encoding;
  final long delGen;
  final int maxDoc;
  final int dimension;
  protected int[] docs = new int[8];
  protected int size;
  private boolean finished;

  protected KnnVectorFieldUpdates(
      int maxDoc, long delGen, String field, VectorEncoding encoding, int dimension) {
    this.maxDoc = maxDoc;
    this.delGen = delGen;
    this.field = field;
    this.encoding = encoding;
    this.dimension = dimension;
  }

  /** Reserves a slot for the next doc and returns its ordinal. Subclasses store the value. */
  protected final int reserve(int doc) {
    if (finished) {
      throw new IllegalStateException("already finished");
    }
    assert doc < maxDoc;
    if (size == docs.length) {
      docs = ArrayUtil.grow(docs, size + 1);
    }
    docs[size] = doc;
    return size++;
  }

  boolean getFinished() {
    return finished;
  }

  boolean any() {
    return size > 0;
  }

  /** Freezes structures and stably sorts updates by docID (last write to a docID wins). */
  final void finish() {
    if (finished) {
      throw new IllegalStateException("already finished");
    }
    finished = true;
    if (size <= 1) {
      return;
    }
    // Stable sort by docID: build an index permutation, sort by (docID asc, original ord asc),
    // then materialize. Vectors are large so we accept the extra allocation for clarity.
    Integer[] order = new Integer[size];
    for (int i = 0; i < size; i++) {
      order[i] = i;
    }
    final int[] docsSnapshot = docs;
    java.util.Arrays.sort(
        order, Comparator.comparingInt((Integer i) -> docsSnapshot[i]).thenComparingInt(i -> i));
    int[] newDocs = new int[size];
    for (int i = 0; i < size; i++) {
      newDocs[i] = docs[order[i]];
    }
    reorderValues(order);
    docs = newDocs;
  }

  /** Reorders the subclass's value storage according to the given permutation of ordinals. */
  protected abstract void reorderValues(Integer[] order);

  /** Returns an {@link Iterator} over the updated documents and their vector values. */
  abstract Iterator iterator();

  /** An iterator over updated documents (in increasing docID order) and their vectors. */
  abstract static class Iterator {
    /** Advances to the next updated doc, or returns {@link DocIdSetIterator#NO_MORE_DOCS}. */
    abstract int nextDoc();

    abstract int docID();

    abstract long delGen();

    /** The float vector for the current doc (only for {@link VectorEncoding#FLOAT32}). */
    abstract float[] floatValue();

    /** The byte vector for the current doc (only for {@link VectorEncoding#BYTE}). */
    abstract byte[] byteValue();
  }

  /**
   * Merge-sorts multiple iterators, one per delGen, favoring the largest delGen that has updates
   * for a given docID (last-write-wins across packets). Mirrors {@link
   * DocValuesFieldUpdates#mergedIterator}.
   */
  static Iterator mergedIterator(Iterator[] subs) {
    if (subs.length == 1) {
      return subs[0];
    }
    // sort by smaller docID, then larger delGen
    PriorityQueue<Iterator> queue =
        PriorityQueue.usingComparator(
            subs.length,
            Comparator.comparingInt(Iterator::docID)
                .thenComparing(Comparator.comparingLong(Iterator::delGen).reversed()));
    for (Iterator sub : subs) {
      if (sub.nextDoc() != NO_MORE_DOCS) {
        queue.add(sub);
      }
    }

    if (queue.size() == 0) {
      return null;
    }

    return new Iterator() {
      private int doc = -1;

      @Override
      int nextDoc() {
        // Advance all sub iterators past current doc
        while (true) {
          if (queue.size() == 0) {
            doc = NO_MORE_DOCS;
            break;
          }
          int newDoc = queue.top().docID();
          if (newDoc != doc) {
            assert newDoc > doc : "doc=" + doc + " newDoc=" + newDoc;
            doc = newDoc;
            break;
          }
          if (queue.top().nextDoc() == NO_MORE_DOCS) {
            queue.pop();
          } else {
            queue.updateTop();
          }
        }
        return doc;
      }

      @Override
      int docID() {
        return doc;
      }

      @Override
      float[] floatValue() {
        return queue.top().floatValue();
      }

      @Override
      byte[] byteValue() {
        return queue.top().byteValue();
      }

      @Override
      long delGen() {
        throw new UnsupportedOperationException();
      }
    };
  }

  /** Float vector updates for one field/segment/packet. */
  static final class FloatKnnVectorFieldUpdates extends KnnVectorFieldUpdates {
    private float[][] values = new float[8][];

    FloatKnnVectorFieldUpdates(int maxDoc, long delGen, String field, int dimension) {
      super(maxDoc, delGen, field, VectorEncoding.FLOAT32, dimension);
    }

    void add(int doc, float[] value) {
      assert value.length == dimension;
      int ord = reserve(doc);
      if (ord == values.length) {
        values = ArrayUtil.grow(values, ord + 1);
      }
      values[ord] = value;
    }

    @Override
    protected void reorderValues(Integer[] order) {
      float[][] newValues = new float[size][];
      for (int i = 0; i < size; i++) {
        newValues[i] = values[order[i]];
      }
      values = newValues;
    }

    @Override
    Iterator iterator() {
      if (getFinished() == false) {
        throw new IllegalStateException("call finish first");
      }
      return new Iterator() {
        private int idx = -1;

        @Override
        int nextDoc() {
          idx++;
          if (idx >= size) {
            return NO_MORE_DOCS;
          }
          // skip to the last entry for this doc (stable sort => last update wins)
          while (idx + 1 < size && docs[idx + 1] == docs[idx]) {
            idx++;
          }
          return docs[idx];
        }

        @Override
        int docID() {
          return idx < 0 ? -1 : (idx >= size ? NO_MORE_DOCS : docs[idx]);
        }

        @Override
        long delGen() {
          return delGen;
        }

        @Override
        float[] floatValue() {
          return values[idx];
        }

        @Override
        byte[] byteValue() {
          throw new UnsupportedOperationException();
        }
      };
    }
  }

  /** Byte vector updates for one field/segment/packet. */
  static final class ByteKnnVectorFieldUpdates extends KnnVectorFieldUpdates {
    private byte[][] values = new byte[8][];

    ByteKnnVectorFieldUpdates(int maxDoc, long delGen, String field, int dimension) {
      super(maxDoc, delGen, field, VectorEncoding.BYTE, dimension);
    }

    void add(int doc, byte[] value) {
      assert value.length == dimension;
      int ord = reserve(doc);
      if (ord == values.length) {
        values = ArrayUtil.grow(values, ord + 1);
      }
      values[ord] = value;
    }

    @Override
    protected void reorderValues(Integer[] order) {
      byte[][] newValues = new byte[size][];
      for (int i = 0; i < size; i++) {
        newValues[i] = values[order[i]];
      }
      values = newValues;
    }

    @Override
    Iterator iterator() {
      if (getFinished() == false) {
        throw new IllegalStateException("call finish first");
      }
      return new Iterator() {
        private int idx = -1;

        @Override
        int nextDoc() {
          idx++;
          if (idx >= size) {
            return NO_MORE_DOCS;
          }
          while (idx + 1 < size && docs[idx + 1] == docs[idx]) {
            idx++;
          }
          return docs[idx];
        }

        @Override
        int docID() {
          return idx < 0 ? -1 : (idx >= size ? NO_MORE_DOCS : docs[idx]);
        }

        @Override
        long delGen() {
          return delGen;
        }

        @Override
        float[] floatValue() {
          throw new UnsupportedOperationException();
        }

        @Override
        byte[] byteValue() {
          return values[idx];
        }
      };
    }
  }
}
