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
import org.apache.lucene.util.IntroSorter;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.RamUsageEstimator;

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

  /** Approximate RAM used by this buffered, resolved update packet. */
  long ramBytesUsed() {
    return RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
        + RamUsageEstimator.sizeOf(field)
        + 3L * Integer.BYTES // maxDoc, dimension, size
        + Long.BYTES // delGen
        + 1 // finished
        + RamUsageEstimator.sizeOf(docs)
        + valuesRamBytesUsed();
  }

  /** RAM used by the subclass's value storage (the float[][] / byte[][] vectors). */
  protected abstract long valuesRamBytesUsed();

  /** Freezes structures and stably sorts updates by docID (last write to a docID wins). */
  final void finish() {
    if (finished) {
      throw new IllegalStateException("already finished");
    }
    finished = true;
    if (size <= 1) {
      return;
    }
    // In-place stable sort by docID, matching DocValuesFieldUpdates#finish: quicksort with an
    // explicit ords array to break ties in original insertion order (so the last update to a docID
    // wins), avoiding any per-element boxing/allocation.
    final int[] ords = new int[size];
    for (int i = 0; i < size; i++) {
      ords[i] = i;
    }
    new IntroSorter() {
      @Override
      protected void swap(int i, int j) {
        int tmpOrd = ords[i];
        ords[i] = ords[j];
        ords[j] = tmpOrd;
        int tmpDoc = docs[i];
        docs[i] = docs[j];
        docs[j] = tmpDoc;
        KnnVectorFieldUpdates.this.swapValues(i, j);
      }

      @Override
      protected int compare(int i, int j) {
        int cmp = Integer.compare(docs[i], docs[j]);
        return cmp != 0 ? cmp : Integer.compare(ords[i], ords[j]);
      }

      private int pivotDoc;
      private int pivotOrd;

      @Override
      protected void setPivot(int i) {
        pivotDoc = docs[i];
        pivotOrd = ords[i];
      }

      @Override
      protected int comparePivot(int j) {
        int cmp = Integer.compare(pivotDoc, docs[j]);
        return cmp != 0 ? cmp : Integer.compare(pivotOrd, ords[j]);
      }
    }.sort(0, size);
  }

  /** Swaps the subclass's value storage at the two ordinals (used by the in-place sort). */
  protected abstract void swapValues(int i, int j);

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
    protected void swapValues(int i, int j) {
      float[] tmp = values[i];
      values[i] = values[j];
      values[j] = tmp;
    }

    @Override
    protected long valuesRamBytesUsed() {
      // backing float[][] references + the size populated float[dimension] vectors
      return RamUsageEstimator.shallowSizeOf(values)
          + (long) size
              * (RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) dimension * Float.BYTES);
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
    protected void swapValues(int i, int j) {
      byte[] tmp = values[i];
      values[i] = values[j];
      values[j] = tmp;
    }

    @Override
    protected long valuesRamBytesUsed() {
      // backing byte[][] references + the size populated byte[dimension] vectors
      return RamUsageEstimator.shallowSizeOf(values)
          + (long) size * (RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + dimension);
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
