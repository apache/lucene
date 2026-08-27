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
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;

/**
 * Vector values restricted to {@code [start, end)} rather than merely masked.
 *
 * <p>Merging vectors walks every value with {@link KnnVectorValues.DocIndexIterator#nextDoc()} and
 * drops whatever the document map sends to {@code -1}, having already read it. Each output of a
 * partitioned merge would therefore read every vector of every input, and k outputs would read them
 * k times -- which for a vector index is most of the data. Seeking to the range instead makes the
 * outputs together read each vector once.
 *
 * <p>Only the iteration is restricted. Ordinals still belong to the reader underneath, so {@code
 * vectorValue(ord)} is unchanged and {@link KnnVectorValues#size()} still reports what that reader
 * holds: a caller reaches a value through the ordinal an iterator gave it, never by counting.
 */
final class DocRangeKnnVectorsReader extends KnnVectorsReader {

  private final KnnVectorsReader in;
  private final int start;
  private final int end;

  DocRangeKnnVectorsReader(KnnVectorsReader in, int start, int end) {
    this.in = in;
    this.start = start;
    this.end = end;
  }

  /**
   * The reader underneath, however many times it has been narrowed.
   *
   * <p>Every output of one partitioned merge narrows the same reader, so anything that must happen
   * once per reader -- {@link KnnVectorsReader#finishMerge()} -- has to be able to tell that these
   * are the same reader.
   */
  static KnnVectorsReader unwrap(KnnVectorsReader reader) {
    while (reader instanceof DocRangeKnnVectorsReader ranged) {
      reader = ranged.in;
    }
    return reader;
  }

  private KnnVectorValues.DocIndexIterator rangeIterator(KnnVectorValues.DocIndexIterator it) {
    return new KnnVectorValues.DocIndexIterator() {
      private int doc = -1;

      @Override
      public int index() {
        return it.index();
      }

      @Override
      public int docID() {
        return doc;
      }

      @Override
      public int nextDoc() throws IOException {
        // The first call seeks to the range; the rest walk inside it.
        return doc = clamp(doc < start ? it.advance(start) : it.nextDoc());
      }

      @Override
      public int advance(int target) throws IOException {
        return doc = clamp(it.advance(Math.max(target, start)));
      }

      @Override
      public long cost() {
        return end - start;
      }

      private int clamp(int d) {
        return d >= end ? NO_MORE_DOCS : d;
      }
    };
  }

  @Override
  public FloatVectorValues getFloatVectorValues(String field) throws IOException {
    final FloatVectorValues values = in.getFloatVectorValues(field);
    if (values == null) {
      return null;
    }
    return wrap(values);
  }

  private FloatVectorValues wrap(FloatVectorValues values) {
    return new FloatVectorValues() {
      @Override
      public float[] vectorValue(int ord) throws IOException {
        return values.vectorValue(ord);
      }

      @Override
      public FloatVectorValues copy() throws IOException {
        return wrap(values.copy());
      }

      @Override
      public int dimension() {
        return values.dimension();
      }

      @Override
      public int size() {
        return values.size();
      }

      @Override
      public DocIndexIterator iterator() {
        return rangeIterator(values.iterator());
      }
    };
  }

  @Override
  public ByteVectorValues getByteVectorValues(String field) throws IOException {
    final ByteVectorValues values = in.getByteVectorValues(field);
    if (values == null) {
      return null;
    }
    return wrapBytes(values);
  }

  private ByteVectorValues wrapBytes(ByteVectorValues values) {
    return new ByteVectorValues() {
      @Override
      public byte[] vectorValue(int ord) throws IOException {
        return values.vectorValue(ord);
      }

      @Override
      public ByteVectorValues copy() throws IOException {
        return wrapBytes(values.copy());
      }

      @Override
      public int dimension() {
        return values.dimension();
      }

      @Override
      public int size() {
        return values.size();
      }

      @Override
      public DocIndexIterator iterator() {
        return rangeIterator(values.iterator());
      }
    };
  }

  @Override
  public Float16VectorValues getFloat16VectorValues(String field) throws IOException {
    final Float16VectorValues values = in.getFloat16VectorValues(field);
    if (values == null) {
      return null;
    }
    return wrapFloat16(values);
  }

  private Float16VectorValues wrapFloat16(Float16VectorValues values) {
    return new Float16VectorValues() {
      @Override
      public short[] vectorValue(int ord) throws IOException {
        return values.vectorValue(ord);
      }

      @Override
      public Float16VectorValues copy() throws IOException {
        return wrapFloat16(values.copy());
      }

      @Override
      public int dimension() {
        return values.dimension();
      }

      @Override
      public int size() {
        return values.size();
      }

      @Override
      public DocIndexIterator iterator() {
        return rangeIterator(values.iterator());
      }
    };
  }

  // Search is not part of a merge; it is left to the reader underneath, over all of its documents.

  @Override
  public void search(String field, float[] target, KnnCollector collector, AcceptDocs acceptDocs)
      throws IOException {
    in.search(field, target, collector, acceptDocs);
  }

  @Override
  public void search(String field, byte[] target, KnnCollector collector, AcceptDocs acceptDocs)
      throws IOException {
    in.search(field, target, collector, acceptDocs);
  }

  @Override
  public void search(String field, short[] target, KnnCollector collector, AcceptDocs acceptDocs)
      throws IOException {
    in.search(field, target, collector, acceptDocs);
  }

  @Override
  public KnnVectorsReader getMergeInstance() throws IOException {
    return new DocRangeKnnVectorsReader(in.getMergeInstance(), start, end);
  }

  @Override
  public void finishMerge() throws IOException {
    in.finishMerge();
  }

  @Override
  public void checkIntegrity(MergePolicy.OneMerge merge) throws IOException {
    in.checkIntegrity(merge);
  }

  @Override
  public void close() throws IOException {
    // The wrapped reader is owned by the reader this narrows, not by the narrowing.
  }
}
