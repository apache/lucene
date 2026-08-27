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
import org.apache.lucene.codecs.DocValuesProducer;

/**
 * A doc values producer whose iterators cover only {@code [start, end)} of the segment.
 *
 * <p>Marking the other documents deleted is not enough to keep a merge from reading them. A merge
 * walks each field's values with {@link DocValuesIterator#nextDoc()} and drops whatever the
 * document map sends to {@code -1}, so the values are decoded first and discarded afterwards; an
 * output of a partitioned merge that wants a twentieth of the segment still pays for all of it, and
 * k outputs pay k times over. Seeking to the range and stopping at its end turns that back into one
 * read of the whole segment shared between the outputs.
 *
 * <p>Only the iteration is restricted. The value space -- the term dictionary behind a sorted field
 * and the ordinals into it -- is left whole, because a merge builds its ordinal map from it and
 * expects the same dictionary a full reader would have shown.
 *
 * <p>The five kinds of doc values differ only in the values they hand back, and not at all in how
 * they are iterated, so each one below is Lucene's plain filter for its kind with the iteration
 * taken over by a shared {@link RangeCursor}.
 */
final class DocRangeDocValuesProducer extends DocValuesProducer {

  private final DocValuesProducer in;
  private final int start;
  private final int end;

  DocRangeDocValuesProducer(DocValuesProducer in, int start, int end) {
    this.in = in;
    this.start = start;
    this.end = end;
  }

  /** Iteration restricted to {@code [start, end)}, over any kind of doc values. */
  private final class RangeCursor {
    private final DocValuesIterator values;
    private int doc = -1;

    RangeCursor(DocValuesIterator values) {
      this.values = values;
    }

    int docID() {
      return doc;
    }

    int nextDoc() throws IOException {
      // The first call seeks to the range; the rest walk inside it. Reaching its end is the end of
      // the iteration, not a step to the next document.
      return doc = clamp(doc < start ? values.advance(start) : values.nextDoc());
    }

    int advance(int target) throws IOException {
      return doc = clamp(values.advance(Math.max(target, start)));
    }

    boolean advanceExact(int target) throws IOException {
      doc = target;
      return target >= start && target < end && values.advanceExact(target);
    }

    private int clamp(int doc) {
      return doc >= end ? DocValuesIterator.NO_MORE_DOCS : doc;
    }
  }

  @Override
  public NumericDocValues getNumeric(FieldInfo field) throws IOException {
    final NumericDocValues values = in.getNumeric(field);
    if (values == null) {
      return null;
    }
    return new FilterNumericDocValues(values) {
      final RangeCursor cursor = new RangeCursor(values);

      @Override
      public int docID() {
        return cursor.docID();
      }

      @Override
      public int nextDoc() throws IOException {
        return cursor.nextDoc();
      }

      @Override
      public int advance(int target) throws IOException {
        return cursor.advance(target);
      }

      @Override
      public boolean advanceExact(int target) throws IOException {
        return cursor.advanceExact(target);
      }
    };
  }

  @Override
  public BinaryDocValues getBinary(FieldInfo field) throws IOException {
    final BinaryDocValues values = in.getBinary(field);
    if (values == null) {
      return null;
    }
    return new FilterBinaryDocValues(values) {
      final RangeCursor cursor = new RangeCursor(values);

      @Override
      public int docID() {
        return cursor.docID();
      }

      @Override
      public int nextDoc() throws IOException {
        return cursor.nextDoc();
      }

      @Override
      public int advance(int target) throws IOException {
        return cursor.advance(target);
      }

      @Override
      public boolean advanceExact(int target) throws IOException {
        return cursor.advanceExact(target);
      }
    };
  }

  @Override
  public SortedDocValues getSorted(FieldInfo field) throws IOException {
    final SortedDocValues values = in.getSorted(field);
    if (values == null) {
      return null;
    }
    return new FilterSortedDocValues(values) {
      final RangeCursor cursor = new RangeCursor(values);

      @Override
      public int docID() {
        return cursor.docID();
      }

      @Override
      public int nextDoc() throws IOException {
        return cursor.nextDoc();
      }

      @Override
      public int advance(int target) throws IOException {
        return cursor.advance(target);
      }

      @Override
      public boolean advanceExact(int target) throws IOException {
        return cursor.advanceExact(target);
      }
    };
  }

  @Override
  public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
    final SortedNumericDocValues values = in.getSortedNumeric(field);
    if (values == null) {
      return null;
    }
    return new FilterSortedNumericDocValues(values) {
      final RangeCursor cursor = new RangeCursor(values);

      @Override
      public int docID() {
        return cursor.docID();
      }

      @Override
      public int nextDoc() throws IOException {
        return cursor.nextDoc();
      }

      @Override
      public int advance(int target) throws IOException {
        return cursor.advance(target);
      }

      @Override
      public boolean advanceExact(int target) throws IOException {
        return cursor.advanceExact(target);
      }
    };
  }

  @Override
  public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
    final SortedSetDocValues values = in.getSortedSet(field);
    if (values == null) {
      return null;
    }
    return new FilterSortedSetDocValues(values) {
      final RangeCursor cursor = new RangeCursor(values);

      @Override
      public int docID() {
        return cursor.docID();
      }

      @Override
      public int nextDoc() throws IOException {
        return cursor.nextDoc();
      }

      @Override
      public int advance(int target) throws IOException {
        return cursor.advance(target);
      }

      @Override
      public boolean advanceExact(int target) throws IOException {
        return cursor.advanceExact(target);
      }
    };
  }

  @Override
  public DocValuesSkipper getSkipper(FieldInfo field) {
    // Left alone: a skipper only ever narrows what a caller has to look at, so restricting it would
    // save nothing that the iterators above have not already saved.
    return in.getSkipper(field);
  }

  @Override
  public void checkIntegrity(MergePolicy.OneMerge merge) throws IOException {
    in.checkIntegrity(merge);
  }

  @Override
  public void close() throws IOException {
    in.close();
  }
}
