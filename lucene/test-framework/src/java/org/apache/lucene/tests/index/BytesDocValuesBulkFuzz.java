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
package org.apache.lucene.tests.index;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.apache.lucene.tests.util.LuceneTestCase.assertEquals;
import static org.apache.lucene.tests.util.LuceneTestCase.random;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.function.Supplier;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.tests.util.RandomLongValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

/**
 * Fuzzes the bulk methods of {@link BinaryDocValues}, {@link SortedDocValues} and {@link
 * SortedSetDocValues}.
 */
final class BytesDocValuesBulkFuzz extends DocValuesBulkFuzz {

  /** The shapes of the values, which are encoded differently. */
  enum Shape {
    /** Eight bytes that sort like longs from a random distribution. */
    LONGS,
    SHORT_RANDOM,
    LONG_RANDOM,
    SHARED_PREFIX,
    FEW_DISTINCT,
    FIXED_LENGTH
  }

  private final List<Supplier<BytesRef>> regions = new ArrayList<>();
  // the values of each doc by id, which are empty if the doc has no value. They are in ascending
  // order and unique for sorted sets, and a single value for the other kinds
  private final BytesRef[][] values;

  BytesDocValuesBulkFuzz(Kind kind, Shape shape, ReaderWrapper readerWrapper) {
    super(kind, readerWrapper);
    Random random = random();

    int maxLength = kind == Kind.BINARY ? 1024 : 300;
    regions.add(generator(shape, maxLength));
    if (random.nextInt(4) == 0) {
      Shape[] shapes = Shape.values();
      for (int i = random.nextInt(1, 3); i > 0; i--) {
        regions.add(generator(shapes[random.nextInt(shapes.length)], maxLength));
      }
    }

    double missingProbability = new double[] {0, 0, 0.3, 0.9}[random.nextInt(4)];
    int maxValuesPerDoc =
        kind == Kind.SORTED_SET && random.nextBoolean() ? random.nextInt(2, 6) : 1;
    values = new BytesRef[numDocs][];
    for (int id = 0; id < numDocs; id++) {
      Supplier<BytesRef> region = regions.get((int) ((long) id * regions.size() / numDocs));
      int numValues =
          random.nextDouble() < missingProbability ? 0 : random.nextInt(1, maxValuesPerDoc + 1);
      TreeSet<BytesRef> docValues = new TreeSet<>();
      for (int i = 0; i < numValues; i++) {
        docValues.add(region.get());
      }
      values[id] = docValues.toArray(new BytesRef[0]);
    }
  }

  private static Supplier<BytesRef> generator(Shape shape, int maxLength) {
    Random random = random();
    return switch (shape) {
      case LONGS -> {
        RandomLongValues longs = RandomLongValues.random(random);
        yield () -> {
          byte[] encoded = new byte[Long.BYTES];
          LongPoint.encodeDimension(longs.nextValue(), encoded, 0);
          return new BytesRef(encoded);
        };
      }
      case SHORT_RANDOM -> () -> randomBytes(random.nextInt(17));
      case LONG_RANDOM -> () -> randomBytes(random.nextInt(maxLength + 1));
      case SHARED_PREFIX -> {
        byte[] prefix = randomBytes(random.nextInt(1, 41)).bytes;
        yield () -> {
          byte[] suffix = randomBytes(random.nextInt(9)).bytes;
          byte[] value = Arrays.copyOf(prefix, prefix.length + suffix.length);
          System.arraycopy(suffix, 0, value, prefix.length, suffix.length);
          return new BytesRef(value);
        };
      }
      case FEW_DISTINCT -> {
        BytesRef[] pool = new BytesRef[random.nextInt(1, 301)];
        for (int i = 0; i < pool.length; i++) {
          pool[i] = randomBytes(random.nextInt(20));
        }
        yield () -> pool[random.nextInt(pool.length)];
      }
      case FIXED_LENGTH -> {
        int length = random.nextInt(1, 31);
        yield () -> randomBytes(length);
      }
    };
  }

  // half of the values only use a few different bytes, so that there are shared prefixes
  private static BytesRef randomBytes(int length) {
    Random random = random();
    byte[] bytes = new byte[length];
    if (random.nextBoolean()) {
      random.nextBytes(bytes);
    } else {
      for (int i = 0; i < length; i++) {
        bytes[i] = (byte) random.nextInt(4);
      }
    }
    return new BytesRef(bytes);
  }

  @Override
  boolean hasValue(int id) {
    return values[id].length > 0;
  }

  @Override
  void addFields(Document doc, int id) {
    for (BytesRef value : values[id]) {
      switch (kind) {
        case BINARY -> doc.add(new BinaryDocValuesField(FIELD, value));
        case SORTED -> doc.add(sortedField(value));
        case SORTED_SET -> {
          doc.add(sortedSetField(value));
          if (random().nextBoolean()) {
            // the same value twice is only stored once
            doc.add(sortedSetField(value));
          }
        }
        default -> throw new AssertionError(kind);
      }
    }
  }

  private SortedDocValuesField sortedField(BytesRef value) {
    return skipIndex
        ? SortedDocValuesField.indexedField(FIELD, value)
        : new SortedDocValuesField(FIELD, value);
  }

  private SortedSetDocValuesField sortedSetField(BytesRef value) {
    return skipIndex
        ? SortedSetDocValuesField.indexedField(FIELD, value)
        : new SortedSetDocValuesField(FIELD, value);
  }

  @Override
  void applyUpdates(RandomIndexWriter writer) throws IOException {
    Random random = random();
    if (kind != Kind.BINARY || random.nextBoolean()) {
      return;
    }
    int numUpdates = random.nextInt(numDocs / 4 + 1);
    for (int i = 0; i < numUpdates; i++) {
      int id = random.nextInt(numDocs);
      if (values[id].length == 1) {
        BytesRef value = regions.get(0).get();
        values[id] = new BytesRef[] {value};
        writer.updateBinaryDocValue(idTerm(id), FIELD, value);
      }
    }
  }

  @Override
  SegmentCheck newSegmentCheck(LeafReader reader) throws IOException {
    return new BytesSegmentCheck(reader);
  }

  private final class BytesSegmentCheck extends SegmentCheck {
    // the different values of the segment in ascending order, which are the terms of the
    // dictionary of sorted doc values
    private final BytesRef[] distinct;

    BytesSegmentCheck(LeafReader reader) throws IOException {
      super(reader);
      TreeSet<BytesRef> terms = new TreeSet<>();
      for (int doc : docsWithValues) {
        terms.addAll(Arrays.asList(values[ids[doc]]));
      }
      distinct = terms.toArray(new BytesRef[0]);
    }

    private long ordOf(BytesRef value) {
      int ord = Arrays.binarySearch(distinct, value);
      assertEquals("value is in the dictionary", true, ord >= 0);
      return ord;
    }

    // the single values of sorted set doc values are sorted doc values, which may be implemented
    // differently
    private SortedDocValues newSorted() throws IOException {
      return switch (kind) {
        case SORTED -> reader.getSortedDocValues(FIELD);
        case SORTED_SET -> {
          SortedSetDocValues sortedSet = reader.getSortedSetDocValues(FIELD);
          yield sortedSet == null ? null : DocValues.unwrapSingleton(sortedSet);
        }
        default -> throw new AssertionError(kind);
      };
    }

    @Override
    DocIdSetIterator newIterator() throws IOException {
      return switch (kind) {
        case BINARY -> reader.getBinaryDocValues(FIELD);
        case SORTED -> reader.getSortedDocValues(FIELD);
        case SORTED_SET -> {
          SortedDocValues singleton = random().nextBoolean() ? newSorted() : null;
          yield singleton != null ? singleton : reader.getSortedSetDocValues(FIELD);
        }
        default -> throw new AssertionError(kind);
      };
    }

    @Override
    void checkExtras() throws IOException {
      switch (kind) {
        case BINARY -> checkBulkValues();
        case SORTED -> checkOrdinalRangeIntoBitSet();
        case SORTED_SET -> checkOrdinalRangeIntoBitSet();
        default -> throw new AssertionError(kind);
      }
    }

    // reads the values of some docs at once, with offsets in the arrays of docs and values
    private void checkBulkValues() throws IOException {
      for (int round = 0; round < 3; round++) {
        BinaryDocValues binary = reader.getBinaryDocValues(FIELD);
        if (binary == null) {
          return;
        }
        Batch batch = randomBatch();
        BytesRef[] actual = new BytesRef[batch.valuesOffset() + batch.size()];
        binary.binaryValues(
            batch.size(), batch.docs(), batch.docsOffset(), actual, batch.valuesOffset());
        for (int i = 0; i < batch.size(); i++) {
          int doc = batch.docs()[batch.docsOffset() + i];
          if (isLive(doc)) {
            BytesRef expected = hasValue(ids[doc]) ? values[ids[doc]][0] : null;
            assertEquals("bulk value of doc " + doc, expected, actual[batch.valuesOffset() + i]);
          }
        }
      }
    }

    // fills bit sets with the docs that have an ordinal in a range, from an iterator that is not
    // positioned, or that is positioned before the range
    private void checkOrdinalRangeIntoBitSet() throws IOException {
      Random random = random();
      for (int round = 0; round < 5; round++) {
        SortedDocValues sorted = newSorted();
        if (sorted == null) {
          return;
        }
        int position = -1;
        if (random.nextBoolean()) {
          position = sorted.advance(random.nextInt(maxDoc));
          if (position == NO_MORE_DOCS) {
            continue;
          }
        }

        DocRange range = randomDocRange(Math.max(position, 0));
        long ordA = random.nextInt(distinct.length + 1);
        long ordB = random.nextInt(distinct.length + 1);
        long minOrd = Math.min(ordA, ordB);
        long maxOrd = Math.max(ordA, ordB);
        FixedBitSet preset = newPresetBitSet(range);
        FixedBitSet actual = preset.clone();
        sorted.ordinalRangeIntoBitSet(
            range.fromDoc(), range.toDoc(), minOrd, maxOrd, actual, range.offset());
        assertMatches(
            "ordinalRangeIntoBitSet [" + minOrd + ", " + maxOrd + "]",
            range,
            preset,
            actual,
            doc -> {
              long ord = ordOf(values[ids[doc]][0]);
              return minOrd <= ord && ord <= maxOrd;
            });
      }
    }
  }
}
