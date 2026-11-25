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
import java.util.Arrays;
import java.util.Random;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.tests.util.RandomLongValues;
import org.apache.lucene.tests.util.RandomLongValues.Bounds;
import org.apache.lucene.tests.util.RandomLongValues.Distribution;
import org.apache.lucene.util.FixedBitSet;

/** Fuzzes the bulk methods of {@link NumericDocValues} and {@link SortedNumericDocValues}. */
final class NumericDocValuesBulkFuzz extends DocValuesBulkFuzz {

  private final RandomLongValues[] regions;
  // the values of each doc by id, in ascending order, which are empty if the doc has no value
  private final long[][] values;

  NumericDocValuesBulkFuzz(Kind kind, Distribution distribution, ReaderWrapper readerWrapper) {
    super(kind, readerWrapper);
    Random random = random();

    regions = new RandomLongValues[random.nextInt(4) == 0 ? random.nextInt(2, 4) : 1];
    regions[0] = RandomLongValues.of(random, distribution);
    for (int i = 1; i < regions.length; i++) {
      regions[i] = RandomLongValues.random(random);
    }

    double missingProbability = new double[] {0, 0, 0.3, 0.9}[random.nextInt(4)];
    int maxValuesPerDoc =
        kind == Kind.SORTED_NUMERIC && random.nextBoolean() ? random.nextInt(2, 8) : 1;
    values = new long[numDocs][];
    for (int id = 0; id < numDocs; id++) {
      RandomLongValues region = regions[(int) ((long) id * regions.length / numDocs)];
      int numValues =
          random.nextDouble() < missingProbability ? 0 : random.nextInt(1, maxValuesPerDoc + 1);
      values[id] = new long[numValues];
      for (int i = 0; i < numValues; i++) {
        values[id][i] = region.nextValue();
      }
      Arrays.sort(values[id]);
    }
  }

  @Override
  boolean hasValue(int id) {
    return values[id].length > 0;
  }

  @Override
  void addFields(Document doc, int id) {
    for (long value : values[id]) {
      doc.add(
          switch (kind) {
            case NUMERIC ->
                skipIndex
                    ? NumericDocValuesField.indexedField(FIELD, value)
                    : new NumericDocValuesField(FIELD, value);
            case SORTED_NUMERIC ->
                skipIndex
                    ? SortedNumericDocValuesField.indexedField(FIELD, value)
                    : new SortedNumericDocValuesField(FIELD, value);
            default -> throw new AssertionError(kind);
          });
    }
  }

  // updates are only checked against the model, so they are not combined with a skip index
  @Override
  void applyUpdates(RandomIndexWriter writer) throws IOException {
    Random random = random();
    if (kind != Kind.NUMERIC || skipIndex || random.nextBoolean()) {
      return;
    }
    int numUpdates = random.nextInt(numDocs / 4 + 1);
    for (int i = 0; i < numUpdates; i++) {
      int id = random.nextInt(numDocs);
      if (values[id].length == 1) {
        values[id][0] = regions[0].nextValue();
        writer.updateNumericDocValue(idTerm(id), FIELD, values[id][0]);
      }
    }
  }

  @Override
  SegmentCheck newSegmentCheck(LeafReader reader) throws IOException {
    return new NumericSegmentCheck(reader);
  }

  private static boolean anyValueIn(long[] docValues, Bounds bounds) {
    for (long value : docValues) {
      if (bounds.contains(value)) {
        return true;
      }
    }
    return false;
  }

  private final class NumericSegmentCheck extends SegmentCheck {

    NumericSegmentCheck(LeafReader reader) throws IOException {
      super(reader);
    }

    // the single values of sorted numeric doc values are numeric doc values, which may be
    // implemented differently
    private NumericDocValues newNumeric() throws IOException {
      return switch (kind) {
        case NUMERIC -> reader.getNumericDocValues(FIELD);
        case SORTED_NUMERIC -> {
          SortedNumericDocValues sortedNumeric = reader.getSortedNumericDocValues(FIELD);
          yield sortedNumeric == null ? null : DocValues.unwrapSingleton(sortedNumeric);
        }
        default -> throw new AssertionError(kind);
      };
    }

    @Override
    DocIdSetIterator newIterator() throws IOException {
      if (kind == Kind.SORTED_NUMERIC && random().nextBoolean()) {
        return reader.getSortedNumericDocValues(FIELD);
      }
      NumericDocValues numeric = newNumeric();
      return numeric != null
          ? numeric
          : kind == Kind.NUMERIC ? null : reader.getSortedNumericDocValues(FIELD);
    }

    @Override
    void checkExtras() throws IOException {
      checkLongValues();
      checkRangeIntoBitSet();
    }

    // reads the values of some docs at once, with offsets in the arrays of docs and values
    private void checkLongValues() throws IOException {
      Random random = random();
      for (int round = 0; round < 3; round++) {
        NumericDocValues numeric = newNumeric();
        if (numeric == null) {
          return;
        }
        Batch batch = randomBatch();
        long defaultValue = random.nextLong();
        long[] actual = new long[batch.valuesOffset() + batch.size()];
        numeric.longValues(
            batch.size(),
            batch.docs(),
            batch.docsOffset(),
            actual,
            batch.valuesOffset(),
            defaultValue);
        for (int i = 0; i < batch.size(); i++) {
          int doc = batch.docs()[batch.docsOffset() + i];
          if (isLive(doc)) {
            long expected = hasValue(ids[doc]) ? values[ids[doc]][0] : defaultValue;
            assertEquals("bulk value of doc " + doc, expected, actual[batch.valuesOffset() + i]);
          }
        }
      }
    }

    // fills bit sets with the docs that have a value in a range, from an iterator that is not
    // positioned, or that is positioned before the range
    private void checkRangeIntoBitSet() throws IOException {
      Random random = random();
      for (int round = 0; round < 5; round++) {
        boolean sortedNumeric = kind == Kind.SORTED_NUMERIC && random.nextBoolean();
        SortedNumericDocValues sortedNumericValues =
            sortedNumeric ? reader.getSortedNumericDocValues(FIELD) : null;
        NumericDocValues numericValues = sortedNumeric ? null : newNumeric();
        DocIdSetIterator iterator = sortedNumeric ? sortedNumericValues : numericValues;
        if (iterator == null) {
          continue;
        }
        int position = -1;
        if (random.nextBoolean()) {
          position = iterator.advance(random.nextInt(maxDoc));
          if (position == NO_MORE_DOCS) {
            continue;
          }
        }

        DocRange range = randomDocRange(Math.max(position, 0));
        Bounds bounds = regions[random.nextInt(regions.length)].nextBound();
        FixedBitSet preset = newPresetBitSet(range);
        FixedBitSet actual = preset.clone();
        if (sortedNumeric) {
          sortedNumericValues.rangeIntoBitSet(
              range.fromDoc(), range.toDoc(), bounds.min(), bounds.max(), actual, range.offset());
        } else {
          numericValues.rangeIntoBitSet(
              range.fromDoc(), range.toDoc(), bounds.min(), bounds.max(), actual, range.offset());
        }
        assertMatches(
            "rangeIntoBitSet " + bounds,
            range,
            preset,
            actual,
            doc -> anyValueIn(values[ids[doc]], bounds));
      }
    }
  }
}
