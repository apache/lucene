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

import java.io.IOException;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.IOBooleanSupplier;
import org.apache.lucene.util.LongBitSet;

/**
 * A {@link TwoPhaseIterator} over doc values that skips documents with values that lie outside a
 * specific range, using a {@link SkipBlockRangeIterator} as an approximation
 *
 * @lucene.experimental
 */
public abstract sealed class DocValuesRangeIterator extends TwoPhaseIterator {

  /**
   * Creates a DocValuesRangeIterator over a NumericDocValues instance
   *
   * @param values the doc values
   * @param skipper an optional skipper to exclude non-matching blocks
   * @param min skip documents with values lower than this
   * @param max skip documents with values higher than this
   */
  public static DocValuesRangeIterator forRange(
      NumericDocValues values, DocValuesSkipper skipper, long min, long max) {
    IOBooleanSupplier check =
        () -> {
          long v = values.longValue();
          return v >= min && v <= max;
        };
    return skipper == null
        ? new DocValuesValueRangeIterator(values, check, 2)
        : new DocValuesBlockRangeIterator(
            values, new SkipBlockRangeIterator(skipper, min, max), check, 2, false);
  }

  /**
   * Creates a DocValuesRangeIterator over a SortedNumericDocValues instance
   *
   * @param values the doc values
   * @param skipper an optional skipper to exclude non-matching blocks
   * @param min skip documents with all values lower than this
   * @param max skip documents with all values higher than this
   */
  public static DocValuesRangeIterator forRange(
      SortedNumericDocValues values, DocValuesSkipper skipper, long min, long max) {
    IOBooleanSupplier check =
        () -> {
          for (int i = 0; i < values.docValueCount(); i++) {
            long v = values.nextValue();
            if (v >= min) {
              return v <= max;
            }
          }
          return false;
        };
    return skipper == null
        ? new DocValuesValueRangeIterator(values, check, 5)
        : new DocValuesBlockRangeIterator(
            values, new SkipBlockRangeIterator(skipper, min, max), check, 2, false);
  }

  /**
   * Creates a DocValuesRangeIterator over a SortedDocValues instance
   *
   * @param values the doc values
   * @param skipper an optional skipper to exclude non-matching blocks
   * @param min skip documents with ordinal values lower than this
   * @param max skip documents with ordinal values higher than this
   */
  public static DocValuesRangeIterator forOrdinalRange(
      SortedDocValues values, DocValuesSkipper skipper, long min, long max) {
    IOBooleanSupplier check =
        () -> {
          long ord = values.ordValue();
          return ord >= min && ord <= max;
        };
    return skipper == null
        ? new DocValuesValueRangeIterator(values, check, 2)
        : new DocValuesBlockRangeIterator(
            values, new SkipBlockRangeIterator(skipper, min, max), check, 2, false);
  }

  /**
   * Creates a DocValuesRangeIterator over a SortedSetDocValues instance
   *
   * @param values the doc values
   * @param skipper an optional skipper to exclude non-matching blocks
   * @param min skip documents with all ordinal values lower than this
   * @param max skip documents with all ordinal values higher than this
   */
  public static DocValuesRangeIterator forOrdinalRange(
      SortedSetDocValues values, DocValuesSkipper skipper, long min, long max) {
    IOBooleanSupplier check =
        () -> {
          for (int i = 0; i < values.docValueCount(); i++) {
            long v = values.nextOrd();
            if (v >= min) {
              return v <= max;
            }
          }
          return false;
        };
    return skipper == null
        ? new DocValuesValueRangeIterator(values, check, 5)
        : new DocValuesBlockRangeIterator(
            values, new SkipBlockRangeIterator(skipper, min, max), check, 5, false);
  }

  private record OrdinalSet(long min, long max, LongBitSet ords) {

    boolean disjoint(DocValuesSkipper skipper) {
      if (skipper == null) {
        return false;
      }
      return min > skipper.maxValue() || max < skipper.minValue();
    }
  }

  private static OrdinalSet buildOrdinalSet(TermsEnum termsEnum, long ordCount) throws IOException {
    if (termsEnum.next() == null) {
      return null;
    }
    // TODO can we be more memory efficient here? eg LongHashSet
    LongBitSet ords = new LongBitSet(ordCount);
    long min = termsEnum.ord();
    ords.set(min);
    long max = min;
    while (termsEnum.next() != null) {
      max = termsEnum.ord();
      ords.set(max);
    }
    return new OrdinalSet(min, max, ords);
  }

  /**
   * Creates a DocValuesRangeIterator over a SortedDocValues instance
   *
   * @param values the doc values
   * @param skipper an optional skipper to exclude non-matching blocks
   * @param terms a TermsEnum containing the ordinal values to match
   */
  public static DocValuesRangeIterator forOrdinalSet(
      SortedDocValues values, DocValuesSkipper skipper, TermsEnum terms) throws IOException {
    OrdinalSet ordinalSet = buildOrdinalSet(terms, values.getValueCount());
    if (ordinalSet == null || ordinalSet.disjoint(skipper)) {
      return new EmptyRangeIterator();
    }
    // TODO add check to OrdinalSet to detect if it can be converted to a range instead
    IOBooleanSupplier check = () -> ordinalSet.ords.get(values.ordValue());
    return skipper == null
        ? new DocValuesValueRangeIterator(values, check, 2)
        : new DocValuesBlockRangeIterator(
            values,
            new SkipBlockRangeIterator(skipper, ordinalSet.min, ordinalSet.max),
            check,
            2,
            true);
  }

  /**
   * Creates a DocValuesRangeIterator over a SortedSetDocValues instance
   *
   * @param values the doc values
   * @param skipper an optional skipper to exclude non-matching blocks
   * @param terms a TermsEnum containing the ordinal values to match
   */
  public static DocValuesRangeIterator forOrdinalSet(
      SortedSetDocValues values, DocValuesSkipper skipper, TermsEnum terms) throws IOException {
    OrdinalSet ordinalSet = buildOrdinalSet(terms, values.getValueCount());
    return forOrdinalSet(values, skipper, ordinalSet);
  }

  /**
   * Creates a DocValuesRangeIterator over a SortedSetDocValues instance
   *
   * @param values the doc values
   * @param skipper an optional skipper to exclude non-matching blocks
   * @param minOrd skip documents with all ordinal values lower than this
   * @param maxOrd skip documents with all ordinal values higher than this
   * @param ords skip documents with all values not in this set
   */
  public static DocValuesRangeIterator forOrdinalSet(
      SortedSetDocValues values,
      DocValuesSkipper skipper,
      long minOrd,
      long maxOrd,
      LongBitSet ords) {
    return forOrdinalSet(values, skipper, new OrdinalSet(minOrd, maxOrd, ords));
  }

  private static DocValuesRangeIterator forOrdinalSet(
      SortedSetDocValues values, DocValuesSkipper skipper, OrdinalSet ordinalSet) {
    if (ordinalSet == null || ordinalSet.disjoint(skipper)) {
      return new EmptyRangeIterator();
    }
    IOBooleanSupplier check =
        () -> {
          for (int i = 0; i < values.docValueCount(); i++) {
            long v = values.nextOrd();
            if (v > ordinalSet.max) {
              return false;
            }
            if (v >= ordinalSet.min && ordinalSet.ords.get(v)) {
              return true;
            }
          }
          return false;
        };
    return skipper == null
        ? new DocValuesValueRangeIterator(values, check, 5)
        : new DocValuesBlockRangeIterator(
            values,
            new SkipBlockRangeIterator(skipper, ordinalSet.min, ordinalSet.max),
            check,
            5,
            true);
  }

  private static final class DocValuesBlockRangeIterator extends DocValuesRangeIterator {

    private final SkipBlockRangeIterator blockIterator;
    private final DocIdSetIterator disi;
    private final IOBooleanSupplier predicate;
    private final float matchCost;
    private final boolean alwaysCheckPredicate;

    private DocValuesBlockRangeIterator(
        DocIdSetIterator disi,
        SkipBlockRangeIterator blockIterator,
        IOBooleanSupplier predicate,
        float matchCost,
        boolean alwaysCheckPredicate) {
      super(blockIterator);
      this.disi = disi;
      this.blockIterator = blockIterator;
      this.predicate = predicate;
      this.matchCost = matchCost;
      this.alwaysCheckPredicate = alwaysCheckPredicate;
    }

    private boolean advanceDisi(int target) throws IOException {
      if (disi.docID() >= target) {
        return disi.docID() == target;
      }
      return disi.advance(target) == target;
    }

    @Override
    public boolean matches() throws IOException {
      if (alwaysCheckPredicate) {
        return advanceDisi(blockIterator.docID()) && predicate.get();
      }
      return switch (blockIterator.getMatch()) {
        case YES -> true;
        case YES_IF_PRESENT -> advanceDisi(blockIterator.docID());
        case MAYBE -> advanceDisi(blockIterator.docID()) && predicate.get();
      };
    }

    @Override
    public int docIDRunEnd() throws IOException {
      if (alwaysCheckPredicate) {
        return blockIterator.docID() + 1;
      }
      return blockIterator.docIDRunEnd();
    }

    @Override
    public float matchCost() {
      return matchCost;
    }
  }

  private static final class DocValuesValueRangeIterator extends DocValuesRangeIterator {

    private final IOBooleanSupplier predicate;
    private final float matchCost;

    private DocValuesValueRangeIterator(
        DocIdSetIterator disi, IOBooleanSupplier predicate, float matchCost) {
      super(disi);
      this.predicate = predicate;
      this.matchCost = matchCost;
    }

    @Override
    public boolean matches() throws IOException {
      return predicate.get();
    }

    @Override
    public float matchCost() {
      return matchCost;
    }
  }

  private static final class EmptyRangeIterator extends DocValuesRangeIterator {

    private EmptyRangeIterator() {
      super(DocIdSetIterator.empty());
    }

    @Override
    public boolean matches() throws IOException {
      return false;
    }

    @Override
    public float matchCost() {
      return 0;
    }
  }

  private DocValuesRangeIterator(DocIdSetIterator approximation) {
    super(approximation);
  }
}
