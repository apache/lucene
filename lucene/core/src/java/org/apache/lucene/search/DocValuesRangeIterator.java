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
import org.apache.lucene.util.FixedBitSet;
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
            values,
            new SkipBlockRangeIterator(skipper, min, max),
            check,
            2,
            false,
            values,
            min,
            max);
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

  /**
   * @param contiguous true iff every ordinal in [min, max] is set, in which case the set is
   *     equivalent to the contiguous range [min, max] and a cheaper range check can be used in
   *     place of the per-doc bit lookup. Computed at construction time so callers don't have to
   *     trust an undocumented invariant about the bounds of set bits in {@code ords}.
   */
  private record OrdinalSet(long min, long max, LongBitSet ords, boolean contiguous) {

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
    // Count distinct ords via getAndSet so a TermsEnum that yields a duplicate ord doesn't fool
    // the contiguity check below. The first set bit (min) is always new on a fresh bitset.
    long distinctCount = 1;
    while (termsEnum.next() != null) {
      max = termsEnum.ord();
      if (ords.getAndSet(max) == false) {
        distinctCount++;
      }
    }
    // If every ord in [min, max] is set, the set is equivalent to forOrdinalRange and can use the
    // cheaper range check + block-level YES short-circuit.
    return new OrdinalSet(min, max, ords, distinctCount == max - min + 1);
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
    if (ordinalSet.contiguous) {
      return forOrdinalRange(values, skipper, ordinalSet.min, ordinalSet.max);
    }
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
    // Pass contiguous=false: callers of this overload aren't required by the javadoc to keep all
    // set bits within [minOrd, maxOrd], so we can't infer contiguity from cardinality alone.
    return forOrdinalSet(values, skipper, new OrdinalSet(minOrd, maxOrd, ords, false));
  }

  private static DocValuesRangeIterator forOrdinalSet(
      SortedSetDocValues values, DocValuesSkipper skipper, OrdinalSet ordinalSet) {
    if (ordinalSet == null || ordinalSet.disjoint(skipper)) {
      return new EmptyRangeIterator();
    }
    if (ordinalSet.contiguous) {
      return forOrdinalRange(values, skipper, ordinalSet.min, ordinalSet.max);
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
    // Non-null only for single-valued numeric doc values, in which case a whole block can be
    // range-evaluated in one shot in intoBitSet rather than confirming matches() one doc at a time.
    private final NumericDocValues numericValues;
    private final long minValue;
    private final long maxValue;

    private DocValuesBlockRangeIterator(
        DocIdSetIterator disi,
        SkipBlockRangeIterator blockIterator,
        IOBooleanSupplier predicate,
        float matchCost,
        boolean alwaysCheckPredicate) {
      this(disi, blockIterator, predicate, matchCost, alwaysCheckPredicate, null, 0, 0);
    }

    private DocValuesBlockRangeIterator(
        DocIdSetIterator disi,
        SkipBlockRangeIterator blockIterator,
        IOBooleanSupplier predicate,
        float matchCost,
        boolean alwaysCheckPredicate,
        NumericDocValues numericValues,
        long minValue,
        long maxValue) {
      super(blockIterator);
      this.disi = disi;
      this.blockIterator = blockIterator;
      this.predicate = predicate;
      this.matchCost = matchCost;
      this.alwaysCheckPredicate = alwaysCheckPredicate;
      this.numericValues = numericValues;
      this.minValue = minValue;
      this.maxValue = maxValue;
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
    public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
      if (numericValues == null) {
        if (alwaysCheckPredicate) {
          // Arbitrary ordinal set: every block (even YES) must run the per-doc predicate, so there
          // is no block-level shortcut to exploit. Confirm each candidate one doc at a time (the
          // TwoPhaseIterator default).
          super.intoBitSet(upTo, bitSet, offset);
          return;
        }
        // Ordinal range (sorted / sorted-set): there is no columnar range-decode like the numeric
        // path, but the block classification still lets us bulk-set YES runs and bulk-mark present
        // docs in YES_IF_PRESENT runs, confirming the ordinal predicate per doc only in MAYBE runs.
        ordinalRangeIntoBitSet(upTo, bitSet, offset);
        return;
      }
      while (blockIterator.docID() < upTo) {
        int blockStart = blockIterator.docID();
        SkipBlockRangeIterator.Match match = blockIterator.getMatch();
        // For MAYBE blocks docIDRunEnd() is conservative (doc+1), so use the full block boundary to
        // evaluate the whole block at once.
        int blockEnd =
            match == SkipBlockRangeIterator.Match.MAYBE
                ? Math.min(upTo, blockIterator.blockEnd())
                : Math.min(upTo, blockIterator.docIDRunEnd());
        switch (match) {
          case YES -> bitSet.set(blockStart - offset, blockEnd - offset);
          case YES_IF_PRESENT -> {
            // All present values are in range, but the field is sparse: set a bit for every
            // doc that has a value. Delegate to intoBitSet so dense codecs can bulk-set the run
            // rather than probing one doc at a time. Only advance forward; a preceding YES block
            // leaves the iterator behind blockStart, while MAYBE/YES_IF_PRESENT blocks leave it
            // at or past it.
            if (numericValues.docID() < blockStart) {
              numericValues.advance(blockStart);
            }
            numericValues.intoBitSet(blockEnd, bitSet, offset);
          }
          case MAYBE ->
              numericValues.rangeIntoBitSet(
                  blockStart, blockEnd, minValue, maxValue, bitSet, offset);
        }
        blockIterator.advance(blockEnd);
      }
    }

    /**
     * Bulk-evaluates an ordinal range over the block structure. Functionally identical to per-doc
     * {@link #matches()} evaluation, but a whole YES run is set in one shot and a YES_IF_PRESENT
     * run's present docs are marked via {@link DocIdSetIterator#intoBitSet}; only MAYBE runs
     * confirm the ordinal predicate per doc. Like {@code matches()}, {@code disi} is the doc-values
     * iterator and {@code predicate} the range check. Mirrors the numeric block loop in {@link
     * #intoBitSet}: keep the two block walks in sync if the block-boundary handling changes.
     */
    private void ordinalRangeIntoBitSet(int upTo, FixedBitSet bitSet, int offset)
        throws IOException {
      while (blockIterator.docID() < upTo) {
        int blockStart = blockIterator.docID();
        SkipBlockRangeIterator.Match match = blockIterator.getMatch();
        // For MAYBE blocks docIDRunEnd() is conservative (doc+1), so use the full block boundary to
        // evaluate the whole block at once.
        int blockEnd =
            match == SkipBlockRangeIterator.Match.MAYBE
                ? Math.min(upTo, blockIterator.blockEnd())
                : Math.min(upTo, blockIterator.docIDRunEnd());
        switch (match) {
          case YES -> bitSet.set(blockStart - offset, blockEnd - offset);
          case YES_IF_PRESENT -> {
            // Every present value is in range, so mark each doc that has a value. Only advance
            // forward: a preceding YES block leaves disi behind blockStart, MAYBE/YES_IF_PRESENT
            // leave it at or past it.
            if (disi.docID() < blockStart) {
              disi.advance(blockStart);
            }
            disi.intoBitSet(blockEnd, bitSet, offset);
          }
          case MAYBE -> {
            // Visit only docs that have a value (like matches() does) and confirm the ordinal
            // predicate one doc at a time.
            if (disi.docID() < blockStart) {
              disi.advance(blockStart);
            }
            for (int doc = disi.docID(); doc < blockEnd; doc = disi.nextDoc()) {
              if (predicate.get()) {
                bitSet.set(doc - offset);
              }
            }
          }
        }
        blockIterator.advance(blockEnd);
      }
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
