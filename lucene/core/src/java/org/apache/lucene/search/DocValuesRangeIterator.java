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
        : new BulkNumericRangeIterator(
            values, new SkipBlockRangeIterator(skipper, min, max), check, 2, min, max);
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
        : new BulkSortedNumericRangeIterator(
            values, new SkipBlockRangeIterator(skipper, min, max), check, 2, min, max);
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
        : new BulkOrdinalRangeIterator(
            values, new SkipBlockRangeIterator(skipper, min, max), check, 2);
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
        : new BulkOrdinalRangeIterator(
            values, new SkipBlockRangeIterator(skipper, min, max), check, 5);
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
            values, new SkipBlockRangeIterator(skipper, ordinalSet.min, ordinalSet.max), check, 2);
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
            values, new SkipBlockRangeIterator(skipper, ordinalSet.min, ordinalSet.max), check, 5);
  }

  /**
   * Skip-indexed range iterator that confirms every candidate one doc at a time. Used for arbitrary
   * ordinal sets, where even a YES block (all docs have a value within the [min, max] ord bounds)
   * may contain docs whose ordinals fall in the gaps of the set, so there is no block-level
   * shortcut and {@code intoBitSet} falls back to the per-doc {@link TwoPhaseIterator} default.
   *
   * <p>The block-aware bulk variants extend this class and override {@link #matches()}, {@link
   * #docIDRunEnd()} and {@link #intoBitSet} to exploit the block classification.
   */
  private static sealed class DocValuesBlockRangeIterator extends DocValuesRangeIterator {

    final SkipBlockRangeIterator blockIterator;
    final DocIdSetIterator disi;
    final IOBooleanSupplier predicate;
    private final float matchCost;

    private DocValuesBlockRangeIterator(
        DocIdSetIterator disi,
        SkipBlockRangeIterator blockIterator,
        IOBooleanSupplier predicate,
        float matchCost) {
      super(blockIterator);
      this.disi = disi;
      this.blockIterator = blockIterator;
      this.predicate = predicate;
      this.matchCost = matchCost;
    }

    final boolean advanceDisi(int target) throws IOException {
      if (disi.docID() >= target) {
        return disi.docID() == target;
      }
      return disi.advance(target) == target;
    }

    @Override
    public boolean matches() throws IOException {
      return advanceDisi(blockIterator.docID()) && predicate.get();
    }

    @Override
    public int docIDRunEnd() throws IOException {
      return blockIterator.docID() + 1;
    }

    @Override
    public final float matchCost() {
      return matchCost;
    }
  }

  /**
   * Base class for the block-aware variants: a whole YES run is set in one shot, a YES_IF_PRESENT
   * run's present docs are marked via {@link DocIdSetIterator#intoBitSet}, and only MAYBE runs need
   * per-block confirmation. Subclasses supply the MAYBE handling in {@link #intoMaybeBlock}, which
   * is the only piece that depends on the underlying doc-values type.
   */
  private abstract static sealed class BulkBlockRangeIterator extends DocValuesBlockRangeIterator {

    private BulkBlockRangeIterator(
        DocIdSetIterator disi,
        SkipBlockRangeIterator blockIterator,
        IOBooleanSupplier predicate,
        float matchCost) {
      super(disi, blockIterator, predicate, matchCost);
    }

    @Override
    public final boolean matches() throws IOException {
      return switch (blockIterator.getMatch()) {
        case YES -> true;
        case YES_IF_PRESENT -> advanceDisi(blockIterator.docID());
        case MAYBE -> advanceDisi(blockIterator.docID()) && predicate.get();
      };
    }

    @Override
    public final int docIDRunEnd() throws IOException {
      return blockIterator.docIDRunEnd();
    }

    @Override
    public final void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
      while (blockIterator.docID() < upTo) {
        int blockStart = blockIterator.docID();
        SkipBlockRangeIterator.Match match = blockIterator.getMatch();
        int blockEnd = blockEnd(upTo, match);
        switch (match) {
          case YES -> bitSet.set(blockStart - offset, blockEnd - offset);
          case YES_IF_PRESENT -> {
            // All present values are in range, so mark every doc that has a value. Delegate to
            // intoBitSet so dense codecs can bulk-set the run rather than probing one doc at a
            // time. Only advance forward; a preceding YES block leaves disi behind blockStart,
            // while MAYBE/YES_IF_PRESENT blocks leave it at or past it.
            if (disi.docID() < blockStart) {
              disi.advance(blockStart);
            }
            disi.intoBitSet(blockEnd, bitSet, offset);
          }
          case MAYBE -> intoMaybeBlock(blockStart, blockEnd, bitSet, offset);
        }
        blockIterator.advance(blockEnd);
      }
    }

    /** Confirms the docs of a single MAYBE block in {@code [blockStart, blockEnd)}. */
    abstract void intoMaybeBlock(int blockStart, int blockEnd, FixedBitSet bitSet, int offset)
        throws IOException;

    // For MAYBE blocks docIDRunEnd() is conservative (doc+1), so use the full block boundary to
    // evaluate/classify the whole block at once.
    private int blockEnd(int upTo, SkipBlockRangeIterator.Match match) throws IOException {
      return match == SkipBlockRangeIterator.Match.MAYBE
          ? Math.min(upTo, blockIterator.blockEnd())
          : Math.min(upTo, blockIterator.docIDRunEnd());
    }

    @Override
    public final void applyMask(int upTo, FixedBitSet bitSet, int offset) throws IOException {
      int cursor = offset;
      FixedBitSet scratch = null;
      while (blockIterator.docID() < upTo) {
        int blockStart = blockIterator.docID();
        if (cursor < blockStart) {
          // [cursor, blockStart) doesn't intersect the query range at all -- clear all bits
          // in this range
          bitSet.clear(cursor - offset, blockStart - offset);
        }
        SkipBlockRangeIterator.Match match = blockIterator.getMatch();
        int blockEnd = blockEnd(upTo, match);

        if (match != SkipBlockRangeIterator.Match.YES
            && bitSet.nextSetBit(blockStart - offset, blockEnd - offset)
                != DocIdSetIterator.NO_MORE_DOCS) {
          int blockLength = blockEnd - blockStart;
          if (scratch == null || scratch.length() < blockLength) {
            scratch = new FixedBitSet(blockLength);
          }
          if (match == SkipBlockRangeIterator.Match.YES_IF_PRESENT) {
            if (disi.docID() < blockStart) {
              disi.advance(blockStart);
            }
            disi.intoBitSet(blockEnd, scratch, blockStart);
          } else {
            intoMaybeBlock(blockStart, blockEnd, scratch, blockStart);
          }
          FixedBitSet.andRange(scratch, 0, bitSet, blockStart - offset, blockLength);
          scratch.clear(0, blockLength);
        }
        cursor = blockEnd;
        blockIterator.advance(blockEnd);
      }
      if (cursor < upTo) {
        // The blockIterator has moved past upTo so any remaining bits above the cursor
        // do not match
        bitSet.clear(cursor - offset, upTo - offset);
      }
    }
  }

  /** Bulk range iterator over single-valued numeric doc values. */
  private static final class BulkNumericRangeIterator extends BulkBlockRangeIterator {

    private final NumericDocValues numericValues;
    private final long minValue;
    private final long maxValue;

    private BulkNumericRangeIterator(
        NumericDocValues values,
        SkipBlockRangeIterator blockIterator,
        IOBooleanSupplier predicate,
        float matchCost,
        long minValue,
        long maxValue) {
      super(values, blockIterator, predicate, matchCost);
      this.numericValues = values;
      this.minValue = minValue;
      this.maxValue = maxValue;
    }

    @Override
    void intoMaybeBlock(int blockStart, int blockEnd, FixedBitSet bitSet, int offset)
        throws IOException {
      // numericValues is the same instance as disi, so a preceding matches() call may have
      // moved it beyond blockStart. Adjust the starting point to keep rangeIntoBitSet's
      // advanceExact calls forward-only.
      int from = Math.max(blockStart, numericValues.docID());
      numericValues.rangeIntoBitSet(from, blockEnd, minValue, maxValue, bitSet, offset);
    }
  }

  /** Bulk range iterator over multi-valued sorted-numeric doc values. */
  private static final class BulkSortedNumericRangeIterator extends BulkBlockRangeIterator {

    private final SortedNumericDocValues sortedNumericValues;
    private final long minValue;
    private final long maxValue;

    private BulkSortedNumericRangeIterator(
        SortedNumericDocValues values,
        SkipBlockRangeIterator blockIterator,
        IOBooleanSupplier predicate,
        float matchCost,
        long minValue,
        long maxValue) {
      super(values, blockIterator, predicate, matchCost);
      this.sortedNumericValues = values;
      this.minValue = minValue;
      this.maxValue = maxValue;
    }

    @Override
    void intoMaybeBlock(int blockStart, int blockEnd, FixedBitSet bitSet, int offset)
        throws IOException {
      // sortedNumericValues is the same instance as disi, so a preceding matches() call may have
      // moved it beyond blockStart. Adjust the starting point to keep rangeIntoBitSet's
      // advanceExact calls forward-only.
      int from = Math.max(blockStart, sortedNumericValues.docID());
      sortedNumericValues.rangeIntoBitSet(from, blockEnd, minValue, maxValue, bitSet, offset);
    }
  }

  /**
   * Bulk range iterator over ordinal (sorted / sorted-set) doc values. There is no columnar
   * range-decode like the numeric path, so MAYBE blocks confirm the ordinal predicate one doc at a
   * time, visiting only docs that have a value.
   */
  private static final class BulkOrdinalRangeIterator extends BulkBlockRangeIterator {

    private BulkOrdinalRangeIterator(
        DocIdSetIterator values,
        SkipBlockRangeIterator blockIterator,
        IOBooleanSupplier predicate,
        float matchCost) {
      super(values, blockIterator, predicate, matchCost);
    }

    @Override
    void intoMaybeBlock(int blockStart, int blockEnd, FixedBitSet bitSet, int offset)
        throws IOException {
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
