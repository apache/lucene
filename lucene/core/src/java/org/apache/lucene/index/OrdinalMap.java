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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

/**
 * Maps per-segment ordinals to/from global ordinal space, using a compact packed-ints
 * representation.
 *
 * <p><b>NOTE</b>: this is a costly operation, as it must merge sort all terms, and may require
 * non-trivial RAM once done. It's better to operate in segment-private ordinal space instead when
 * possible.
 *
 * @lucene.internal
 */
public class OrdinalMap implements Accountable {
  // TODO: we could also have a utility method to merge Terms[] and use size() as a weight when we
  //  need it
  // TODO: use more efficient packed ints structures?

  private static class TermsEnumAndOrderedPos {

    // stores n where termsEnum is the nth largest segment in the index
    final int orderedDescPos;
    final TermsEnum termsEnum;
    BytesRef currentTerm;

    public TermsEnumAndOrderedPos(TermsEnum termsEnum, int orderedDescPos) {
      this.termsEnum = termsEnum;
      this.orderedDescPos = orderedDescPos;
    }

    public BytesRef next() throws IOException {
      currentTerm = termsEnum.next();
      return currentTerm;
    }
  }

  private static class SegmentMap implements Accountable {
    private static final long BASE_RAM_BYTES_USED =
        RamUsageEstimator.shallowSizeOfInstance(SegmentMap.class);

    /** Return list of indices sorted in descending order */
    private static int[] sortIndices(final long[] segmentWeights) {
      final int[] descSortedSegments = new int[segmentWeights.length];
      for (int i = 0; i < segmentWeights.length; ++i) {
        descSortedSegments[i] = i;
      }
      new InPlaceMergeSorter() {
        @Override
        protected void swap(int i, int j) {
          final int tmp = descSortedSegments[i];
          descSortedSegments[i] = descSortedSegments[j];
          descSortedSegments[j] = tmp;
        }

        @Override
        protected int compare(int i, int j) {
          // j first since we actually want higher weights first
          return Long.compare(
              segmentWeights[descSortedSegments[j]], segmentWeights[descSortedSegments[i]]);
        }
      }.sort(0, segmentWeights.length);
      return descSortedSegments;
    }

    /** Inverse the map. */
    private static int[] inverse(int[] map) {
      final int[] inverse = new int[map.length];
      for (int i = 0; i < map.length; ++i) {
        inverse[map[i]] = i;
      }
      return inverse;
    }

    private final int[] descSortedSegments, oldToSorted;

    /**
     * Creates a SegmentMap
     *
     * @param segmentWeights 'Weights' are assigned to each per segment TermsEnum passed into the
     *     OrdinalMap. This is ideally correlated with the number of unique terms in each
     *     per-segment TermsEnum
     */
    SegmentMap(long[] segmentWeights) {
      // lists the TermsEnum indices ordered by weight in descending order
      descSortedSegments = sortIndices(segmentWeights);
      // maps each TermsEnum's index to its position in descSortedSegments
      oldToSorted = inverse(descSortedSegments);
      assert Arrays.equals(descSortedSegments, inverse(oldToSorted));
    }

    int getSegmentAtDescSortedPosition(int position) {
      return descSortedSegments[position];
    }

    int getSortedPositionForSegment(int segment) {
      return oldToSorted[segment];
    }

    @Override
    public long ramBytesUsed() {
      return BASE_RAM_BYTES_USED
          + RamUsageEstimator.sizeOf(descSortedSegments)
          + RamUsageEstimator.sizeOf(oldToSorted);
    }
  }

  /**
   * Create an ordinal map that uses the number of unique values of each {@link SortedDocValues}
   * instance as a weight.
   *
   * @see #build(IndexReader.CacheKey, TermsEnum[], long[], float)
   */
  public static OrdinalMap build(
      IndexReader.CacheKey owner, SortedDocValues[] values, float acceptableOverheadRatio)
      throws IOException {
    final TermsEnum[] subs = new TermsEnum[values.length];
    final long[] weights = new long[values.length];
    for (int i = 0; i < values.length; ++i) {
      subs[i] = values[i].termsEnum();
      weights[i] = values[i].getValueCount();
    }
    return build(owner, subs, weights, acceptableOverheadRatio);
  }

  /**
   * Create an ordinal map that uses the number of unique values of each {@link SortedSetDocValues}
   * instance as a weight.
   *
   * @see #build(IndexReader.CacheKey, TermsEnum[], long[], float)
   */
  public static OrdinalMap build(
      IndexReader.CacheKey owner, SortedSetDocValues[] values, float acceptableOverheadRatio)
      throws IOException {
    final TermsEnum[] subs = new TermsEnum[values.length];
    final long[] weights = new long[values.length];
    for (int i = 0; i < values.length; ++i) {
      subs[i] = values[i].termsEnum();
      weights[i] = values[i].getValueCount();
    }
    return build(owner, subs, weights, acceptableOverheadRatio);
  }

  /**
   * Creates an ordinal map that allows mapping ords to/from a merged space from <code>subs</code>.
   *
   * @param owner a cache key
   * @param perSegmentTermEnums TermsEnums that support {@link TermsEnum#ord()}. They need not be
   *     dense (e.g. can be FilteredTermsEnums}.
   * @param segmentWeights a weight for each TermEnum/segment. This is ideally correlated with the
   *     number of unique terms that each term enum introduces compared to the other term enums
   * @throws IOException if an I/O error occurred.
   */
  public static OrdinalMap build(
      IndexReader.CacheKey owner,
      TermsEnum[] perSegmentTermEnums,
      long[] segmentWeights,
      float acceptableOverheadRatio)
      throws IOException {
    if (perSegmentTermEnums.length != segmentWeights.length) {
      throw new IllegalArgumentException(
          "perSegmentTermEnums and segmentWeights must have the same length");
    }

    // enums are not sorted, so let's sort to save memory
    final SegmentMap segmentMap = new SegmentMap(segmentWeights);
    return new OrdinalMap(owner, perSegmentTermEnums, segmentMap, acceptableOverheadRatio);
  }

  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(OrdinalMap.class);

  /** Cache key of whoever asked for this awful thing */
  public final IndexReader.CacheKey owner;
  // number of global ordinals
  final long valueCount;
  // globalOrd -> (globalOrd - segmentOrd) where segmentOrd is the ordinal in the first segment
  // that contains this term
  final LongValues globalOrdDeltas;
  // globalOrd -> first segment container
  final LongValues firstSegments;
  // (segmentIndex, segmentOrd) -> globalOrd
  final LongValues[] segmentToGlobalOrds;
  // the map from/to segment ids
  final SegmentMap segmentMap;
  // ram usage
  final long ramBytesUsed;

  /**
   * Here is how the OrdinalMap encodes the mapping from global ords to local segment ords. Assume
   * we have the following global mapping for a doc values field: <br>
   * bar -> 0, cat -> 1, dog -> 2, foo -> 3 <br>
   * And our index is split into 2 segments with the following local mappings for that same doc
   * values field: <br>
   * Segment 0: bar -> 0, foo -> 1 <br>
   * Segment 1: cat -> 0, dog -> 1 <br>
   * We will then encode delta between the local and global mapping in a packed 2d array keyed by
   * (segmentIndex, segmentOrd). So the following 2d array will be created by OrdinalMap: <br>
   * [[0, 2], [1, 1]]
   *
   * <p>The general algorithm for creating an OrdinalMap (skipping over some implementation details
   * and optimizations) is as follows:
   *
   * <p>[1] Create and populate a PQ with ({@link TermsEnum}, index) tuples where index is the
   * position of the termEnum in an array of termEnum's sorted by descending size. The PQ itself
   * will be ordered by {@link TermsEnum#term()}
   *
   * <p>[2] We will iterate through every term in the index now. In order to do so, we will start
   * with the first term at the top of the PQ . We keep track of a global ord, and track the
   * difference between the global ord and {@link TermsEnum#ord()} in ordDeltas, which maps: <br>
   * (segmentIndex, {@link TermsEnum#ord()}) -> globalTermOrdinal - {@link TermsEnum#ord()} <br>
   * We then call {@link TermsEnum#next()} then update the PQ to iterate (remember the PQ maintains
   * and order based on {@link TermsEnum#term()} which changes on the next() calls). If the current
   * term exists in some other segment, the top of the queue will contain that segment. If not, the
   * top of the queue will contain a segment with the next term in the index and the global ord will
   * also be incremented.
   *
   * <p>[3] We use some information gathered in the previous step to perform optimizations on memory
   * usage and building time in the following steps, for more detail on those, look at the code.
   *
   * <p>[4] We will then populate segmentToGlobalOrds, which maps (segmentIndex, segmentOrd) ->
   * globalOrd. Using the information we tracked in ordDeltas, we can construct this information
   * relatively easily.
   *
   * @param owner For caching purposes
   * @param perSegmentTermsEnums A TermsEnum[], where each index corresponds to a segment
   * @param segmentMap Provides two maps, newToOld which lists segments in descending 'weight' order
   *     (see {@link SegmentMap} for more details) and a oldToNew map which maps each original
   *     segment index to their position in newToOld
   * @param acceptableOverheadRatio Acceptable overhead memory usage for some packed data structures
   * @throws IOException throws IOException
   */
  OrdinalMap(
      IndexReader.CacheKey owner,
      TermsEnum[] perSegmentTermsEnums,
      SegmentMap segmentMap,
      float acceptableOverheadRatio)
      throws IOException {
    // create the ordinal mappings by pulling a TermsEnum over each sub's
    // unique terms, and walking a MultiTermsEnum over those
    this.owner = owner;
    this.segmentMap = segmentMap;
    // even though we accept an overhead ratio, we keep these ones with COMPACT
    // since they are only used to resolve values given a global ord, which is
    // slow anyway

    // Tracks the delta between the per-segment ord for the largest segment ord a term is in and the
    // term's global ord,
    // ordered by the term's global ord
    PackedLongValues.Builder globalOrdDeltas =
        PackedLongValues.monotonicBuilder(PackedInts.COMPACT);

    // Tracks the first (and largest) segment a term appears in, ordered by the term's global ord
    PackedLongValues.Builder firstSegments = PackedLongValues.packedBuilder(PackedInts.COMPACT);

    boolean firstSegmentHasAllTerms = true;

    // Treat this as a long[][] array where we map:
    // (segmentIndex, termSegmentOrdinal) -> globalTermOrdinal - termSegmentOrdinal
    final PackedLongValues.Builder[] ordDeltas =
        new PackedLongValues.Builder[perSegmentTermsEnums.length];
    for (int i = 0; i < ordDeltas.length; i++) {
      ordDeltas[i] = PackedLongValues.monotonicBuilder(acceptableOverheadRatio);
    }

    // Per segment, keeps track if a non-zero delta occurs in a segment, if a negative delta occurs
    // in a segment, and will let us know the largest amount of bits required to hold the deltas for
    // a segment
    long[] ordDeltaBits = new long[perSegmentTermsEnums.length];

    // tracks current term ord we are on for each segment
    long[] segmentOrds = new long[perSegmentTermsEnums.length];

    // Just merge-sorts by first term in TermsEnum:
    PriorityQueue<TermsEnumAndOrderedPos> queue =
        new PriorityQueue<>(perSegmentTermsEnums.length) {
          @Override
          protected boolean lessThan(TermsEnumAndOrderedPos a, TermsEnumAndOrderedPos b) {
            return a.currentTerm.compareTo(b.currentTerm) < 0;
          }
        };

    for (int i = 0; i < perSegmentTermsEnums.length; i++) {
      TermsEnumAndOrderedPos sub =
          new TermsEnumAndOrderedPos(
              perSegmentTermsEnums[segmentMap.getSegmentAtDescSortedPosition(i)], i);
      if (sub.next() != null) {
        queue.add(sub);
      }
    }

    BytesRefBuilder currentTerm = new BytesRefBuilder();

    long globalOrd = 0;
    while (queue.size() != 0) {
      TermsEnumAndOrderedPos top = queue.top();
      currentTerm.copyBytes(top.currentTerm);

      int firstSegmentIndex = Integer.MAX_VALUE;
      long globalOrdDelta = Long.MAX_VALUE;

      // Iterates through every segment containing currentTerm
      do {
        top = queue.top();
        long segmentOrd = top.termsEnum.ord(); // per-segment ord of current term
        long delta = globalOrd - segmentOrd;
        int segmentIndex = top.orderedDescPos;
        // We compute the least segment where the term occurs. In case the
        // first segment contains most (or better all) values, this will
        // help save significant memory
        if (segmentIndex < firstSegmentIndex) {
          firstSegmentIndex = segmentIndex;
          globalOrdDelta = delta;
        }
        ordDeltaBits[segmentIndex] |= delta;

        // for each per-segment ord, map it back to the global term; the while loop is needed
        // in case the incoming TermsEnums don't have compact ordinals (some ordinal values
        // are skipped), which can happen e.g. with a FilteredTermsEnum:
        assert segmentOrds[segmentIndex] <= segmentOrd;

        // TODO: we could specialize this case (the while loop is not needed when the ords
        // are compact)
        do {
          ordDeltas[segmentIndex].add(delta);
          segmentOrds[segmentIndex]++;
        } while (segmentOrds[segmentIndex] <= segmentOrd);

        if (top.next() == null) {
          // remove fully iterated segment
          queue.pop();
        } else {
          // by calling updateTop(), the top of the PQ will now contain the next lowest global term
          // (in BytesRef order),
          // or the same term if it is in another segment
          // TODO: is it faster to just go through every segment and only update once we need to go
          // to the next term?
          queue.updateTop();
        }
      } while (queue.size() != 0 && queue.top().currentTerm.equals(currentTerm.get()));

      // for each unique term, just mark the first (largest) segment index/delta where it occurs
      firstSegments.add(firstSegmentIndex);
      globalOrdDeltas.add(globalOrdDelta);
      globalOrd++;

      if (firstSegmentHasAllTerms && firstSegmentIndex != 0) {
        firstSegmentHasAllTerms = false;
      }
    }

    long ramBytesUsed = BASE_RAM_BYTES_USED + segmentMap.ramBytesUsed();
    this.valueCount = globalOrd;

    // If the first segment contains all of the global ords, then we can apply a small optimization
    // and hardcode the first segment indices and global ord deltas as all zeroes.
    boolean areFirstSegmentAndGlobalOrdsEquivalent =
        ordDeltaBits.length > 0 && ordDeltaBits[0] == 0L;
    if (areFirstSegmentAndGlobalOrdsEquivalent && firstSegmentHasAllTerms) {
      this.firstSegments = LongValues.ZEROES;
      this.globalOrdDeltas = LongValues.ZEROES;
    } else {
      PackedLongValues packedFirstSegments = firstSegments.build();
      PackedLongValues packedGlobalOrdDeltas = globalOrdDeltas.build();
      this.firstSegments = packedFirstSegments;
      this.globalOrdDeltas = packedGlobalOrdDeltas;
      ramBytesUsed += packedFirstSegments.ramBytesUsed() + packedGlobalOrdDeltas.ramBytesUsed();
    }

    // ordDeltas is typically the bottleneck, so let's see what we can do to make it faster
    segmentToGlobalOrds = new LongValues[perSegmentTermsEnums.length];
    ramBytesUsed += RamUsageEstimator.shallowSizeOf(segmentToGlobalOrds);
    for (int segmentIndex = 0; segmentIndex < ordDeltas.length; ++segmentIndex) {
      final PackedLongValues deltas = ordDeltas[segmentIndex].build();
      if (ordDeltaBits[segmentIndex] == 0L) {
        // segment ords perfectly match global ordinals
        // likely in case of low cardinalities and large segments
        segmentToGlobalOrds[segmentIndex] = LongValues.IDENTITY;
      } else {
        final int bitsRequired =
            ordDeltaBits[segmentIndex] < 0
                ? 64
                : PackedInts.bitsRequired(ordDeltaBits[segmentIndex]);
        final long monotonicBits = deltas.ramBytesUsed() * 8;
        final long packedBits = bitsRequired * deltas.size();
        if (deltas.size() <= Integer.MAX_VALUE
            && packedBits <= monotonicBits * (1 + acceptableOverheadRatio)) {
          // monotonic compression mostly adds overhead, let's keep the mapping in plain packed ints
          final int size = (int) deltas.size();
          final PackedInts.Mutable newDeltas =
              PackedInts.getMutable(size, bitsRequired, acceptableOverheadRatio);
          final PackedLongValues.Iterator it = deltas.iterator();
          for (int ord = 0; ord < size; ++ord) {
            newDeltas.set(ord, it.next());
          }
          assert it.hasNext() == false;
          segmentToGlobalOrds[segmentIndex] =
              new LongValues() {
                @Override
                public long get(long ord) {
                  return ord + newDeltas.get((int) ord);
                }
              };
          ramBytesUsed += newDeltas.ramBytesUsed();
        } else {
          segmentToGlobalOrds[segmentIndex] =
              new LongValues() {
                @Override
                public long get(long ord) {
                  return ord + deltas.get(ord);
                }
              };
          ramBytesUsed += deltas.ramBytesUsed();
        }
        ramBytesUsed += RamUsageEstimator.shallowSizeOf(segmentToGlobalOrds[segmentIndex]);
      }
    }

    this.ramBytesUsed = ramBytesUsed;
  }

  /**
   * Given a segment number, return a {@link LongValues} instance that maps segment ordinals to
   * global ordinals.
   */
  public LongValues getGlobalOrds(int segmentIndex) {
    return segmentToGlobalOrds[segmentMap.getSortedPositionForSegment(segmentIndex)];
  }

  /**
   * Given global ordinal, returns the ordinal of the first segment which contains this ordinal (the
   * corresponding to the segment return {@link #getFirstSegmentNumber}).
   */
  public long getFirstSegmentOrd(long globalOrd) {
    return globalOrd - globalOrdDeltas.get(globalOrd);
  }

  /** Given a global ordinal, returns the index of the first segment that contains this term. */
  public int getFirstSegmentNumber(long globalOrd) {
    return segmentMap.getSegmentAtDescSortedPosition((int) firstSegments.get(globalOrd));
  }

  /** Returns the total number of unique terms in global ord space. */
  public long getValueCount() {
    return valueCount;
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed;
  }

  @Override
  public Collection<Accountable> getChildResources() {
    List<Accountable> resources = new ArrayList<>();
    resources.add(Accountables.namedAccountable("segment map", segmentMap));
    // TODO: would be nice to return the ordinal and segment maps too, but it's not straightforward
    //  because of optimizations.
    return resources;
  }
}
