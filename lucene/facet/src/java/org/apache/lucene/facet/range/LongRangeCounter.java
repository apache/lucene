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
package org.apache.lucene.facet.range;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.util.FixedBitSet;

/**
 * Segment tree for counting numeric ranges. Works for both single- and multi-valued cases (assuming
 * you use it correctly).
 *
 * <p>Usage notes: For counting against a single value field/source, callers should call
 * addSingleValued() for each value and then call finish() after all documents have been processed.
 * The call to finish() will inform the caller how many documents didn't match against any ranges.
 * After finish() has been called, the caller-provided count buffer (passed into the ctor) will be
 * populated with accurate range counts.
 *
 * <p>For counting against a multi-valued field, callers should call startDoc() at the beginning of
 * processing each doc, followed by addMultiValued() for each value, and then endDoc() at the end of
 * the doc. The call to endDoc() will inform the caller if that document matched against any ranges.
 *
 * <p>Note that it's possible to mix single- and multi-valued call patterns. Docs having a single
 * value only need to use addSingleValued(), while docs with multiple values need to use startDoc(),
 * addMultiValued(), endDoc(). The caller should always call finish() at the end to ensure all
 * counts are flushed and final missing docs are identified.
 */
final class LongRangeCounter {

  /** segment tree root node */
  private final LongRangeNode root;
  /** number of requested ranges */
  private final int numRanges;
  /** elementary segment boundaries used for efficient counting (bsearch to find interval) */
  private final long[] boundaries;
  /** accumulated counts for all of the ranges */
  private final int[] countBuffer;
  /** whether-or-not there are leaf counts that still need to be rolled up at the end */
  private boolean hasUnflushedCounts = false;

  // Needed only for counting single-valued docs:
  /** counts seen in each elementary interval leaf */
  private int[] singleValuedLeafCounts;

  // Needed only for counting multi-valued docs:
  /** whether-or-not an elementary interval has seen at least one match for a single doc */
  private FixedBitSet multiValuedDocLeafHits;
  /** whether-or-not a range has seen at least one match for a single doc */
  private FixedBitSet multiValuedDocRangeHits;

  // Used during rollup
  private int leafUpto;
  private int missingCount;

  LongRangeCounter(LongRange[] ranges, int[] countBuffer) {

    // We'll populate the user-provided count buffer with range counts:
    this.countBuffer = countBuffer;

    // Build elementary intervals:
    List<InclusiveRange> elementaryIntervals = buildElementaryIntervals(ranges);

    // Build binary tree on top of intervals:
    root = split(0, elementaryIntervals.size(), elementaryIntervals);

    // Set outputs, so we know which range to output for each node in the tree:
    for (int i = 0; i < ranges.length; i++) {
      root.addOutputs(i, ranges[i]);
    }

    // Set boundaries (ends of each elementary interval):
    boundaries = new long[elementaryIntervals.size()];
    for (int i = 0; i < boundaries.length; i++) {
      boundaries[i] = elementaryIntervals.get(i).end;
    }

    numRanges = ranges.length;
  }

  /** Start processing a new doc. It's unnecessary to call this for single-value cases. */
  void startDoc() {
    if (multiValuedDocLeafHits == null) {
      multiValuedDocLeafHits = new FixedBitSet(boundaries.length);
    } else {
      multiValuedDocLeafHits.clear(0, multiValuedDocLeafHits.length());
    }
  }

  /**
   * Finish processing a new doc. Returns whether-or-not the document contributed a count to at
   * least one range. It's unnecessary to call this for single-value cases.
   */
  boolean endDoc() {
    assert multiValuedDocLeafHits != null : "must call startDoc() first";

    // Necessary to rollup after each doc for multi-valued case:

    // Short-circuit if the caller didn't specify any ranges to count
    if (numRanges == 0) {
      return false;
    }

    if (multiValuedDocRangeHits == null) {
      multiValuedDocRangeHits = new FixedBitSet(numRanges);
    } else {
      multiValuedDocRangeHits.clear(0, multiValuedDocRangeHits.length());
    }

    leafUpto = 0;
    rollupMultiValued(root);

    boolean docContributedToAtLeastOneRange = false;
    for (int i = multiValuedDocRangeHits.nextSetBit(0); i < multiValuedDocRangeHits.length(); ) {
      countBuffer[i]++;
      docContributedToAtLeastOneRange = true;
      if (++i < multiValuedDocRangeHits.length()) {
        i = multiValuedDocRangeHits.nextSetBit(i);
      }
    }

    return docContributedToAtLeastOneRange;
  }

  /** Count a single valued doc */
  void addSingleValued(long v) {
    // NOTE: this works too, but it's ~6% slower on a simple
    // test with a high-freq TermQuery w/ range faceting on
    // wikimediumall:
    /*
    int index = Arrays.binarySearch(boundaries, v);
    if (index < 0) {
      index = -index-1;
    }
    leafCounts[index]++;
    */

    // Binary search to find matched elementary range; we
    // are guaranteed to find a match because the last
    // boundary is Long.MAX_VALUE:

    if (singleValuedLeafCounts == null) {
      singleValuedLeafCounts = new int[boundaries.length];
    }

    int lo = 0;
    int hi = boundaries.length - 1;
    while (true) {
      int mid = (lo + hi) >>> 1;
      if (v <= boundaries[mid]) {
        if (mid == 0) {
          singleValuedLeafCounts[0]++;
          hasUnflushedCounts = true;
          return;
        } else {
          hi = mid - 1;
        }
      } else if (v > boundaries[mid + 1]) {
        lo = mid + 1;
      } else {
        singleValuedLeafCounts[mid + 1]++;
        hasUnflushedCounts = true;
        return;
      }
    }
  }

  /** Count a multi-valued doc value */
  void addMultiValued(long v) {
    assert multiValuedDocLeafHits != null : "must call startDoc() first";

    int lo = 0;
    int hi = boundaries.length - 1;
    while (true) {
      int mid = (lo + hi) >>> 1;
      if (v <= boundaries[mid]) {
        if (mid == 0) {
          multiValuedDocLeafHits.set(0);
          return;
        } else {
          hi = mid - 1;
        }
      } else if (v > boundaries[mid + 1]) {
        lo = mid + 1;
      } else {
        multiValuedDocLeafHits.set(mid + 1);
        return;
      }
    }
  }

  /**
   * Finish processing all documents. This will return the number of docs that didn't contribute to
   * any ranges (that weren't already reported when calling endDoc()).
   */
  int finish() {
    if (hasUnflushedCounts) {
      missingCount = 0;
      leafUpto = 0;
      rollupSingleValued(root, false);

      return missingCount;
    } else {
      return 0;
    }
  }

  private static List<InclusiveRange> buildElementaryIntervals(LongRange[] ranges) {
    // Maps all range inclusive endpoints to int flags; 1
    // = start of interval, 2 = end of interval.  We need to
    // track the start vs end case separately because if a
    // given point is both, then it must be its own
    // elementary interval:
    Map<Long, Integer> endsMap = new HashMap<>();

    endsMap.put(Long.MIN_VALUE, 1);
    endsMap.put(Long.MAX_VALUE, 2);

    for (LongRange range : ranges) {
      Integer cur = endsMap.get(range.min);
      if (cur == null) {
        endsMap.put(range.min, 1);
      } else {
        endsMap.put(range.min, cur.intValue() | 1);
      }
      cur = endsMap.get(range.max);
      if (cur == null) {
        endsMap.put(range.max, 2);
      } else {
        endsMap.put(range.max, cur.intValue() | 2);
      }
    }

    List<Long> endsList = new ArrayList<>(endsMap.keySet());
    Collections.sort(endsList);

    // Build elementaryIntervals (a 1D Venn diagram):
    List<InclusiveRange> elementaryIntervals = new ArrayList<>();
    int upto0 = 1;
    long v = endsList.get(0);
    long prev;
    if (endsMap.get(v) == 3) {
      elementaryIntervals.add(new InclusiveRange(v, v));
      prev = v + 1;
    } else {
      prev = v;
    }

    while (upto0 < endsList.size()) {
      v = endsList.get(upto0);
      int flags = endsMap.get(v);
      if (flags == 3) {
        // This point is both an end and a start; we need to
        // separate it:
        if (v > prev) {
          elementaryIntervals.add(new InclusiveRange(prev, v - 1));
        }
        elementaryIntervals.add(new InclusiveRange(v, v));
        prev = v + 1;
      } else if (flags == 1) {
        // This point is only the start of an interval;
        // attach it to next interval:
        if (v > prev) {
          elementaryIntervals.add(new InclusiveRange(prev, v - 1));
        }
        prev = v;
      } else {
        assert flags == 2;
        // This point is only the end of an interval; attach
        // it to last interval:
        elementaryIntervals.add(new InclusiveRange(prev, v));
        prev = v + 1;
      }
      upto0++;
    }

    return elementaryIntervals;
  }

  private static LongRangeNode split(int start, int end, List<InclusiveRange> elementaryIntervals) {
    if (start == end - 1) {
      // leaf
      InclusiveRange range = elementaryIntervals.get(start);
      return new LongRangeNode(range.start, range.end, null, null, start);
    } else {
      int mid = (start + end) >>> 1;
      LongRangeNode left = split(start, mid, elementaryIntervals);
      LongRangeNode right = split(mid, end, elementaryIntervals);
      return new LongRangeNode(left.start, right.end, left, right, -1);
    }
  }

  private int rollupSingleValued(LongRangeNode node, boolean sawOutputs) {
    int count;
    sawOutputs |= node.outputs != null;
    if (node.left != null) {
      count = rollupSingleValued(node.left, sawOutputs);
      count += rollupSingleValued(node.right, sawOutputs);
    } else {
      // Leaf:
      count = singleValuedLeafCounts[leafUpto];
      leafUpto++;
      if (!sawOutputs) {
        // This is a missing count (no output ranges were
        // seen "above" us):
        missingCount += count;
      }
    }
    if (node.outputs != null) {
      for (int rangeIndex : node.outputs) {
        countBuffer[rangeIndex] += count;
      }
    }

    return count;
  }

  private boolean rollupMultiValued(LongRangeNode node) {
    boolean containedHit;
    if (node.left != null) {
      containedHit = rollupMultiValued(node.left);
      containedHit |= rollupMultiValued(node.right);
    } else {
      // Leaf:
      containedHit = multiValuedDocLeafHits.get(leafUpto);
      leafUpto++;
    }
    if (containedHit && node.outputs != null) {
      for (int rangeIndex : node.outputs) {
        multiValuedDocRangeHits.set(rangeIndex);
      }
    }

    return containedHit;
  }

  private static final class InclusiveRange {
    public final long start;
    public final long end;

    public InclusiveRange(long start, long end) {
      assert end >= start;
      this.start = start;
      this.end = end;
    }

    @Override
    public String toString() {
      return start + " to " + end;
    }
  }

  /** Holds one node of the segment tree. */
  public static final class LongRangeNode {
    final LongRangeNode left;
    final LongRangeNode right;

    // Our range, inclusive:
    final long start;
    final long end;

    // If we are a leaf, the index into elementary ranges that
    // we point to:
    final int leafIndex;

    // Which range indices to output when a query goes
    // through this node:
    List<Integer> outputs;

    public LongRangeNode(
        long start, long end, LongRangeNode left, LongRangeNode right, int leafIndex) {
      this.start = start;
      this.end = end;
      this.left = left;
      this.right = right;
      this.leafIndex = leafIndex;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      toString(sb, 0);
      return sb.toString();
    }

    static void indent(StringBuilder sb, int depth) {
      for (int i = 0; i < depth; i++) {
        sb.append("  ");
      }
    }

    /** Recursively assigns range outputs to each node. */
    void addOutputs(int index, LongRange range) {
      if (start >= range.min && end <= range.max) {
        // Our range is fully included in the incoming
        // range; add to our output list:
        if (outputs == null) {
          outputs = new ArrayList<>();
        }
        outputs.add(index);
      } else if (left != null) {
        assert right != null;
        // Recurse:
        left.addOutputs(index, range);
        right.addOutputs(index, range);
      }
    }

    void toString(StringBuilder sb, int depth) {
      indent(sb, depth);
      if (left == null) {
        assert right == null;
        sb.append("leaf: ").append(start).append(" to ").append(end);
      } else {
        sb.append("node: ").append(start).append(" to ").append(end);
      }
      if (outputs != null) {
        sb.append(" outputs=");
        sb.append(outputs);
      }
      sb.append('\n');

      if (left != null) {
        assert right != null;
        left.toString(sb, depth + 1);
        right.toString(sb, depth + 1);
      }
    }
  }
}
