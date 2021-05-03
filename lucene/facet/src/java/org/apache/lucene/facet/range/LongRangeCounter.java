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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Segment tree for counting numeric ranges. Works for both single- and multi-valued cases (assuming
 * you use it correctly).
 *
 * <p>Usage notes: For counting against a single value field/source, callers should call add() for
 * each value and then call finish() after all documents have been processed. The call to finish()
 * will inform the caller how many documents didn't match against any ranges. After finish() has
 * been called, the caller-provided count buffer (passed into the ctor) will be populated with
 * accurate range counts.
 *
 * <p>For counting against a multi-valued field, callers should call startDoc() at the beginning of
 * processing each doc, followed by add() for each value, and then endDoc() at the end of the doc.
 * The call to endDoc() will inform the caller if that document matched against any ranges. It is
 * not necessary to call finish() for multi-valued cases. After each call to endDoc(), the
 * caller-provided count buffer (passed into the ctor) will be populated with accurate range counts.
 */
final class LongRangeCounter {

  /** segment tree root node */
  private final LongRangeNode root;
  /** elementary segment boundaries used for efficient counting (bsearch to find interval) */
  private final long[] boundaries;
  /** accumulated counts for all of the ranges */
  private final int[] countBuffer;
  /** whether-or-not we're counting docs that could be multi-valued */
  final boolean isMultiValued;

  // Needed only for counting single-valued docs:
  /** counts seen in each elementary interval leaf */
  private final int[] singleValuedLeafCounts;

  // Needed only for counting multi-valued docs:
  /** whether-or-not an elementary interval has seen at least one match for a single doc */
  private final boolean[] multiValuedDocLeafHits;
  /** whether-or-not a range has seen at least one match for a single doc */
  private final boolean[] multiValuedDocRangeHits;

  // Used during rollup
  private int leafUpto;
  private int missingCount;

  LongRangeCounter(LongRange[] ranges, int[] countBuffer, boolean isMultiValued) {
    // Whether-or-not we're processing docs that could be multi-valued:
    this.isMultiValued = isMultiValued;

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

    // Setup to count:
    if (isMultiValued == false) {
      // Setup to count single-valued docs only:
      singleValuedLeafCounts = new int[boundaries.length];
      multiValuedDocLeafHits = null;
      multiValuedDocRangeHits = null;
    } else {
      // Setup to count multi-valued docs:
      singleValuedLeafCounts = null;
      multiValuedDocLeafHits = new boolean[boundaries.length];
      multiValuedDocRangeHits = new boolean[ranges.length];
    }
  }

  /**
   * Start processing a new doc. It's unnecessary to call this for single-value cases (but it
   * doesn't cause problems if you do).
   */
  void startDoc() {
    if (isMultiValued) {
      Arrays.fill(multiValuedDocLeafHits, false);
    }
  }

  /**
   * Finish processing a new doc. Returns whether-or-not the document contributed a count to at
   * least one range. It's unnecessary to call this for single-value cases, and the return value in
   * such cases will always be {@code true} (but calling it doesn't cause any problems).
   */
  boolean endDoc() {
    // Necessary to rollup after each doc for multi-valued case:
    if (isMultiValued) {
      leafUpto = 0;
      Arrays.fill(multiValuedDocRangeHits, false);
      rollupMultiValued(root);
      boolean docContributedToAtLeastOneRange = false;
      for (int i = 0; i < multiValuedDocRangeHits.length; i++) {
        if (multiValuedDocRangeHits[i]) {
          countBuffer[i]++;
          docContributedToAtLeastOneRange = true;
        }
      }

      return docContributedToAtLeastOneRange;
    } else {
      return true;
    }
  }

  /** Count a value. */
  void add(long v) {
    if (isMultiValued) {
      addMultiValued(v);
    } else {
      addSingleValued(v);
    }
  }

  /**
   * Finish processing all documents. This will return the number of docs that didn't contribute to
   * any ranges. It's unnecessary to call this for multi-value cases, and the return value will
   * always be zero.
   */
  int finish() {
    if (isMultiValued == false) {
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

  private void addSingleValued(long v) {
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

    int lo = 0;
    int hi = boundaries.length - 1;
    while (true) {
      int mid = (lo + hi) >>> 1;
      if (v <= boundaries[mid]) {
        if (mid == 0) {
          singleValuedLeafCounts[0]++;
          return;
        } else {
          hi = mid - 1;
        }
      } else if (v > boundaries[mid + 1]) {
        lo = mid + 1;
      } else {
        singleValuedLeafCounts[mid + 1]++;
        return;
      }
    }
  }

  private void addMultiValued(long v) {
    int lo = 0;
    int hi = boundaries.length - 1;
    while (true) {
      int mid = (lo + hi) >>> 1;
      if (v <= boundaries[mid]) {
        if (mid == 0) {
          multiValuedDocLeafHits[0] = true;
          return;
        } else {
          hi = mid - 1;
        }
      } else if (v > boundaries[mid + 1]) {
        lo = mid + 1;
      } else {
        int idx = mid + 1;
        multiValuedDocLeafHits[idx] = true;
        return;
      }
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
      containedHit = multiValuedDocLeafHits[leafUpto];
      leafUpto++;
    }
    if (containedHit && node.outputs != null) {
      for (int rangeIndex : node.outputs) {
        multiValuedDocRangeHits[rangeIndex] = true;
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
