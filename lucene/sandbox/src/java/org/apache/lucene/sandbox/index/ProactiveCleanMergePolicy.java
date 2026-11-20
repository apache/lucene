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

package org.apache.lucene.sandbox.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.index.FilterMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.util.Unwrappable;

/**
 * A {@link FilterMergePolicy} that force-picks same-size-tier merges of zero-delete segments before
 * the wrapped policy scores candidates. That keeps cheap HNSW join-set merges available and avoids
 * pairing clean segments with deleted ones.
 *
 * <p>When every segment has zero deletes (append-only), this policy delegates to the wrapped policy
 * unchanged: {@link TieredMergePolicy}'s size-tier budget is a better fit than packing every clean
 * flush. Segments below {@code minProactiveSegmentSize} (the wrapped TMP floor by default) are left
 * for the inner cascade. Forced merges are not intercepted.
 *
 * <p>On commit, {@link #findFullFlushMerges} also requires each source segment to be smaller than
 * {@link #maxFullFlushMergeSize}. With a wrapped TMP those two bounds are the same by default (the
 * floor), so full-flush proactive merges are a no-op until {@code minProactiveSegmentSize} is set
 * below that cap.
 *
 * @lucene.experimental
 */
public class ProactiveCleanMergePolicy extends FilterMergePolicy {
  private static final long DEFAULT_MAX_PROACTIVE_MERGE_BYTES = 5L * 1024 * 1024 * 1024;
  private static final long DEFAULT_MIN_PROACTIVE_SEGMENT_SIZE = 16L * 1024 * 1024;

  private int minSegmentsForProactive = 3;
  private int maxProactiveMergeSegments = 10;
  private long maxProactiveMergeBytes;
  private long minProactiveSegmentSize;

  public ProactiveCleanMergePolicy(MergePolicy in) {
    super(in);
    MergePolicy unwrapped = Unwrappable.unwrapAll(in);
    if (unwrapped instanceof TieredMergePolicy) {
      TieredMergePolicy tmp = (TieredMergePolicy) unwrapped;
      maxProactiveMergeBytes = mbToBytes(tmp.getMaxMergedSegmentMB());
      minProactiveSegmentSize = mbToBytes(tmp.getFloorSegmentMB());
    } else {
      maxProactiveMergeBytes = DEFAULT_MAX_PROACTIVE_MERGE_BYTES;
      minProactiveSegmentSize = DEFAULT_MIN_PROACTIVE_SEGMENT_SIZE;
    }
  }

  public int getMinSegmentsForProactive() {
    return minSegmentsForProactive;
  }

  public void setMinSegmentsForProactive(int minSegmentsForProactive) {
    if (minSegmentsForProactive < 2) {
      throw new IllegalArgumentException(
          "minSegmentsForProactive must be >= 2 (got " + minSegmentsForProactive + ")");
    }
    this.minSegmentsForProactive = minSegmentsForProactive;
  }

  public int getMaxProactiveMergeSegments() {
    return maxProactiveMergeSegments;
  }

  public void setMaxProactiveMergeSegments(int maxProactiveMergeSegments) {
    if (maxProactiveMergeSegments < 2) {
      throw new IllegalArgumentException(
          "maxProactiveMergeSegments must be >= 2 (got " + maxProactiveMergeSegments + ")");
    }
    this.maxProactiveMergeSegments = maxProactiveMergeSegments;
  }

  public long getMaxProactiveMergeBytes() {
    return maxProactiveMergeBytes;
  }

  public void setMaxProactiveMergeBytes(long maxProactiveMergeBytes) {
    if (maxProactiveMergeBytes <= 0) {
      throw new IllegalArgumentException(
          "maxProactiveMergeBytes must be > 0 (got " + maxProactiveMergeBytes + ")");
    }
    this.maxProactiveMergeBytes = maxProactiveMergeBytes;
  }

  public long getMinProactiveSegmentSize() {
    return minProactiveSegmentSize;
  }

  public void setMinProactiveSegmentSize(long minProactiveSegmentSize) {
    if (minProactiveSegmentSize < 0) {
      throw new IllegalArgumentException(
          "minProactiveSegmentSize must be >= 0 (got " + minProactiveSegmentSize + ")");
    }
    this.minProactiveSegmentSize = minProactiveSegmentSize;
  }

  @Override
  public MergeSpecification findMerges(
      MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext)
      throws IOException {
    return selectMerges(mergeTrigger, segmentInfos, mergeContext, false);
  }

  @Override
  public MergeSpecification findFullFlushMerges(
      MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext)
      throws IOException {
    return selectMerges(mergeTrigger, segmentInfos, mergeContext, true);
  }

  private MergeSpecification selectMerges(
      MergeTrigger mergeTrigger,
      SegmentInfos segmentInfos,
      MergeContext mergeContext,
      boolean fullFlush)
      throws IOException {
    if (hasDeletes(segmentInfos, mergeContext) == false) {
      return delegateFind(mergeTrigger, segmentInfos, mergeContext, fullFlush);
    }
    long maxSizeBytes = fullFlush ? maxFullFlushMergeSize() : maxProactiveMergeBytes;
    List<SegAndSize> candidates = collectCleanCandidates(segmentInfos, mergeContext, maxSizeBytes);
    if (candidates.size() < minSegmentsForProactive) {
      return delegateFind(mergeTrigger, segmentInfos, mergeContext, fullFlush);
    }
    Collections.sort(candidates, (a, b) -> Long.compare(a.size, b.size));
    List<OneMerge> proactive = packTiers(candidates);
    if (proactive.isEmpty()) {
      return delegateFind(mergeTrigger, segmentInfos, mergeContext, fullFlush);
    }
    Set<SegmentCommitInfo> taken = new HashSet<>();
    for (int i = 0; i < proactive.size(); i++) {
      taken.addAll(proactive.get(i).segments);
    }
    SegmentInfos remaining = leftovers(segmentInfos, taken);
    MergeSpecification tmp = delegateFind(mergeTrigger, remaining, mergeContext, fullFlush);
    return combine(proactive, tmp);
  }

  private MergeSpecification delegateFind(
      MergeTrigger mergeTrigger,
      SegmentInfos segmentInfos,
      MergeContext mergeContext,
      boolean fullFlush)
      throws IOException {
    if (fullFlush) {
      return in.findFullFlushMerges(mergeTrigger, segmentInfos, mergeContext);
    }
    return in.findMerges(mergeTrigger, segmentInfos, mergeContext);
  }

  private static boolean hasDeletes(SegmentInfos segmentInfos, MergeContext mergeContext)
      throws IOException {
    for (SegmentCommitInfo sci : segmentInfos) {
      if (mergeContext.numDeletesToMerge(sci) != 0) {
        return true;
      }
    }
    return false;
  }

  private List<SegAndSize> collectCleanCandidates(
      SegmentInfos segmentInfos, MergeContext mergeContext, long maxSizeBytes) throws IOException {
    Set<SegmentCommitInfo> merging = mergeContext.getMergingSegments();
    List<SegAndSize> candidates = new ArrayList<>();
    for (SegmentCommitInfo sci : segmentInfos) {
      if (merging.contains(sci)) {
        continue;
      }
      if (mergeContext.numDeletesToMerge(sci) != 0) {
        continue;
      }
      long segSize = size(sci, mergeContext);
      if (segSize < minProactiveSegmentSize) {
        continue;
      }
      if (segSize >= maxSizeBytes) {
        continue;
      }
      candidates.add(new SegAndSize(sci, segSize));
    }
    return candidates;
  }

  private List<OneMerge> packTiers(List<SegAndSize> candidates) {
    List<OneMerge> merges = new ArrayList<>();
    int i = 0;
    while (i < candidates.size()) {
      List<SegmentCommitInfo> group = new ArrayList<>();
      long firstSize = candidates.get(i).size;
      long sum = 0;
      int j = i;
      while (j < candidates.size() && group.size() < maxProactiveMergeSegments) {
        long nextSize = candidates.get(j).size;
        if (nextSize > 2 * firstSize) {
          break;
        }
        if (sum + nextSize > maxProactiveMergeBytes) {
          break;
        }
        group.add(candidates.get(j).info);
        sum += nextSize;
        j++;
      }
      if (group.size() >= minSegmentsForProactive) {
        merges.add(new OneMerge(group));
        i = j;
      } else {
        i++;
      }
    }
    return merges;
  }

  private static SegmentInfos leftovers(SegmentInfos segmentInfos, Set<SegmentCommitInfo> taken) {
    SegmentInfos remaining = new SegmentInfos(segmentInfos.getIndexCreatedVersionMajor());
    for (SegmentCommitInfo sci : segmentInfos) {
      if (taken.contains(sci) == false) {
        remaining.add(sci);
      }
    }
    return remaining;
  }

  private static MergeSpecification combine(List<OneMerge> proactive, MergeSpecification tmp) {
    if (proactive.isEmpty()) {
      return tmp;
    }
    MergeSpecification spec = new MergeSpecification();
    for (int i = 0; i < proactive.size(); i++) {
      spec.add(proactive.get(i));
    }
    if (tmp != null) {
      List<OneMerge> tmpMerges = tmp.merges;
      for (int i = 0; i < tmpMerges.size(); i++) {
        spec.add(tmpMerges.get(i));
      }
    }
    return spec;
  }

  private static long mbToBytes(double mb) {
    return (long) (mb * 1024 * 1024);
  }

  private static final class SegAndSize {
    final SegmentCommitInfo info;
    final long size;

    SegAndSize(SegmentCommitInfo info, long size) {
      this.info = info;
      this.size = size;
    }
  }
}
