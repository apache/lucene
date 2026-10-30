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
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.util.InfoStream;

/**
 * Wraps a {@link MergePolicy} and runs optional {@link MergeSelector}s before and after it. With no
 * selectors set, every decision is the wrapped policy's.
 *
 * <p>{@link #findMerges} uses {@link #setBeforeMergeSelector} then the wrapped policy, then {@link
 * #setAfterMergeSelector}. Passing null clears a slot. Forced merges, commits, and {@code
 * getReader} stay with the wrapped policy, because full-flush merges block the caller for up to
 * {@link IndexWriterConfig#getMaxFullFlushMergeWaitMillis()}.
 *
 * <p>Segments a selector claims are reported as already merging for everything that runs after it.
 * The full segment list is always passed on. Stock policies already refuse to merge across a
 * merging segment and, in the tiered policy, count those bytes in the merge budget. Removing the
 * segments instead would open a hole and let a log policy merge the neighbours together.
 *
 * @lucene.experimental
 */
public final class ProactiveMergePolicy extends FilterMergePolicy {

  private MergeSelector beforeMergeSelector;
  private MergeSelector afterMergeSelector;

  /** Wraps {@code in}. A null policy is rejected. */
  public ProactiveMergePolicy(MergePolicy in) {
    if (in == null) {
      throw new IllegalArgumentException("in must not be null");
    }
    super(in);
  }

  /** Selects merges before the wrapped policy. Null clears the slot. */
  public ProactiveMergePolicy setBeforeMergeSelector(MergeSelector selector) {
    this.beforeMergeSelector = selector;
    return this;
  }

  /** Selects merges after the wrapped policy. Null clears the slot. */
  public ProactiveMergePolicy setAfterMergeSelector(MergeSelector selector) {
    this.afterMergeSelector = selector;
    return this;
  }

  @Override
  public SizeUnit getSizeUnit() {
    return in.getSizeUnit();
  }

  @Override
  public String toString() {
    return "ProactiveMergePolicy(in="
        + in
        + ", before="
        + beforeMergeSelector
        + ", after="
        + afterMergeSelector
        + ")";
  }

  @Override
  public MergeSpecification findMerges(
      MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext)
      throws IOException {
    if (beforeMergeSelector == null && afterMergeSelector == null) {
      return super.findMerges(mergeTrigger, segmentInfos, mergeContext);
    }
    LinkedHashSet<SegmentCommitInfo> claimed = new LinkedHashSet<>();
    MergeContext withClaims = new ClaimedSegmentsMergeContext(mergeContext, claimed);
    MergeSpecification beforeSpec =
        selectorMerges(beforeMergeSelector, mergeTrigger, segmentInfos, withClaims, claimed);
    MergeSpecification stock = in.findMerges(mergeTrigger, segmentInfos, withClaims);
    reserve(stock, claimed);
    MergeSpecification afterSpec =
        selectorMerges(afterMergeSelector, mergeTrigger, segmentInfos, withClaims, claimed);
    return combine(beforeSpec, stock, afterSpec);
  }

  private MergeSpecification selectorMerges(
      MergeSelector selector,
      MergeTrigger trigger,
      SegmentInfos infos,
      MergeContext context,
      Set<SegmentCommitInfo> claimed)
      throws IOException {
    if (selector == null) {
      return null;
    }
    List<List<SegmentCommitInfo>> groups = selector.select(trigger, infos, context);
    if (groups == null || groups.isEmpty()) {
      return null;
    }
    Set<SegmentCommitInfo> present = new HashSet<>();
    for (SegmentCommitInfo info : infos) {
      present.add(info);
    }
    MergeSpecification spec = null;
    for (List<SegmentCommitInfo> group : groups) {
      if (accept(group, present, context) == false) {
        continue;
      }
      if (spec == null) {
        spec = new MergeSpecification();
      }
      spec.add(new OneMerge(group));
      if (verbose(context)) {
        message("add selector merge=" + segString(context, group), context);
      }
      claimed.addAll(group);
    }
    return spec;
  }

  private boolean accept(
      List<SegmentCommitInfo> group, Set<SegmentCommitInfo> present, MergeContext context) {
    // MERGE_FINISHED calls findMerges from IndexWriter.merge, and an exception there is tragic.
    // Assertions still fail the tests. With them off, a bad group is skipped like registerMerge.
    if (group == null || group.isEmpty()) {
      assert false : "selector returned a null or empty group";
      skip(context, "empty", "selector returned a null or empty group");
      return false;
    }
    String names = names(group);
    Set<SegmentCommitInfo> seen = new HashSet<>();
    for (SegmentCommitInfo info : group) {
      if (info == null) {
        assert false : "selector group contains a null segment: [" + names + "]";
        skip(context, names, "selector group contains a null segment");
        return false;
      }
      if (seen.add(info) == false) {
        assert false : "selector group repeats segment " + info.info.name + ": [" + names + "]";
        skip(context, names, "segment " + info.info.name + " is repeated");
        return false;
      }
    }
    Set<SegmentCommitInfo> merging = context.getMergingSegments();
    for (SegmentCommitInfo info : group) {
      if (present.contains(info) == false) {
        skip(context, names, "segment " + info.info.name + " is not in the index");
        return false;
      }
      if (merging.contains(info)) {
        skip(context, names, "segment " + info.info.name + " is already merging");
        return false;
      }
    }
    return true;
  }

  private void skip(MergeContext context, String names, String reason) {
    if (verbose(context)) {
      message("skip selector group [" + names + "]: " + reason, context);
    }
  }

  private static String names(List<SegmentCommitInfo> group) {
    StringBuilder names = new StringBuilder();
    for (int i = 0; i < group.size(); i++) {
      if (i > 0) {
        names.append(' ');
      }
      SegmentCommitInfo info = group.get(i);
      names.append(info == null ? "null" : info.info.name);
    }
    return names.toString();
  }

  private static void reserve(MergeSpecification spec, Set<SegmentCommitInfo> claimed) {
    if (spec == null) {
      return;
    }
    for (OneMerge merge : spec.merges) {
      claimed.addAll(merge.segments);
    }
  }

  private static MergeSpecification combine(
      MergeSpecification before, MergeSpecification stock, MergeSpecification after) {
    if (before == null && after == null) {
      return stock;
    }
    MergeSpecification spec = new MergeSpecification();
    append(spec, before);
    append(spec, stock);
    append(spec, after);
    if (spec.merges.isEmpty()) {
      return null;
    }
    return spec;
  }

  private static void append(MergeSpecification spec, MergeSpecification from) {
    if (from == null) {
      return;
    }
    for (OneMerge merge : from.merges) {
      spec.add(merge);
    }
  }

  /** Claims are visible to everything that runs later in the same call. */
  private static final class ClaimedSegmentsMergeContext implements MergeContext {
    private final MergeContext in;
    private final Set<SegmentCommitInfo> claimed;

    ClaimedSegmentsMergeContext(MergeContext in, Set<SegmentCommitInfo> claimed) {
      this.in = in;
      this.claimed = claimed;
    }

    @Override
    public int numDeletesToMerge(SegmentCommitInfo info) throws IOException {
      return in.numDeletesToMerge(info);
    }

    @Override
    public int numDeletedDocs(SegmentCommitInfo info) {
      return in.numDeletedDocs(info);
    }

    @Override
    public InfoStream getInfoStream() {
      return in.getInfoStream();
    }

    @Override
    public Set<SegmentCommitInfo> getMergingSegments() {
      if (claimed.isEmpty()) {
        return in.getMergingSegments();
      }
      LinkedHashSet<SegmentCommitInfo> merging =
          new LinkedHashSet<>(in.getMergingSegments().size() + claimed.size());
      merging.addAll(in.getMergingSegments());
      merging.addAll(claimed);
      return Collections.unmodifiableSet(merging);
    }
  }
}
