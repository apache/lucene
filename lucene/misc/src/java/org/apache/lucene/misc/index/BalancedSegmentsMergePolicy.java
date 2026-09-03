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
package org.apache.lucene.misc.index;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FilterMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.util.Bits;

/**
 * Makes {@link org.apache.lucene.index.IndexWriter#forceMerge(int)} leave segments holding about
 * the same number of documents.
 *
 * <p>{@code forceMerge(n)} says how many segments to leave but nothing about how the documents are
 * shared between them, and in practice the share is uneven: merging a hundred equal segments of a
 * thousand documents into four leaves them holding 97,000, 1,000, 1,000 and 1,000. Since a search
 * is parallelised by segment, a query then waits on a segment holding 97% of the index. This policy
 * leaves four segments of 25,000.
 *
 * <pre class="prettyprint">
 *   iwc.setMergePolicy(new BalancedSegmentsMergePolicy(new TieredMergePolicy()));
 *   ...
 *   writer.forceMerge(numberOfSearchThreads);
 * </pre>
 *
 * <p>The whole forced merge is planned at once: the segments are packed into groups and each group
 * is given the number of outputs its size deserves, so the work is several independent merges the
 * scheduler can run concurrently rather than one merge of everything. A group that is already a
 * single segment of about the right size is left alone, so a nearly balanced index is balanced
 * without being rewritten. The number of outputs is per merge rather than global, so a merge may
 * also have a single input: {@code forceMerge(16)} on a one-segment index splits it into sixteen.
 *
 * <p>Only forced merges are affected; ordinary merging is left to the wrapped policy. No index sort
 * is needed, since documents are shared out by position rather than by any key. A policy that
 * placed its boundaries on a key instead would give each output a range of that key, which this one
 * does not attempt.
 *
 * @lucene.experimental
 */
public class BalancedSegmentsMergePolicy extends FilterMergePolicy {

  /** Wraps {@link TieredMergePolicy}. */
  public BalancedSegmentsMergePolicy() {
    this(new TieredMergePolicy());
  }

  /**
   * @param in the policy that decides ordinary merges; only forced merges are changed
   */
  public BalancedSegmentsMergePolicy(MergePolicy in) {
    super(in);
  }

  @Override
  public MergeSpecification findForcedMerges(
      SegmentInfos infos,
      int maxSegmentCount,
      Map<SegmentCommitInfo, Boolean> segmentsToMerge,
      MergeContext context)
      throws IOException {
    if (maxSegmentCount == Integer.MAX_VALUE || maxSegmentCount < 1) {
      // Not a request for a particular number of segments, so there is nothing to balance.
      return super.findForcedMerges(infos, maxSegmentCount, segmentsToMerge, context);
    }
    final List<SegmentCommitInfo> eligible = new ArrayList<>();
    for (SegmentCommitInfo info : infos) {
      if (segmentsToMerge.containsKey(info) == false) {
        continue;
      }
      if (context.getMergingSegments().contains(info)) {
        // A plan is already running. Since a plan is made for the whole index at once, planning
        // again over what is left of it would give the outputs still being written a second share
        // of the segment count, so wait for it to finish instead.
        return null;
      }
      eligible.add(info);
    }
    if (eligible.isEmpty()) {
      return null;
    }
    long total = 0;
    for (SegmentCommitInfo info : eligible) {
      total += liveDocs(info, context);
    }
    if (total == 0) {
      return super.findForcedMerges(infos, maxSegmentCount, segmentsToMerge, context);
    }
    // Largest first, so a segment bigger than a share of its own takes as many outputs as it needs
    // and the small ones pack into what is left.
    eligible.sort(
        Comparator.comparingLong((SegmentCommitInfo info) -> liveDocsUnchecked(info, context))
            .reversed());

    final double share = (double) total / maxSegmentCount;
    final MergeSpecification spec = new MergeSpecification();
    final List<SegmentCommitInfo> group = new ArrayList<>();
    long groupDocs = 0;
    int assigned = 0;
    for (int i = 0; i < eligible.size(); i++) {
      group.add(eligible.get(i));
      groupDocs += liveDocs(eligible.get(i), context);
      final int left = eligible.size() - i - 1;
      if (left > 0) {
        final double withNext = groupDocs + liveDocs(eligible.get(i + 1), context);
        if (Math.abs(withNext - share) < Math.abs(groupDocs - share)) {
          continue; // taking the next segment too gets this group closer to a share
        }
      }
      // What this group has earned, less one output for every group still to be formed. Each of
      // those needs at least one segment, so at most `left` of them remain.
      final int outputs =
          left == 0
              ? maxSegmentCount - assigned
              : Math.min((int) Math.round(groupDocs / share), maxSegmentCount - assigned - 1);
      if (outputs < 1) {
        continue; // no output left to give it; it joins the next group
      }
      if (group.size() > 1 || outputs > 1 || isMerged(infos, group.get(0), context) == false) {
        // A lone segment that is already the right size and carries nothing to reclaim needs no
        // merge at all.
        spec.add(new Split(new ArrayList<>(group), outputs));
      }
      assigned += outputs;
      group.clear();
      groupDocs = 0;
    }
    return spec.merges.isEmpty() ? null : spec;
  }

  /**
   * What this segment will contribute to an output. {@link MergeContext#numDeletesToMerge} rather
   * than the recorded delete count, so that deletes still buffered, and any a policy has decided to
   * reclaim, are shared out with everything else rather than turning up as an imbalance later.
   */
  private static long liveDocs(SegmentCommitInfo info, MergeContext context) throws IOException {
    return info.info.maxDoc() - context.numDeletesToMerge(info);
  }

  /** {@link #liveDocs} for a comparator, which cannot throw. */
  private static long liveDocsUnchecked(SegmentCommitInfo info, MergeContext context) {
    try {
      return liveDocs(info, context);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** One merge writing {@code outputs} segments, each an equal share of every input. */
  private static class Split extends OneMerge {
    private final int outputs;

    Split(List<SegmentCommitInfo> segments, int outputs) {
      super(segments);
      this.outputs = outputs;
    }

    @Override
    public boolean isPartitioned() {
      return true;
    }

    @Override
    public int[][] getDocRangePartitions(List<CodecReader> readers) throws IOException {
      final int[][] boundaries = new int[readers.size()][];
      for (int i = 0; i < readers.size(); i++) {
        boundaries[i] = boundaries(readers.get(i), outputs);
      }
      return boundaries;
    }

    /**
     * Cuts one input into {@code outputs} ranges holding the same number of live documents. Every
     * output takes the same fraction of every input, so the outputs end up the same size whatever
     * the inputs looked like, and deleted documents -- which the merge will drop -- do not count
     * towards a share.
     */
    private static int[] boundaries(CodecReader reader, int outputs) throws IOException {
      final int maxDoc = reader.maxDoc();
      final Bits liveDocs = reader.getLiveDocs();
      final int[] boundaries = new int[outputs + 1];
      boundaries[outputs] = maxDoc;
      final int live = reader.numDocs();
      int seen = 0;
      int next = 1;
      for (int doc = 0; doc < maxDoc && next < outputs; doc++) {
        if (liveDocs == null || liveDocs.get(doc)) {
          seen++;
          while (next < outputs && (long) seen * outputs >= (long) live * next) {
            boundaries[next++] = doc + 1;
          }
        }
      }
      while (next < outputs) {
        boundaries[next++] = maxDoc;
      }
      return alignToBlocks(reader, boundaries);
    }

    /**
     * Moves each boundary to the end of the block it falls in, since a merge may not put a block's
     * children in a different segment from their parent. A parent field's numeric doc values hold a
     * value on the last document of each block, so advancing it lands on a block end.
     */
    private static int[] alignToBlocks(CodecReader reader, int[] boundaries) throws IOException {
      final String parentField = reader.getFieldInfos().getParentField();
      if (parentField == null) {
        return boundaries;
      }
      final NumericDocValues parents = reader.getNumericDocValues(parentField);
      if (parents == null) {
        return boundaries;
      }
      for (int o = 1; o < boundaries.length - 1; o++) {
        final int boundary = boundaries[o];
        if (boundary <= boundaries[o - 1] || boundary >= reader.maxDoc()) {
          boundaries[o] = Math.max(boundaries[o - 1], Math.min(boundary, reader.maxDoc()));
          continue;
        }
        final int blockEnd = parents.advance(boundary - 1);
        boundaries[o] = blockEnd == NumericDocValues.NO_MORE_DOCS ? reader.maxDoc() : blockEnd + 1;
      }
      return boundaries;
    }
  }
}
