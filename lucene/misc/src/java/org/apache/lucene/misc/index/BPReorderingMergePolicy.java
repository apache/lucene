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
import java.util.Collections;
import java.util.Map;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FilterMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.misc.index.BPIndexReorderer.NotEnoughRAMException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.SetOnce;

/**
 * A merge policy that reorders merged segments according to a {@link BPIndexReorderer}. When
 * reordering doesn't have enough RAM, it simply skips reordering in order not to fail the merge. So
 * make sure to give enough RAM to your {@link BPIndexReorderer} via {@link
 * BPIndexReorderer#setRAMBudgetMB(double)}.
 */
public final class BPReorderingMergePolicy extends FilterMergePolicy {

  /** Whether a segment has been reordered. */
  static final String REORDERED = "bp.reordered";

  private final BPIndexReorderer reorderer;
  private int minNaturalMergeNumDocs = 1;
  private float minNaturalMergeRatioFromBiggestSegment = 0f;

  /**
   * Sole constructor. It takes the merge policy that should be used to compute merges, and will
   * then reorder doc IDs from all merges above the configured minimum doc count, as well as all
   * forced merges.
   *
   * <p>If you wish to only run reordering upon forced merges, pass {@link Integer#MAX_VALUE} as a
   * {@code minNaturalMergeNumDocs}. Otherwise a default value of {@code 2^18 = 262,144} is
   * suggested. This should help retain merging optimizations on small merges while reordering the
   * larger segments that are important for good search performance.
   *
   * @param in the merge policy to use to compute merges
   * @param reorderer the {@link BPIndexReorderer} to use to renumber doc IDs
   */
  public BPReorderingMergePolicy(MergePolicy in, BPIndexReorderer reorderer) {
    super(in);
    this.reorderer = reorderer;
  }

  /**
   * Set the minimum number of docs that a merge must have for the resulting segment to be
   * reordered.
   */
  public void setMinNaturalMergeNumDocs(int minNaturalMergeNumDocs) {
    if (minNaturalMergeNumDocs < 1) {
      throw new IllegalArgumentException(
          "minNaturalMergeNumDocs must be at least 1, got " + minNaturalMergeNumDocs);
    }
    this.minNaturalMergeNumDocs = minNaturalMergeNumDocs;
  }

  /**
   * Set the minimum number of docs that a merge must have for the resulting segment to be
   * reordered, as a ratio of the total number of documents of the current biggest segment in the
   * index. This parameter helps only enable reordering on segments that are large enough that they
   * will significantly contribute to overall search performance.
   */
  public void setMinNaturalMergeRatioFromBiggestSegment(
      float minNaturalMergeRatioFromBiggestSegment) {
    if (minNaturalMergeRatioFromBiggestSegment >= 0 == false
        || minNaturalMergeRatioFromBiggestSegment < 1 == false) {
      throw new IllegalArgumentException(
          "minNaturalMergeRatioFromBiggestSegment must be in [0, 1), got "
              + minNaturalMergeRatioFromBiggestSegment);
    }
    this.minNaturalMergeRatioFromBiggestSegment = minNaturalMergeRatioFromBiggestSegment;
  }

  private MergeSpecification maybeReorder(
      MergeSpecification spec, boolean forced, SegmentInfos infos) {
    if (spec == null) {
      return null;
    }

    final int minNumDocs;
    if (forced) {
      // No minimum size for forced merges
      minNumDocs = 1;
    } else {
      int maxMaxDoc = 0;
      if (infos != null) {
        for (SegmentCommitInfo sci : infos) {
          maxMaxDoc = Math.max(sci.info.maxDoc(), maxMaxDoc);
        }
      }
      minNumDocs =
          Math.max(
              this.minNaturalMergeNumDocs,
              (int) ((double) minNaturalMergeRatioFromBiggestSegment * maxMaxDoc));
    }

    MergeSpecification newSpec = new MergeSpecification();
    for (OneMerge oneMerge : spec.merges) {

      newSpec.add(
          new OneMerge(oneMerge) {

            private final SetOnce<Boolean> reordered = new SetOnce<>();

            @Override
            public CodecReader wrapForMerge(CodecReader reader) throws IOException {
              return oneMerge.wrapForMerge(reader);
            }

            @Override
            public Sorter.DocMap reorder(CodecReader reader, Directory dir) throws IOException {
              Sorter.DocMap docMap = null;
              if (reader.numDocs() >= minNumDocs) {
                try {
                  docMap = reorderer.computeDocMap(reader, dir);
                } catch (
                    @SuppressWarnings("unused")
                    NotEnoughRAMException e) {
                  // skip reordering, we don't have enough RAM anyway
                }
              }
              reordered.set(docMap != null);
              return docMap;
            }

            @Override
            public void setMergeInfo(SegmentCommitInfo info) {
              Boolean reordered = this.reordered.get();
              if (reordered == null) {
                // reordering was not called, likely because an index sort is configured
                reordered = false;
              }
              info.info.addDiagnostics(
                  Collections.singletonMap(REORDERED, Boolean.toString(reordered)));
              super.setMergeInfo(info);
            }
          });
    }
    return newSpec;
  }

  @Override
  public MergeSpecification findMerges(
      MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext)
      throws IOException {
    return maybeReorder(
        super.findMerges(mergeTrigger, segmentInfos, mergeContext), false, segmentInfos);
  }

  @Override
  public MergeSpecification findForcedMerges(
      SegmentInfos segmentInfos,
      int maxSegmentCount,
      Map<SegmentCommitInfo, Boolean> segmentsToMerge,
      MergeContext mergeContext)
      throws IOException {
    return maybeReorder(
        super.findForcedMerges(segmentInfos, maxSegmentCount, segmentsToMerge, mergeContext),
        true,
        segmentInfos);
  }

  @Override
  public MergeSpecification findForcedDeletesMerges(
      SegmentInfos segmentInfos, MergeContext mergeContext) throws IOException {
    return maybeReorder(
        super.findForcedDeletesMerges(segmentInfos, mergeContext), true, segmentInfos);
  }

  @Override
  public MergeSpecification findFullFlushMerges(
      MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext)
      throws IOException {
    return maybeReorder(
        super.findFullFlushMerges(mergeTrigger, segmentInfos, mergeContext), false, segmentInfos);
  }

  @Override
  public MergeSpecification findMerges(CodecReader... readers) throws IOException {
    // addIndexes is considered a forced merge
    return maybeReorder(super.findMerges(readers), true, null);
  }
}
