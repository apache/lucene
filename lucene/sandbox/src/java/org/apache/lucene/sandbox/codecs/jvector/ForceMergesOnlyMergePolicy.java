/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.jvector;

import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A merge policy that only merges segments if they are forced.
 * This is useful for testing and benchmarking purposes. Since it can be used for benchmarks, it is placed in the common
 * codec module.
 */
public class ForceMergesOnlyMergePolicy extends MergePolicy {
    private final boolean useCompoundFile;

    public ForceMergesOnlyMergePolicy() {
        this(false);
    }

    public ForceMergesOnlyMergePolicy(boolean useCompoundFile) {
        super();
        this.useCompoundFile = useCompoundFile;
    }

    @Override
    public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext)
        throws IOException {
        return null;
    }

    @Override
    public MergeSpecification findForcedMerges(
        SegmentInfos segmentInfos,
        int maxSegmentCount,
        Map<SegmentCommitInfo, Boolean> segmentsToMerge,
        MergeContext mergeContext
    ) throws IOException {
        // If the segments are already merged (e.g. there's only 1 segment), or
        // there are <maxNumSegments:.
        if (isMerged(segmentInfos, maxSegmentCount, segmentsToMerge, mergeContext)) {
            if (verbose(mergeContext)) {
                message("already merged; skip", mergeContext);
            }
            return null;
        }
        final List<SegmentCommitInfo> segments = segmentInfos.asList();
        MergeSpecification spec = new MergeSpecification();

        final OneMerge merge = new OneMerge(segments);
        spec.add(merge);
        return spec;
    }

    @Override
    public boolean useCompoundFile(SegmentInfos segmentInfos, SegmentCommitInfo newSegment, MergeContext mergeContext) throws IOException {
        return useCompoundFile;
    }

    @Override
    public MergeSpecification findForcedDeletesMerges(SegmentInfos segmentInfos, MergeContext mergeContext) throws IOException {
        return null;
    }

    /**
     * Returns true if the number of segments eligible for merging is less than or equal to the
     * specified {@code maxNumSegments}.
     */
    protected boolean isMerged(
        SegmentInfos infos,
        int maxNumSegments,
        Map<SegmentCommitInfo, Boolean> segmentsToMerge,
        MergeContext mergeContext
    ) throws IOException {
        final int numSegments = infos.size();
        int numToMerge = 0;
        SegmentCommitInfo mergeInfo = null;
        boolean segmentIsOriginal = false;
        for (int i = 0; i < numSegments && numToMerge <= maxNumSegments; i++) {
            final SegmentCommitInfo info = infos.info(i);
            final Boolean isOriginal = segmentsToMerge.get(info);
            if (isOriginal != null) {
                segmentIsOriginal = isOriginal;
                numToMerge++;
                mergeInfo = info;
            }
        }

        return numToMerge <= maxNumSegments && (numToMerge != 1 || !segmentIsOriginal || isMerged(infos, mergeInfo, mergeContext));
    }
}
