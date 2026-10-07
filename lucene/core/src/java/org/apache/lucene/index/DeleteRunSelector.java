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
import java.util.List;
import java.util.Set;

/**
 * Merges adjacent segments whose delete percentage is at least a threshold.
 *
 * <p>{@link LogMergePolicy} waits for {@code mergeFactor} segments before it will merge a level, so
 * deletes can sit in a level that is not full yet. Install this after that policy. {@code
 * deletesPct} is the minimum delete percentage, greater than 0 and at most 100. {@code
 * maxMergeBytes} caps the sum of delete-prorated segment sizes in one run, the same live-byte size
 * {@link LogByteSizeMergePolicy} compares with {@code maxMergeMB} when delete calibration is on. A
 * run closes before the next segment would pass that cap. A segment larger than the cap is still
 * merged on its own, so its deletes are reclaimed. A segment that is already merging ends the
 * current run and is not a member.
 *
 * @lucene.experimental
 */
public final class DeleteRunSelector implements MergeSelector {

  private final double deletesPct;
  private final long maxMergeBytes;

  /** Builds a selector for runs at or above {@code deletesPct}, capped at {@code maxMergeBytes}. */
  public DeleteRunSelector(double deletesPct, long maxMergeBytes) {
    if (deletesPct <= 0 || deletesPct > 100) {
      throw new IllegalArgumentException("deletesPct must be in (0, 100], got " + deletesPct);
    }
    if (maxMergeBytes < 1) {
      throw new IllegalArgumentException("maxMergeBytes must be >= 1, got " + maxMergeBytes);
    }
    this.deletesPct = deletesPct;
    this.maxMergeBytes = maxMergeBytes;
  }

  @Override
  public String toString() {
    return "DeleteRunSelector(deletesPct=" + deletesPct + ", maxMergeBytes=" + maxMergeBytes + ")";
  }

  @Override
  public List<List<SegmentCommitInfo>> select(
      MergeTrigger trigger, SegmentInfos infos, MergePolicy.MergeContext context)
      throws IOException {
    Set<SegmentCommitInfo> merging = context.getMergingSegments();
    List<List<SegmentCommitInfo>> runs = new ArrayList<>();
    List<SegmentCommitInfo> run = null;
    long runBytes = 0;
    for (int i = 0; i < infos.size(); i++) {
      SegmentCommitInfo info = infos.info(i);
      if (merging.contains(info) || deletePct(info, context) < deletesPct) {
        run = close(runs, run);
        runBytes = 0;
        continue;
      }
      long segBytes = liveBytes(info, context);
      if (run != null && runBytes + segBytes > maxMergeBytes) {
        run = close(runs, run);
        runBytes = 0;
      }
      if (run == null) {
        run = new ArrayList<>();
      }
      run.add(info);
      runBytes += segBytes;
    }
    close(runs, run);
    return runs;
  }

  private static long liveBytes(SegmentCommitInfo info, MergePolicy.MergeContext context)
      throws IOException {
    long byteSize = info.sizeInBytes();
    int maxDoc = info.info.maxDoc();
    if (maxDoc <= 0) {
      return byteSize;
    }
    double delRatio = (double) context.numDeletesToMerge(info) / maxDoc;
    return (long) (byteSize * (1.0 - delRatio));
  }

  private static double deletePct(SegmentCommitInfo info, MergePolicy.MergeContext context)
      throws IOException {
    int maxDoc = info.info.maxDoc();
    if (maxDoc <= 0) {
      return 0;
    }
    return 100.0 * context.numDeletesToMerge(info) / maxDoc;
  }

  private static List<SegmentCommitInfo> close(
      List<List<SegmentCommitInfo>> runs, List<SegmentCommitInfo> run) {
    if (run != null) {
      runs.add(run);
    }
    return null;
  }
}
