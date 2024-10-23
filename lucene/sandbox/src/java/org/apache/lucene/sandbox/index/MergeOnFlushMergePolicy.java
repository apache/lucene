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
import java.util.List;
import org.apache.lucene.index.FilterMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;

/**
 * A simple extension to wrap {@link MergePolicy} to merge all tiny segments (or at least segments
 * smaller than specified in {@link #setSmallSegmentThresholdMB(double)} into one segment on commit.
 */
public class MergeOnFlushMergePolicy extends FilterMergePolicy {
  private long smallSegmentThresholdBytes = Units.mbToBytes(100.0);

  /**
   * Creates a MergeOnFlushMergePolicy merge policy instance wrapping another.
   *
   * @param mergePolicy the wrapped {@link MergePolicy}
   */
  public MergeOnFlushMergePolicy(MergePolicy mergePolicy) {
    super(mergePolicy);
  }

  public double getSmallSegmentThresholdMB() {
    return Units.bytesToMB(smallSegmentThresholdBytes);
  }

  /**
   * @param smallSegmentThresholdMB all segments smaller than this will be merged into a single
   *     segment before commit completes.
   */
  public void setSmallSegmentThresholdMB(double smallSegmentThresholdMB) {
    this.smallSegmentThresholdBytes = Units.mbToBytes(smallSegmentThresholdMB);
  }

  @Override
  public MergeSpecification findFullFlushMerges(
      MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext)
      throws IOException {
    List<SegmentCommitInfo> smallSegments = new ArrayList<>();
    for (SegmentCommitInfo sci : segmentInfos) {
      if (sci.sizeInBytes() < smallSegmentThresholdBytes) {
        if (mergeContext.getMergingSegments().contains(sci) == false) {
          smallSegments.add(sci);
        }
      }
    }
    if (smallSegments.size() > 1) {
      MergeSpecification mergeSpecification = new MergeSpecification();
      mergeSpecification.add(new OneMerge(smallSegments));
      return mergeSpecification;
    }
    return null;
  }

  /** Utility class to handle conversion between megabytes and bytes */
  static class Units {

    private Units() {}

    public static double bytesToMB(long bytes) {
      return bytes / 1024. / 1024.;
    }

    public static long mbToBytes(double megabytes) {
      return (long) (megabytes * 1024 * 1024);
    }
  }
}
