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
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.tests.index.BaseMergePolicyTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Version;

/** Test for {@link MergeOnFlushMergePolicy}. */
public class TestMergeOnFlushMergePolicy extends BaseMergePolicyTestCase {

  @Override
  protected MergePolicy mergePolicy() {
    Random r = random();
    MergePolicy mergePolicy = newMergePolicy();
    MergeOnFlushMergePolicy mergeOnFlushPolicy = new MergeOnFlushMergePolicy(mergePolicy);
    mergeOnFlushPolicy.setMaxCFSSegmentSizeMB(mergePolicy.getMaxCFSSegmentSizeMB());
    mergeOnFlushPolicy.setNoCFSRatio(mergePolicy.getNoCFSRatio());
    mergeOnFlushPolicy.setSmallSegmentThresholdMB(TestUtil.nextInt(r, 1, 100));
    if (mergePolicy instanceof TieredMergePolicy) {
      ((TieredMergePolicy) mergePolicy)
          .setMaxMergedSegmentMB(TestUtil.nextInt(random(), 1024, 10 * 1024));
    }
    return mergeOnFlushPolicy;
  }

  @Override
  protected void assertSegmentInfos(MergePolicy policy, SegmentInfos infos) {}

  @Override
  protected void assertMerge(MergePolicy policy, MergePolicy.MergeSpecification merge) {}

  public void testFindFullFlushMerges() throws IOException {
    MergeOnFlushMergePolicy mergePolicy = (MergeOnFlushMergePolicy) mergePolicy();
    double smallSegmentThresholdMB = mergePolicy.getSmallSegmentThresholdMB();
    Random r = random();

    for (int j = 0; j < 10_000; j++) {
      SegmentInfos segmentInfos = new SegmentInfos(Version.LATEST.major);
      int numSegs = random().nextInt(50);
      Set<SegmentCommitInfo> mergingSegments = new HashSet<>();
      Set<SegmentCommitInfo> smallSegments = new HashSet<>();
      for (int i = 0; i < numSegs; i++) {
        SegmentCommitInfo sci =
            makeSegmentCommitInfo(
                "_" + i,
                TestUtil.nextInt(r, 10, 100),
                r.nextInt(10),
                r.nextDouble() * 2.0 * smallSegmentThresholdMB,
                IndexWriter.SOURCE_FLUSH);
        if (sci.sizeInBytes() < MergeOnFlushMergePolicy.Units.mbToBytes(smallSegmentThresholdMB)) {
          smallSegments.add(sci);
        }
        if (r.nextBoolean()) {
          mergingSegments.add(sci);
        }
        segmentInfos.add(sci);
      }
      MockMergeContext context = new MockMergeContext(SegmentCommitInfo::getDelCount);
      context.setMergingSegments(mergingSegments);
      MergePolicy.MergeSpecification mergeSpecification;

      mergeSpecification =
          mergePolicy.findFullFlushMerges(MergeTrigger.COMMIT, segmentInfos, context);

      if (mergeSpecification == null) {
        // If we didn't compute a merge, then we have at most one small segment not already
        // participating in a merge.
        boolean foundNonMergingSmallSegment = false;
        for (SegmentCommitInfo smallSegment : smallSegments) {
          if (!mergingSegments.contains(smallSegment)) {
            assertFalse(
                "If no merges, then at most one small segment is not already merging",
                foundNonMergingSmallSegment);
            foundNonMergingSmallSegment = true;
          }
        }
      } else {
        for (MergePolicy.OneMerge oneMerge : mergeSpecification.merges) {
          for (SegmentCommitInfo sci : oneMerge.segments) {
            assertTrue("Merges only contain small segments", smallSegments.contains(sci));
            assertFalse(
                "findFullFlushMerges must not return already merging segments",
                mergingSegments.contains(sci));
          }
        }
      }
    }
  }

  @Override
  public void testNoPathologicalMerges() throws IOException {
    // no-op: MergeOnFlushMergePolicy makes no effort to avoid O(n^2) merges
  }
}
