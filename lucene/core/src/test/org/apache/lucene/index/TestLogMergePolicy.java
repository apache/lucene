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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.index.MergePolicy.MergeContext;
import org.apache.lucene.index.MergePolicy.MergeSpecification;
import org.apache.lucene.index.MergePolicy.OneMerge;
import org.apache.lucene.tests.index.BaseMergePolicyTestCase;
import org.apache.lucene.util.Version;

public class TestLogMergePolicy extends BaseMergePolicyTestCase {

  @Override
  public MergePolicy mergePolicy() {
    return newLogMergePolicy(random());
  }

  public void testDefaultForcedMergeMB() {
    LogByteSizeMergePolicy mp = new LogByteSizeMergePolicy();
    assertTrue(mp.getMaxMergeMBForForcedMerge() > 0.0);
  }

  @Override
  protected void assertSegmentInfos(MergePolicy policy, SegmentInfos infos) throws IOException {
    LogMergePolicy mp = (LogMergePolicy) policy;

    MergeContext mockMergeContext = new MockMergeContext(SegmentCommitInfo::getDelCount);
    for (SegmentCommitInfo info : infos) {
      assertTrue(mp.size(info, mockMergeContext) / mp.getMergeFactor() < mp.maxMergeSize);
    }
  }

  @Override
  protected void assertMerge(MergePolicy policy, MergeSpecification merge) throws IOException {
    LogMergePolicy lmp = (LogMergePolicy) policy;
    for (OneMerge oneMerge : merge.merges) {
      assertEquals(lmp.getMergeFactor(), oneMerge.segments.size());
    }
  }

  public void testIgnoreLargeSegments() throws IOException {
    LogDocMergePolicy mergePolicy = new LogDocMergePolicy();
    IOStats stats = new IOStats();
    mergePolicy.setMaxMergeDocs(10_000);
    AtomicLong segNameGenerator = new AtomicLong();
    MergeContext mergeContext = new MockMergeContext(SegmentCommitInfo::getDelCount);
    SegmentInfos segmentInfos = new SegmentInfos(Version.LATEST.major);
    // 1 segment that reached the maximum segment size
    segmentInfos.add(
        makeSegmentCommitInfo(
            "_" + segNameGenerator.getAndIncrement(), 11_000, 0, 0, IndexWriter.SOURCE_MERGE));
    // and 10 segments below the max segment size, but within the same level
    for (int i = 0; i < 10; ++i) {
      segmentInfos.add(
          makeSegmentCommitInfo(
              "_" + segNameGenerator.getAndIncrement(), 3_000, 0, 0, IndexWriter.SOURCE_MERGE));
    }
    // LogMergePolicy used to have a bug that would make it exclude the first mergeFactor segments
    // from merging if any of them was above the maximum merged size
    MergeSpecification spec =
        mergePolicy.findMerges(MergeTrigger.EXPLICIT, segmentInfos, mergeContext);
    assertNotNull(spec);
    for (OneMerge oneMerge : spec.merges) {
      segmentInfos =
          applyMerge(segmentInfos, oneMerge, "_" + segNameGenerator.getAndIncrement(), stats);
    }
    assertEquals(2, segmentInfos.size());
    assertEquals(11_000, segmentInfos.info(0).info.maxDoc());
    assertEquals(30_000, segmentInfos.info(1).info.maxDoc());
  }

  public void testIncreasingSegmentSizes() throws IOException {
    LogDocMergePolicy mergePolicy = new LogDocMergePolicy();
    IOStats stats = new IOStats();
    AtomicLong segNameGenerator = new AtomicLong();
    MergeContext mergeContext = new MockMergeContext(SegmentCommitInfo::getDelCount);
    SegmentInfos segmentInfos = new SegmentInfos(Version.LATEST.major);
    // 11 segments of increasing sizes
    for (int i = 0; i < 11; ++i) {
      segmentInfos.add(
          makeSegmentCommitInfo(
              "_" + segNameGenerator.getAndIncrement(),
              (i + 1) * 1000,
              0,
              0,
              IndexWriter.SOURCE_MERGE));
    }
    MergeSpecification spec =
        mergePolicy.findMerges(MergeTrigger.EXPLICIT, segmentInfos, mergeContext);
    assertNotNull(spec);
    for (OneMerge oneMerge : spec.merges) {
      segmentInfos =
          applyMerge(segmentInfos, oneMerge, "_" + segNameGenerator.getAndIncrement(), stats);
    }
    assertEquals(2, segmentInfos.size());
    assertEquals(55_000, segmentInfos.info(0).info.maxDoc());
    assertEquals(11_000, segmentInfos.info(1).info.maxDoc());
  }

  public void testSmallMiddleSegment() throws IOException {
    LogDocMergePolicy mergePolicy = new LogDocMergePolicy();
    IOStats stats = new IOStats();
    AtomicLong segNameGenerator = new AtomicLong();
    MergeContext mergeContext = new MockMergeContext(SegmentCommitInfo::getDelCount);
    SegmentInfos segmentInfos = new SegmentInfos(Version.LATEST.major);
    // 5 big segments
    for (int i = 0; i < 5; ++i) {
      segmentInfos.add(
          makeSegmentCommitInfo(
              "_" + segNameGenerator.getAndIncrement(), 10_000, 0, 0, IndexWriter.SOURCE_MERGE));
    }
    // 1 segment on a lower tier
    segmentInfos.add(
        makeSegmentCommitInfo(
            "_" + segNameGenerator.getAndIncrement(), 1_000, 0, 0, IndexWriter.SOURCE_MERGE));
    // 5 big segments again
    for (int i = 0; i < 5; ++i) {
      segmentInfos.add(
          makeSegmentCommitInfo(
              "_" + segNameGenerator.getAndIncrement(), 10_000, 0, 0, IndexWriter.SOURCE_MERGE));
    }
    // Ensure that having a small segment in the middle doesn't prevent merging
    MergeSpecification spec =
        mergePolicy.findMerges(MergeTrigger.EXPLICIT, segmentInfos, mergeContext);
    assertNotNull(spec);
    for (OneMerge oneMerge : spec.merges) {
      segmentInfos =
          applyMerge(segmentInfos, oneMerge, "_" + segNameGenerator.getAndIncrement(), stats);
    }
    assertEquals(2, segmentInfos.size());
    assertEquals(91_000, segmentInfos.info(0).info.maxDoc());
    assertEquals(10_000, segmentInfos.info(1).info.maxDoc());
  }

  public void testRejectUnbalancedMerges() throws IOException {
    LogDocMergePolicy mergePolicy = new LogDocMergePolicy();
    mergePolicy.setMinMergeDocs(10_000);
    IOStats stats = new IOStats();
    AtomicLong segNameGenerator = new AtomicLong();
    MergeContext mergeContext = new MockMergeContext(SegmentCommitInfo::getDelCount);
    SegmentInfos segmentInfos = new SegmentInfos(Version.LATEST.major);
    // 1 100-docs segment
    segmentInfos.add(
        makeSegmentCommitInfo(
            "_" + segNameGenerator.getAndIncrement(), 100, 0, 0, IndexWriter.SOURCE_MERGE));
    // 9 1-doc segments again
    for (int i = 0; i < 9; ++i) {
      segmentInfos.add(
          makeSegmentCommitInfo(
              "_" + segNameGenerator.getAndIncrement(), 1, 0, 0, IndexWriter.SOURCE_FLUSH));
    }
    // Ensure though we're below the floor size, the merge would be too unbalanced
    MergeSpecification spec =
        mergePolicy.findMerges(MergeTrigger.EXPLICIT, segmentInfos, mergeContext);
    assertNull(spec);

    // another 1-doc segment, now we can merge 10 1-doc segments
    segmentInfos.add(
        makeSegmentCommitInfo(
            "_" + segNameGenerator.getAndIncrement(), 1, 0, 0, IndexWriter.SOURCE_FLUSH));
    spec = mergePolicy.findMerges(MergeTrigger.EXPLICIT, segmentInfos, mergeContext);
    assertNotNull(spec);
    for (OneMerge oneMerge : spec.merges) {
      segmentInfos =
          applyMerge(segmentInfos, oneMerge, "_" + segNameGenerator.getAndIncrement(), stats);
    }
    assertEquals(2, segmentInfos.size());
    assertEquals(100, segmentInfos.info(0).info.maxDoc());
    assertEquals(10, segmentInfos.info(1).info.maxDoc());
  }
}
