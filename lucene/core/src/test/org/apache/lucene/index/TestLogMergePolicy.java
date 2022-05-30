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
    // TODO
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
              "_" + segNameGenerator.getAndIncrement(), 5_000, 0, 0, IndexWriter.SOURCE_MERGE));
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
    assertEquals(50_000, segmentInfos.info(1).info.maxDoc());
  }

}
