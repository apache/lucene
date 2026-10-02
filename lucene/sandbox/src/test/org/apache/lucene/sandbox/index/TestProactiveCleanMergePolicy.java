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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterMergePolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergePolicy.MergeSpecification;
import org.apache.lucene.index.MergePolicy.OneMerge;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.BaseMergePolicyTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Version;

/** Test for {@link ProactiveCleanMergePolicy}. */
public class TestProactiveCleanMergePolicy extends BaseMergePolicyTestCase {

  private static final double ABOVE_FLOOR_MB = 20.0;
  private static final double BELOW_FLOOR_MB = 1.0;
  private static final long ONE_MB = 1024L * 1024;

  @Override
  protected MergePolicy mergePolicy() {
    MergePolicy inner = newMergePolicy();
    if (inner instanceof TieredMergePolicy) {
      ((TieredMergePolicy) inner)
          .setMaxMergedSegmentMB(TestUtil.nextInt(random(), 1024, 10 * 1024));
    }
    return new ProactiveCleanMergePolicy(inner);
  }

  @Override
  protected void assertSegmentInfos(MergePolicy policy, SegmentInfos infos) {}

  @Override
  protected void assertMerge(MergePolicy policy, MergeSpecification merge) {}

  public void testThreeCleanAndOneDirtyFires() throws IOException {
    ProactiveCleanMergePolicy policy = new ProactiveCleanMergePolicy(noNaturalMerges());
    MergeSpecification spec =
        find(policy, infos(clean("_0"), clean("_1"), clean("_2"), dirty("_3")));
    assertNotNull(spec);
    assertEquals(1, spec.merges.size());
    assertEquals(List.of("_0", "_1", "_2"), names(spec.merges.get(0)));
  }

  public void testThreeCleanAndTwoDirtyLeavesDirtyToInner() throws IOException {
    RecordingMergePolicy inner = new RecordingMergePolicy(noNaturalMerges());
    ProactiveCleanMergePolicy policy = new ProactiveCleanMergePolicy(inner);
    SegmentCommitInfo d3 = dirty("_3");
    SegmentCommitInfo d4 = dirty("_4");
    MergeSpecification spec = find(policy, infos(clean("_0"), clean("_1"), clean("_2"), d3, d4));
    assertNotNull(spec);
    assertEquals(List.of("_0", "_1", "_2"), names(spec.merges.get(0)));
    assertNotNull(inner.lastInfos);
    assertEquals(Set.of("_3", "_4"), names(inner.lastInfos));
    assertSame(d3, inner.lastInfos.info(0));
    assertSame(d4, inner.lastInfos.info(1));
  }

  public void testThreeCleanAndTwoDirtyTmpMayMergeDirty() throws IOException {
    ProactiveCleanMergePolicy policy = new ProactiveCleanMergePolicy(new TieredMergePolicy());
    MergeSpecification spec =
        find(policy, infos(clean("_0"), clean("_1"), clean("_2"), dirty("_3"), dirty("_4")));
    assertNotNull(spec);
    assertEquals(List.of("_0", "_1", "_2"), names(spec.merges.get(0)));
  }

  public void testAllDirtyEqualsInner() throws IOException {
    TieredMergePolicy tmp = new TieredMergePolicy();
    ProactiveCleanMergePolicy policy = new ProactiveCleanMergePolicy(tmp);
    SegmentInfos segmentInfos = infos(dirty("_0"), dirty("_1"), dirty("_2"), dirty("_3"));
    MockMergeContext ctx = context();
    assertSameSpecification(
        tmp.findMerges(MergeTrigger.EXPLICIT, segmentInfos, ctx),
        policy.findMerges(MergeTrigger.EXPLICIT, segmentInfos, ctx));
  }

  public void testAppendOnlyEqualsInner() throws IOException {
    TieredMergePolicy tmp = new TieredMergePolicy();
    ProactiveCleanMergePolicy policy = new ProactiveCleanMergePolicy(tmp);
    SegmentInfos segmentInfos = infos(clean("_0"), clean("_1"), clean("_2"), clean("_3"));
    MockMergeContext ctx = context();
    assertSameSpecification(
        tmp.findMerges(MergeTrigger.EXPLICIT, segmentInfos, ctx),
        policy.findMerges(MergeTrigger.EXPLICIT, segmentInfos, ctx));
  }

  public void testAppendOnlyDoesNotFilterInfos() throws IOException {
    RecordingMergePolicy inner = new RecordingMergePolicy(new TieredMergePolicy());
    ProactiveCleanMergePolicy policy = new ProactiveCleanMergePolicy(inner);
    SegmentInfos segmentInfos = infos(clean("_0"), clean("_1"), clean("_2"));
    find(policy, segmentInfos);
    assertSame(segmentInfos, inner.lastInfos);
  }

  public void testBelowFloorSkipped() throws IOException {
    ProactiveCleanMergePolicy policy = new ProactiveCleanMergePolicy(noNaturalMerges());
    MergeSpecification spec =
        find(
            policy,
            infos(
                cleanBelowFloor("_0"), cleanBelowFloor("_1"), cleanBelowFloor("_2"), dirty("_3")));
    assertNull(spec);
  }

  public void testSizeTierTooSkewedWithDefaultMin() throws IOException {
    ProactiveCleanMergePolicy policy = new ProactiveCleanMergePolicy(noNaturalMerges());
    MergeSpecification spec =
        find(
            policy,
            infos(
                clean("_0", ABOVE_FLOOR_MB),
                clean("_1", ABOVE_FLOOR_MB * 1.2),
                clean("_2", ABOVE_FLOOR_MB * 5),
                dirty("_3")));
    assertNull(spec);
  }

  public void testCombinedSizeOverMaxBytes() throws IOException {
    ProactiveCleanMergePolicy policy = new ProactiveCleanMergePolicy(noNaturalMerges());
    policy.setMaxProactiveMergeBytes(30 * ONE_MB);
    assertNull(find(policy, infos(clean("_0"), clean("_1"), clean("_2"), dirty("_3"))));
  }

  public void testOneCleanAndDirtyDoesNotFire() throws IOException {
    ProactiveCleanMergePolicy policy = new ProactiveCleanMergePolicy(noNaturalMerges());
    assertNull(find(policy, infos(clean("_0"), dirty("_1"))));
  }

  public void testAlreadyMergingCleanExcluded() throws IOException {
    ProactiveCleanMergePolicy policy = new ProactiveCleanMergePolicy(noNaturalMerges());
    SegmentCommitInfo c0 = clean("_0");
    MockMergeContext ctx = context();
    ctx.setMergingSegments(Set.of(c0));
    assertNull(
        policy.findMerges(
            MergeTrigger.EXPLICIT, infos(c0, clean("_1"), clean("_2"), dirty("_3")), ctx));
  }

  public void testFindFullFlushMergesSkipsLargeClean() throws IOException {
    ProactiveCleanMergePolicy policy = new ProactiveCleanMergePolicy(noNaturalMerges());
    policy.setMinProactiveSegmentSize(ONE_MB);
    assertNull(
        policy.findFullFlushMerges(
            MergeTrigger.COMMIT,
            infos(clean("_0"), clean("_1"), clean("_2"), dirty("_3")),
            context()));
  }

  public void testFindFullFlushMergesTakesSmallAboveFloor() throws IOException {
    ProactiveCleanMergePolicy policy = new ProactiveCleanMergePolicy(noNaturalMerges());
    policy.setMinProactiveSegmentSize(ONE_MB);
    double smallMb = 5.0;
    MergeSpecification spec =
        policy.findFullFlushMerges(
            MergeTrigger.COMMIT,
            infos(
                clean("_0", smallMb),
                clean("_1", smallMb),
                clean("_2", smallMb),
                dirty("_3", smallMb)),
            context());
    assertNotNull(spec);
    assertEquals(1, spec.merges.size());
    assertEquals(List.of("_0", "_1", "_2"), names(spec.merges.get(0)));
  }

  public void testLeftoverSciIdentity() throws IOException {
    RecordingMergePolicy inner = new RecordingMergePolicy(noNaturalMerges());
    ProactiveCleanMergePolicy policy = new ProactiveCleanMergePolicy(inner);
    SegmentCommitInfo dirty = dirty("_3");
    find(policy, infos(clean("_0"), clean("_1"), clean("_2"), dirty));
    assertNotNull(inner.lastInfos);
    assertEquals(1, inner.lastInfos.size());
    assertSame(dirty, inner.lastInfos.info(0));
  }

  public void testTmpMixesCleanWithDirtyWhenOverBudget() throws IOException {
    TieredMergePolicy tmp = overBudgetTmp();
    MergeSpecification spec =
        tmp.findMerges(
            MergeTrigger.EXPLICIT,
            infos(clean("_0"), clean("_1"), clean("_2"), clean("_3"), clean("_4"), dirty40("_5")),
            context());
    assertNotNull("TMP should merge when segsPerTier=2 and 6 same-tier segs exist", spec);
    assertTrue(
        "TMP should pair a 0-delete seg with a dirty one when over budget", hasMixedMerge(spec));
  }

  public void testProactiveDoesNotMixPackableCleansWhenTmpWould() throws IOException {
    TieredMergePolicy tmp = overBudgetTmp();
    ProactiveCleanMergePolicy policy = new ProactiveCleanMergePolicy(tmp);
    MergeSpecification spec =
        find(
            policy,
            infos(clean("_0"), clean("_1"), clean("_2"), clean("_3"), clean("_4"), dirty40("_5")));
    assertNotNull(spec);
    assertEquals(List.of("_0", "_1", "_2", "_3", "_4"), names(spec.merges.get(0)));
    assertFalse(
        "proactive must not put packable 0-delete segs in a mixed merge", hasMixedMerge(spec));
  }

  public void testTmpMixesCleanWithOverFortyDirtyWhenOverBudget() throws IOException {
    TieredMergePolicy tmp = overBudgetTmp();
    MergeSpecification spec =
        tmp.findMerges(
            MergeTrigger.EXPLICIT,
            infos(
                clean("_0"),
                clean("_1"),
                clean("_2"),
                clean("_3"),
                clean("_4"),
                dirtyOverForty("_5")),
            context());
    assertNotNull("TMP should merge when segsPerTier=2 and 6 same-tier segs exist", spec);
    assertTrue(
        "TMP should pair a 0-delete seg with a >40%-deleted one when over budget",
        hasMixedMerge(spec));
  }

  public void testProactiveDoesNotMixWhenTmpWouldMixOverForty() throws IOException {
    TieredMergePolicy tmp = overBudgetTmp();
    ProactiveCleanMergePolicy policy = new ProactiveCleanMergePolicy(tmp);
    MergeSpecification spec =
        find(
            policy,
            infos(
                clean("_0"),
                clean("_1"),
                clean("_2"),
                clean("_3"),
                clean("_4"),
                dirtyOverForty("_5")));
    assertNotNull(spec);
    assertEquals(List.of("_0", "_1", "_2", "_3", "_4"), names(spec.merges.get(0)));
    assertFalse(
        "proactive must not put packable 0-delete segs in a mixed merge", hasMixedMerge(spec));
  }

  public void testForcedMergesEqualInner() throws IOException {
    TieredMergePolicy tmp = new TieredMergePolicy();
    ProactiveCleanMergePolicy policy = new ProactiveCleanMergePolicy(tmp);
    SegmentInfos segmentInfos = infos(clean("_0"), clean("_1"), dirty("_2"), dirty("_3"));
    Map<SegmentCommitInfo, Boolean> segmentsToMerge = new HashMap<>();
    for (SegmentCommitInfo sci : segmentInfos) {
      segmentsToMerge.put(sci, Boolean.TRUE);
    }
    MockMergeContext ctx = context();
    assertSameSpecification(
        tmp.findForcedMerges(segmentInfos, 1, segmentsToMerge, ctx),
        policy.findForcedMerges(segmentInfos, 1, segmentsToMerge, ctx));
  }

  public void testLiveAppendOnlyMatchesTmp() throws IOException {
    int maxBufferedDocs = 20;
    int numDocs = 200;
    List<Integer> tmpLayout = indexAndSnapshot(new TieredMergePolicy(), maxBufferedDocs, numDocs);
    List<Integer> proactiveLayout =
        indexAndSnapshot(
            new ProactiveCleanMergePolicy(new TieredMergePolicy()), maxBufferedDocs, numDocs);
    assertEquals(tmpLayout, proactiveLayout);
  }

  public void testLiveMixedProactiveTakesCleanGroup() throws IOException {
    int maxBufferedDocs = 10;
    ProactiveCleanMergePolicy proactive = new ProactiveCleanMergePolicy(new TieredMergePolicy());
    proactive.setMinProactiveSegmentSize(0);
    MixingRecorder recorder = new MixingRecorder(proactive);
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = liveWriterConfig(recorder, maxBufferedDocs);
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < 40; i++) {
          w.addDocument(idDoc(i));
        }
        // Partial deletes so the dirty flush stays live (100% deleted segs are dropped).
        for (int i = 0; i < 5; i++) {
          w.deleteDocuments(new Term("id", Integer.toString(i)));
        }
        for (int i = 40; i < 70; i++) {
          w.addDocument(idDoc(i));
        }
        w.flush();
        w.maybeMerge();
      }
    }
    assertFalse(
        "proactive merge must not mix 0-delete segs into dirty ones when a legal group exists",
        recorder.sawMixedMergeWhileCleanGroupExisted);
    assertTrue(
        "expected a clean proactive group once deletes mixed the index",
        recorder.sawCleanProactiveGroup);
  }

  private List<Integer> indexAndSnapshot(MergePolicy mergePolicy, int maxBufferedDocs, int numDocs)
      throws IOException {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = liveWriterConfig(mergePolicy, maxBufferedDocs);
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < numDocs; i++) {
          w.addDocument(new Document());
        }
      }
      return segmentMaxDocs(dir);
    }
  }

  private IndexWriterConfig liveWriterConfig(MergePolicy mergePolicy, int maxBufferedDocs) {
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMergePolicy(mergePolicy);
    iwc.setMergeScheduler(new SerialMergeScheduler());
    iwc.setMaxBufferedDocs(maxBufferedDocs);
    iwc.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    iwc.setCodec(TestUtil.getDefaultCodec());
    iwc.setUseCompoundFile(false);
    return iwc;
  }

  private static Document idDoc(int id) {
    Document doc = new Document();
    doc.add(new StringField("id", Integer.toString(id), Field.Store.NO));
    return doc;
  }

  private static List<Integer> segmentMaxDocs(Directory dir) throws IOException {
    try (DirectoryReader reader = DirectoryReader.open(dir)) {
      List<Integer> maxDocs = new ArrayList<>();
      for (LeafReaderContext leaf : reader.leaves()) {
        maxDocs.add(leaf.reader().maxDoc());
      }
      Collections.sort(maxDocs);
      return maxDocs;
    }
  }

  private MergeSpecification find(ProactiveCleanMergePolicy policy, SegmentInfos infos)
      throws IOException {
    return policy.findMerges(MergeTrigger.EXPLICIT, infos, context());
  }

  private static MockMergeContext context() {
    return new MockMergeContext(SegmentCommitInfo::getDelCount);
  }

  private SegmentCommitInfo clean(String name) {
    return clean(name, ABOVE_FLOOR_MB);
  }

  private SegmentCommitInfo clean(String name, double sizeMB) {
    return makeSegmentCommitInfo(name, 1000, 0, sizeMB, IndexWriter.SOURCE_FLUSH);
  }

  private SegmentCommitInfo cleanBelowFloor(String name) {
    return makeSegmentCommitInfo(name, 1000, 0, BELOW_FLOOR_MB, IndexWriter.SOURCE_FLUSH);
  }

  private SegmentCommitInfo dirty(String name) {
    return dirty(name, ABOVE_FLOOR_MB);
  }

  private SegmentCommitInfo dirty(String name, double sizeMB) {
    return makeSegmentCommitInfo(name, 1000, 100, sizeMB, IndexWriter.SOURCE_FLUSH);
  }

  private SegmentCommitInfo dirty40(String name) {
    return makeSegmentCommitInfo(name, 1000, 400, ABOVE_FLOOR_MB, IndexWriter.SOURCE_FLUSH);
  }

  /** 50% deleted: above HNSW's 40% base-graph cutoff, still large enough for TMP's 1.5x rule. */
  private SegmentCommitInfo dirtyOverForty(String name) {
    return makeSegmentCommitInfo(name, 1000, 500, ABOVE_FLOOR_MB, IndexWriter.SOURCE_FLUSH);
  }

  private static TieredMergePolicy overBudgetTmp() {
    TieredMergePolicy tmp = new TieredMergePolicy();
    tmp.setSegmentsPerTier(2);
    tmp.setTargetSearchConcurrency(1);
    return tmp;
  }

  private static boolean hasMixedMerge(MergeSpecification spec) {
    for (OneMerge merge : spec.merges) {
      boolean clean = false;
      boolean dirty = false;
      for (SegmentCommitInfo sci : merge.segments) {
        if (sci.getDelCount() == 0) {
          clean = true;
        } else {
          dirty = true;
        }
      }
      if (clean && dirty) {
        return true;
      }
    }
    return false;
  }

  private static SegmentInfos infos(SegmentCommitInfo... scis) {
    SegmentInfos segmentInfos = new SegmentInfos(Version.LATEST.major);
    for (SegmentCommitInfo sci : scis) {
      segmentInfos.add(sci);
    }
    return segmentInfos;
  }

  private static List<String> names(OneMerge merge) {
    List<String> names = new ArrayList<>();
    for (SegmentCommitInfo sci : merge.segments) {
      names.add(sci.info.name);
    }
    return names;
  }

  private static Set<String> names(SegmentInfos infos) {
    Set<String> names = new HashSet<>();
    for (SegmentCommitInfo sci : infos) {
      names.add(sci.info.name);
    }
    return names;
  }

  private static void assertSameSpecification(
      MergeSpecification expected, MergeSpecification actual) {
    if (expected == null) {
      assertNull(actual);
      return;
    }
    assertNotNull(actual);
    assertEquals(expected.merges.size(), actual.merges.size());
    for (int i = 0; i < expected.merges.size(); i++) {
      assertEquals(names(expected.merges.get(i)), names(actual.merges.get(i)));
    }
  }

  /**
   * Wrapped TMP that never proposes natural merges, so tests can observe only proactive {@link
   * OneMerge}s. {@link #size} still delegates to TMP.
   */
  private static MergePolicy noNaturalMerges() {
    return new FilterMergePolicy(new TieredMergePolicy()) {
      @Override
      public MergeSpecification findMerges(
          MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext) {
        return null;
      }

      @Override
      public MergeSpecification findFullFlushMerges(
          MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext) {
        return null;
      }
    };
  }

  private static final class RecordingMergePolicy extends FilterMergePolicy {
    SegmentInfos lastInfos;

    RecordingMergePolicy(MergePolicy in) {
      super(in);
    }

    @Override
    public MergeSpecification findMerges(
        MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext)
        throws IOException {
      lastInfos = segmentInfos;
      return in.findMerges(mergeTrigger, segmentInfos, mergeContext);
    }

    @Override
    public MergeSpecification findFullFlushMerges(
        MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext)
        throws IOException {
      lastInfos = segmentInfos;
      return in.findFullFlushMerges(mergeTrigger, segmentInfos, mergeContext);
    }
  }

  /**
   * Notes whether a legal 0-delete group existed at merge-selection time and whether the returned
   * spec mixed those clean segments with dirty ones.
   */
  private static final class MixingRecorder extends FilterMergePolicy {
    boolean sawMixedMergeWhileCleanGroupExisted;
    boolean sawCleanProactiveGroup;

    MixingRecorder(MergePolicy in) {
      super(in);
    }

    @Override
    public MergeSpecification findMerges(
        MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext)
        throws IOException {
      MergeSpecification spec = in.findMerges(mergeTrigger, segmentInfos, mergeContext);
      inspect(spec, segmentInfos, mergeContext);
      return spec;
    }

    @Override
    public MergeSpecification findFullFlushMerges(
        MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext)
        throws IOException {
      MergeSpecification spec = in.findFullFlushMerges(mergeTrigger, segmentInfos, mergeContext);
      inspect(spec, segmentInfos, mergeContext);
      return spec;
    }

    private void inspect(
        MergeSpecification spec, SegmentInfos segmentInfos, MergeContext mergeContext)
        throws IOException {
      int clean = 0;
      int dirty = 0;
      for (SegmentCommitInfo sci : segmentInfos) {
        if (mergeContext.getMergingSegments().contains(sci)) {
          continue;
        }
        if (mergeContext.numDeletesToMerge(sci) == 0) {
          clean++;
        } else {
          dirty++;
        }
      }
      boolean groupPossible = clean >= 3 && dirty > 0;
      if (spec == null || groupPossible == false) {
        return;
      }
      for (OneMerge merge : spec.merges) {
        int mergeClean = 0;
        int mergeDirty = 0;
        for (SegmentCommitInfo sci : merge.segments) {
          if (mergeContext.numDeletesToMerge(sci) == 0) {
            mergeClean++;
          } else {
            mergeDirty++;
          }
        }
        // Mixing 3+ packable cleans with dirty segs is the failure mode; 1 leftover clean
        // handed to TMP is allowed.
        if (mergeClean >= 3 && mergeDirty > 0) {
          sawMixedMergeWhileCleanGroupExisted = true;
        }
        if (mergeClean >= 3 && mergeDirty == 0) {
          sawCleanProactiveGroup = true;
        }
      }
    }
  }
}
