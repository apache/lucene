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
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.ToIntFunction;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.MergePolicy.MergeSpecification;
import org.apache.lucene.index.MergePolicy.OneMerge;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;

/**
 * Checks {@link ProactiveMergePolicy}. Selectors add merges around a wrapped policy. Claimed
 * segments stay in the segment list and are reported as already merging, so a log policy does not
 * merge across them. {@link DeleteRunSelector} reclaims deletes below the merge factor, bounded by
 * a byte cap. Forced merges and full-flush merges stay with the wrapped policy.
 */
public class TestProactiveMergePolicy extends LuceneTestCase {

  private static final long DEFAULT_CAP = 2048L * 1024 * 1024;

  static final class Groups implements MergeSelector {
    private final List<List<SegmentCommitInfo>> groups;

    Groups(List<List<SegmentCommitInfo>> groups) {
      this.groups = groups;
    }

    @Override
    public List<List<SegmentCommitInfo>> select(
        MergeTrigger trigger, SegmentInfos infos, MergePolicy.MergeContext context) {
      return groups;
    }
  }

  static final class MockMergeContext implements MergePolicy.MergeContext {
    private final ToIntFunction<SegmentCommitInfo> numDeletesFunc;
    private Set<SegmentCommitInfo> mergingSegments = Collections.emptySet();
    private InfoStream infoStream = InfoStream.NO_OUTPUT;

    MockMergeContext(ToIntFunction<SegmentCommitInfo> numDeletesFunc) {
      this.numDeletesFunc = numDeletesFunc;
    }

    @Override
    public int numDeletesToMerge(SegmentCommitInfo info) {
      return numDeletesFunc.applyAsInt(info);
    }

    @Override
    public int numDeletedDocs(SegmentCommitInfo info) {
      return numDeletesToMerge(info);
    }

    @Override
    public InfoStream getInfoStream() {
      return infoStream;
    }

    @Override
    public Set<SegmentCommitInfo> getMergingSegments() {
      return mergingSegments;
    }

    void setMergingSegments(Set<SegmentCommitInfo> mergingSegments) {
      this.mergingSegments = mergingSegments;
    }

    void setInfoStream(InfoStream infoStream) {
      this.infoStream = infoStream;
    }
  }

  static final class RecordingLog extends LogByteSizeMergePolicy {
    SegmentInfos lastInfos;
    Set<SegmentCommitInfo> lastMerging = Set.of();

    @Override
    public MergeSpecification findMerges(MergeTrigger trigger, SegmentInfos infos, MergeContext ctx)
        throws IOException {
      lastInfos = infos;
      lastMerging = new LinkedHashSet<>(ctx.getMergingSegments());
      return super.findMerges(trigger, infos, ctx);
    }
  }

  static final class ListInfoStream extends InfoStream {
    final List<String> messages = new ArrayList<>();

    @Override
    public void message(String component, String message) {
      if ("MP".equals(component)) {
        messages.add(message);
      }
    }

    @Override
    public boolean isEnabled(String component) {
      return "MP".equals(component);
    }

    @Override
    public void close() {}
  }

  private static final Directory FAKE_DIRECTORY =
      new Directory() {
        @Override
        public String[] listAll() {
          throw new UnsupportedOperationException();
        }

        @Override
        public void deleteFile(String name) {
          throw new UnsupportedOperationException();
        }

        @Override
        public long fileLength(String name) {
          if (name.endsWith(".liv")) {
            return 0;
          }
          int idx = name.indexOf("_size=");
          if (idx == -1) {
            throw new IllegalArgumentException("No _size= in: " + name);
          }
          return Long.parseLong(name.substring(idx + 6, name.indexOf('.', idx)));
        }

        @Override
        public IndexOutput createOutput(String name, IOContext context) {
          throw new UnsupportedOperationException();
        }

        @Override
        public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) {
          throw new UnsupportedOperationException();
        }

        @Override
        public void sync(java.util.Collection<String> names) {
          throw new UnsupportedOperationException();
        }

        @Override
        public void rename(String source, String dest) {
          throw new UnsupportedOperationException();
        }

        @Override
        public void syncMetaData() {
          throw new UnsupportedOperationException();
        }

        @Override
        public IndexInput openInput(String name, IOContext context) {
          throw new UnsupportedOperationException();
        }

        @Override
        public Lock obtainLock(String name) {
          throw new UnsupportedOperationException();
        }

        @Override
        public void close() {}

        @Override
        public void copyFrom(Directory from, String src, String dest, IOContext context) {
          throw new UnsupportedOperationException();
        }

        @Override
        public Set<String> getPendingDeletions() {
          return Collections.emptySet();
        }
      };

  private static SegmentCommitInfo seg(String name, int maxDoc, int deletes, double sizeMB) {
    byte[] id = new byte[StringHelper.ID_LENGTH];
    random().nextBytes(id);
    SegmentInfo info =
        new SegmentInfo(
            FAKE_DIRECTORY,
            Version.LATEST,
            Version.LATEST,
            name,
            maxDoc,
            false,
            false,
            TestUtil.getDefaultCodec(),
            Collections.emptyMap(),
            id,
            Collections.singletonMap(IndexWriter.SOURCE, IndexWriter.SOURCE_FLUSH),
            null);
    info.setFiles(Collections.singleton(name + "_size=" + (long) (sizeMB * 1024 * 1024) + ".fake"));
    return new SegmentCommitInfo(info, deletes, 0, 0, 0, 0, StringHelper.randomId());
  }

  private static SegmentInfos infos(SegmentCommitInfo... segments) {
    SegmentInfos sis = new SegmentInfos(Version.LATEST.major);
    for (SegmentCommitInfo segment : segments) {
      sis.add(segment);
    }
    return sis;
  }

  private static MockMergeContext ctx() {
    return new MockMergeContext(SegmentCommitInfo::getDelCount);
  }

  private static boolean sameMerge(OneMerge merge, SegmentCommitInfo... segments) {
    return merge.segments.equals(List.of(segments));
  }

  private static boolean paired(MergeSpecification spec, SegmentCommitInfo a, SegmentCommitInfo b) {
    if (spec == null) {
      return false;
    }
    for (OneMerge merge : spec.merges) {
      if (merge.segments.contains(a) && merge.segments.contains(b)) {
        return true;
      }
    }
    return false;
  }

  public void testNullInnerPolicyThrows() {
    expectThrows(IllegalArgumentException.class, () -> new ProactiveMergePolicy(null));
  }

  public void testNoSelectorsMatchWrappedPolicy() throws IOException {
    LogByteSizeMergePolicy log = new LogByteSizeMergePolicy();
    SegmentInfos sis =
        infos(seg("_0", 1000, 0, 10), seg("_1", 1000, 0, 10), seg("_2", 1000, 0, 10));
    MockMergeContext context = ctx();
    assertNull(log.findMerges(MergeTrigger.SEGMENT_FLUSH, sis, context));
    assertNull(new ProactiveMergePolicy(log).findMerges(MergeTrigger.SEGMENT_FLUSH, sis, context));
  }

  public void testClearingSelectorRestoresWrappedPolicy() throws IOException {
    SegmentCommitInfo only = seg("_0", 1000, 500, 10);
    SegmentInfos sis = infos(only);
    LogByteSizeMergePolicy log = new LogByteSizeMergePolicy();
    MockMergeContext context = ctx();
    ProactiveMergePolicy policy =
        new ProactiveMergePolicy(log).setBeforeMergeSelector(new Groups(List.of(List.of(only))));
    MergeSpecification claimed = policy.findMerges(MergeTrigger.EXPLICIT, sis, context);
    assertNotNull(claimed);
    assertEquals(1, claimed.merges.size());
    assertTrue(sameMerge(claimed.merges.get(0), only));

    policy.setBeforeMergeSelector(null);
    assertNull(policy.findMerges(MergeTrigger.EXPLICIT, sis, context));
    assertNull(log.findMerges(MergeTrigger.EXPLICIT, sis, context));
  }

  public void testSizeUnitFollowsWrappedPolicy() {
    assertEquals(
        MergePolicy.SizeUnit.DOCS, new ProactiveMergePolicy(new LogDocMergePolicy()).getSizeUnit());
    assertEquals(
        MergePolicy.SizeUnit.BYTES,
        new ProactiveMergePolicy(new LogByteSizeMergePolicy()).getSizeUnit());
  }

  public void testDeleteRunSplitsOnCleanAndMergingSegments() throws IOException {
    DeleteRunSelector selector = new DeleteRunSelector(30, DEFAULT_CAP);
    MockMergeContext context = ctx();
    SegmentCommitInfo high = seg("_0", 1000, 500, 10);
    SegmentCommitInfo keep = seg("_1", 1000, 100, 10);
    SegmentCommitInfo high2 = seg("_2", 1000, 400, 10);
    SegmentCommitInfo clean = seg("_c", 1000, 0, 10);

    List<List<SegmentCommitInfo>> runs =
        selector.select(MergeTrigger.SEGMENT_FLUSH, infos(high, high2), context);
    assertEquals(List.of(List.of(high, high2)), runs);

    runs = selector.select(MergeTrigger.SEGMENT_FLUSH, infos(high, clean, high2), context);
    assertEquals(List.of(List.of(high), List.of(high2)), runs);

    runs = selector.select(MergeTrigger.SEGMENT_FLUSH, infos(high, keep, high2), context);
    assertEquals(List.of(List.of(high), List.of(high2)), runs);

    context.setMergingSegments(Set.of(high2));
    runs = selector.select(MergeTrigger.SEGMENT_FLUSH, infos(high, high2, keep), context);
    assertEquals(List.of(List.of(high)), runs);
  }

  public void testDeleteRunRespectsMaxMergeBytes() throws IOException {
    long mb = 1024L * 1024;
    DeleteRunSelector selector = new DeleteRunSelector(30, 100 * mb);
    MockMergeContext context = ctx();
    SegmentCommitInfo[] large = new SegmentCommitInfo[6];
    for (int i = 0; i < large.length; i++) {
      large[i] = seg("_" + i, 1000, 500, 160);
    }
    List<List<SegmentCommitInfo>> runs =
        selector.select(MergeTrigger.SEGMENT_FLUSH, infos(large), context);
    assertEquals(6, runs.size());
    for (int i = 0; i < large.length; i++) {
      assertEquals(List.of(large[i]), runs.get(i));
    }

    SegmentCommitInfo a = seg("_a", 1000, 500, 80);
    SegmentCommitInfo b = seg("_b", 1000, 500, 80);
    SegmentCommitInfo c = seg("_c", 1000, 500, 80);
    SegmentCommitInfo d = seg("_d", 1000, 500, 80);
    runs = selector.select(MergeTrigger.SEGMENT_FLUSH, infos(a, b, c, d), context);
    assertEquals(List.of(List.of(a, b), List.of(c, d)), runs);

    SegmentCommitInfo huge = seg("_h", 1000, 500, 400);
    runs = selector.select(MergeTrigger.SEGMENT_FLUSH, infos(huge), context);
    assertEquals(List.of(List.of(huge)), runs);
  }

  public void testToStringShowsSelectors() {
    DeleteRunSelector selector = new DeleteRunSelector(30, DEFAULT_CAP);
    String text =
        new ProactiveMergePolicy(new LogByteSizeMergePolicy())
            .setAfterMergeSelector(selector)
            .toString();
    assertTrue(text.contains("LogByteSizeMergePolicy"));
    assertTrue(text.contains("before=null"));
    assertTrue(text.contains("deletesPct=30.0"));
    assertTrue(text.contains("maxMergeBytes=" + DEFAULT_CAP));
  }

  public void testNineHalfDeletedSegmentsMergeWhenLogPolicyDoesNot() throws IOException {
    LogByteSizeMergePolicy log = new LogByteSizeMergePolicy();
    MockMergeContext context = ctx();
    SegmentInfos sis = new SegmentInfos(Version.LATEST.major);
    for (int i = 0; i < 9; i++) {
      sis.add(seg("_" + i, 10000, 5000, 50));
    }
    assertNull(log.findMerges(MergeTrigger.SEGMENT_FLUSH, sis, context));

    MergeSpecification spec =
        new ProactiveMergePolicy(log)
            .setAfterMergeSelector(new DeleteRunSelector(30, DEFAULT_CAP))
            .findMerges(MergeTrigger.SEGMENT_FLUSH, sis, context);
    assertNotNull(spec);
    assertEquals(1, spec.merges.size());
    assertEquals(sis.asList(), spec.merges.get(0).segments);
  }

  public void testFullFlushAndForcedMergesStayWithWrappedPolicy() throws IOException {
    LogByteSizeMergePolicy log = new LogByteSizeMergePolicy();
    log.setMergeFactor(10);
    SegmentInfos sis = infos(seg("_0", 1000, 500, 10), seg("_1", 1000, 500, 10));
    MockMergeContext context = ctx();
    ProactiveMergePolicy policy =
        new ProactiveMergePolicy(log)
            .setBeforeMergeSelector(new DeleteRunSelector(30, DEFAULT_CAP));

    MergeSpecification natural = policy.findMerges(MergeTrigger.SEGMENT_FLUSH, sis, context);
    assertNotNull(natural);
    assertEquals(1, natural.merges.size());
    assertEquals(sis.asList(), natural.merges.get(0).segments);

    assertNull(policy.findFullFlushMerges(MergeTrigger.COMMIT, sis, context));
    assertNull(log.findFullFlushMerges(MergeTrigger.COMMIT, sis, context));
    assertNull(
        policy.findForcedMerges(
            sis,
            10,
            java.util.Map.of(sis.info(0), Boolean.TRUE, sis.info(1), Boolean.TRUE),
            context));
  }

  public void testBeforeClaimDoesNotGlueLogNeighbors() throws IOException {
    RecordingLog log = new RecordingLog();
    log.setMergeFactor(2);
    SegmentCommitInfo s0 = seg("_0", 1000, 0, 32);
    SegmentCommitInfo claimed = seg("_1", 1000, 0, 32);
    SegmentCommitInfo s2 = seg("_2", 1000, 0, 32);
    SegmentCommitInfo s3 = seg("_3", 1000, 0, 32);
    SegmentInfos full = infos(s0, claimed, s2, s3);
    MockMergeContext context = ctx();

    MergeSpecification glued =
        log.findMerges(MergeTrigger.SEGMENT_FLUSH, infos(s0, s2, s3), context);
    assertTrue(paired(glued, s0, s2));

    MergeSpecification spec =
        new ProactiveMergePolicy(log)
            .setBeforeMergeSelector(new Groups(List.of(List.of(claimed))))
            .findMerges(MergeTrigger.SEGMENT_FLUSH, full, context);
    assertNotNull(spec);
    assertFalse(paired(spec, s0, s2));
    boolean claimedMerge = false;
    for (OneMerge merge : spec.merges) {
      if (sameMerge(merge, claimed)) {
        claimedMerge = true;
      }
    }
    assertTrue(claimedMerge);
    assertEquals(4, log.lastInfos.size());
    assertTrue(log.lastMerging.contains(claimed));
    assertFalse(log.lastMerging.contains(s0));
    assertFalse(log.lastMerging.contains(s2));
  }

  public void testNestedWrapperSeesOuterClaimsAsMerging() throws IOException {
    RecordingLog log = new RecordingLog();
    SegmentCommitInfo claimed = seg("_0", 1000, 0, 10);
    SegmentCommitInfo other = seg("_1", 1000, 0, 10);
    SegmentInfos sis = infos(claimed, other);
    new ProactiveMergePolicy(new ProactiveMergePolicy(log))
        .setBeforeMergeSelector(new Groups(List.of(List.of(claimed))))
        .findMerges(MergeTrigger.SEGMENT_FLUSH, sis, ctx());
    assertEquals(2, log.lastInfos.size());
    assertSame(claimed, log.lastInfos.info(0));
    assertSame(other, log.lastInfos.info(1));
    assertTrue(log.lastMerging.contains(claimed));
    assertFalse(log.lastMerging.contains(other));
  }

  public void testNonContiguousClaimStaysOneMerge() throws IOException {
    RecordingLog log = new RecordingLog();
    log.setMergeFactor(2);
    SegmentCommitInfo left = seg("_0", 1000, 0, 32);
    SegmentCommitInfo middle = seg("_1", 1000, 0, 32);
    SegmentCommitInfo right = seg("_2", 1000, 0, 32);
    SegmentInfos sis = infos(left, middle, right);
    MergeSpecification spec =
        new ProactiveMergePolicy(log)
            .setBeforeMergeSelector(new Groups(List.of(List.of(left, right))))
            .findMerges(MergeTrigger.SEGMENT_FLUSH, sis, ctx());
    assertNotNull(spec);
    boolean claimed = false;
    for (OneMerge merge : spec.merges) {
      if (sameMerge(merge, left, right)) {
        claimed = true;
      }
      assertFalse(merge.segments.contains(middle) && merge.segments.contains(left));
    }
    assertTrue(claimed);
    assertEquals(3, log.lastInfos.size());
    assertTrue(log.lastMerging.contains(left));
    assertTrue(log.lastMerging.contains(right));
    assertFalse(log.lastMerging.contains(middle));
  }

  public void testAfterSelectorTreatsStockMergeAsBarrier() throws IOException {
    SegmentCommitInfo high = seg("_0", 1000, 500, 10);
    SegmentCommitInfo middle = seg("_1", 1000, 500, 10);
    SegmentCommitInfo high2 = seg("_2", 1000, 400, 10);
    SegmentInfos sis = infos(high, middle, high2);
    MergeSpecification stock = new MergeSpecification();
    stock.add(new OneMerge(List.of(middle)));
    LogByteSizeMergePolicy log =
        new LogByteSizeMergePolicy() {
          @Override
          public MergeSpecification findMerges(
              MergeTrigger trigger, SegmentInfos infos, MergeContext ctx) {
            return stock;
          }
        };
    MergeSpecification spec =
        new ProactiveMergePolicy(log)
            .setAfterMergeSelector(new DeleteRunSelector(30, DEFAULT_CAP))
            .findMerges(MergeTrigger.SEGMENT_FLUSH, sis, ctx());
    assertNotNull(spec);
    assertEquals(3, spec.merges.size());
    assertTrue(sameMerge(spec.merges.get(0), middle));
    assertTrue(sameMerge(spec.merges.get(1), high));
    assertTrue(sameMerge(spec.merges.get(2), high2));
  }

  public void testConflictingGroupIsSkipped() throws IOException {
    SegmentCommitInfo merging = seg("_0", 10, 0, 1);
    SegmentCommitInfo other = seg("_1", 10, 0, 1);
    SegmentCommitInfo free1 = seg("_2", 10, 0, 1);
    SegmentCommitInfo free2 = seg("_3", 10, 0, 1);
    SegmentCommitInfo outside = seg("_x", 10, 0, 1);
    SegmentInfos sis = infos(merging, other, free1, free2);
    MockMergeContext context = ctx();
    context.setMergingSegments(Set.of(merging));
    ListInfoStream stream = new ListInfoStream();
    context.setInfoStream(stream);
    List<List<SegmentCommitInfo>> groups = new ArrayList<>();
    groups.add(List.of(merging, other));
    groups.add(List.of(outside));
    groups.add(List.of(free1, free2));
    RecordingLog log = new RecordingLog();
    MergeSpecification spec =
        new ProactiveMergePolicy(log)
            .setBeforeMergeSelector(new Groups(groups))
            .findMerges(MergeTrigger.SEGMENT_FLUSH, sis, context);
    assertNotNull(spec);
    assertEquals(1, spec.merges.size());
    assertTrue(sameMerge(spec.merges.get(0), free1, free2));
    assertEquals(4, log.lastInfos.size());
    assertSame(other, log.lastInfos.info(1));
    assertTrue(log.lastMerging.contains(merging));
    assertFalse(log.lastMerging.contains(other));
    assertTrue(log.lastMerging.contains(free1));
    assertTrue(log.lastMerging.contains(free2));
    assertFalse(log.lastMerging.contains(outside));
    assertTrue(logged(stream, "skip selector group [_0 _1]", "already merging"));
    assertTrue(logged(stream, "skip selector group [_x]", "not in the index"));
  }

  private static boolean logged(ListInfoStream stream, String... parts) {
    for (String message : stream.messages) {
      boolean all = true;
      for (String part : parts) {
        if (message.contains(part) == false) {
          all = false;
          break;
        }
      }
      if (all) {
        return true;
      }
    }
    return false;
  }

  public void testBrokenSelectorGroupsAssert() throws IOException {
    SegmentCommitInfo a = seg("_0", 10, 0, 1);
    SegmentInfos sis = infos(a);
    MockMergeContext context = ctx();
    List<List<SegmentCommitInfo>> empty = new ArrayList<>();
    empty.add(List.of());
    List<SegmentCommitInfo> repeated = List.of(a, a);
    List<SegmentCommitInfo> withNull = new ArrayList<>();
    withNull.add(a);
    withNull.add(null);
    List<List<SegmentCommitInfo>> nullGroup = new ArrayList<>();
    nullGroup.add(null);

    assertBrokenGroup(sis, context, empty);
    assertBrokenGroup(sis, context, List.of(repeated));
    assertBrokenGroup(sis, context, List.of(withNull));
    assertBrokenGroup(sis, context, nullGroup);
  }

  private static void assertBrokenGroup(
      SegmentInfos sis, MockMergeContext context, List<List<SegmentCommitInfo>> groups)
      throws IOException {
    ProactiveMergePolicy policy =
        new ProactiveMergePolicy(new LogByteSizeMergePolicy())
            .setBeforeMergeSelector(new Groups(groups));
    if (TEST_ASSERTS_ENABLED) {
      expectThrows(
          AssertionError.class, () -> policy.findMerges(MergeTrigger.SEGMENT_FLUSH, sis, context));
    } else {
      assertNull(policy.findMerges(MergeTrigger.SEGMENT_FLUSH, sis, context));
    }
  }

  public void testDeleteRunReclaimsDeletes() throws IOException {
    Directory untouched = buildHalfDeletedIndex();
    Directory reclaimed = buildHalfDeletedIndex();
    try {
      assertEquals(9, openAndMerge(untouched, new LogByteSizeMergePolicy()));
      assertEquals(0, openAndMerge(reclaimed, deleteRuns()));
    } finally {
      untouched.close();
      reclaimed.close();
    }
  }

  private static MergePolicy deleteRuns() {
    LogByteSizeMergePolicy log = new LogByteSizeMergePolicy();
    log.setMergeFactor(10);
    return new ProactiveMergePolicy(log)
        .setAfterMergeSelector(new DeleteRunSelector(30, DEFAULT_CAP));
  }

  private static int openAndMerge(Directory dir, MergePolicy policy) throws IOException {
    IndexWriterConfig config = newIndexWriterConfig(new MockAnalyzer(random()));
    config.setOpenMode(OpenMode.APPEND);
    config.setMergePolicy(policy);
    config.setMergeScheduler(new SerialMergeScheduler());
    try (IndexWriter writer = new IndexWriter(dir, config)) {
      try (DirectoryReader before = DirectoryReader.open(writer)) {
        assertEquals(9, before.numDeletedDocs());
        assertEquals(9, before.leaves().size());
      }
      writer.maybeMerge();
      try (DirectoryReader after = DirectoryReader.open(writer)) {
        return after.numDeletedDocs();
      }
    }
  }

  private static Directory buildHalfDeletedIndex() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig(new MockAnalyzer(random()));
    config.setMergePolicy(NoMergePolicy.INSTANCE);
    config.setMaxBufferedDocs(2);
    config.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    config.setMergeScheduler(new SerialMergeScheduler());
    try (IndexWriter writer = new IndexWriter(dir, config)) {
      for (int i = 0; i < 18; i++) {
        Document doc = new Document();
        doc.add(new StringField("id", Integer.toString(i), Store.NO));
        writer.addDocument(doc);
      }
      for (int i = 0; i < 18; i += 2) {
        writer.deleteDocuments(new Term("id", Integer.toString(i)));
      }
      writer.commit();
    }
    return dir;
  }
}
