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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;

/**
 * Verifies that an index may hold more documents in total than any single segment can, i.e. more
 * than the per-segment {@link IndexWriter#MAX_DOCS} limit, spread across multiple segments, while
 * every individual segment stays within that per-segment limit.
 *
 * <p>Indexing billions of documents is not practical in a unit test, so this uses {@link
 * IndexWriter#setMaxDocsPerSegment(int)} to lower the per-segment cap to a small number. The
 * whole-index total then exceeds the per-segment cap with only a handful of tiny documents, which
 * exercises exactly the same code paths that would fire at the real {@code Integer.MAX_VALUE - 128}
 * boundary.
 */
public class TestMaxDocsAcrossSegments extends LuceneTestCase {

  public void testTotalExceedsPerSegmentLimit() throws Exception {
    final int perSegment = 8;
    IndexWriter.setMaxDocsPerSegment(perSegment);
    try (Directory dir = newDirectory()) {
      // The per-segment doc limit is a whole-index invariant regardless of merge policy, so pin the
      // default TieredMergePolicy rather than the randomized test one.
      IndexWriterConfig config = new IndexWriterConfig().setMergePolicy(new TieredMergePolicy());
      final int numDocs = perSegment * 10 + 3; // 83: an order of magnitude past the per-segment cap

      try (IndexWriter w = new IndexWriter(dir, config)) {
        for (int i = 0; i < numDocs; i++) {
          Document doc = new Document();
          doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
          w.addDocument(doc);
        }
        w.commit();

        try (DirectoryReader reader = DirectoryReader.open(w)) {
          // The index holds more docs than any single segment is allowed to.
          assertEquals(numDocs, reader.maxDoc());
          assertEquals(numDocs, reader.numDocs());
          assertTrue(
              "expected the docs to be split across multiple segments", reader.leaves().size() > 1);
          assertPerSegmentLimit(reader, perSegment);
        }

        // Force-merging to a single segment is impossible once the live-doc count exceeds the
        // per-segment cap: the policy must instead keep the merged segments within the cap.
        w.forceMerge(1);
        w.commit();
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(numDocs, reader.numDocs());
        final int minSegments = (numDocs + perSegment - 1) / perSegment;
        assertTrue(
            "force-merge must not coalesce below "
                + minSegments
                + " segments; got "
                + reader.leaves().size(),
            reader.leaves().size() >= minSegments);
        assertPerSegmentLimit(reader, perSegment);
      }
    } finally {
      IndexWriter.setMaxDocsPerSegment(IndexWriter.MAX_DOCS);
    }
  }

  /**
   * Same as {@link #testTotalExceedsPerSegmentLimit()} but with {@link LogByteSizeMergePolicy}, the
   * policy Elasticsearch uses for time-based data (the indices most likely to grow past the limit).
   * forceMerge must leave the segments split rather than build one oversized segment.
   */
  public void testTimeBasedPolicyExceedsPerSegmentLimit() throws Exception {
    final int perSegment = 8;
    IndexWriter.setMaxDocsPerSegment(perSegment);
    try (Directory dir = newDirectory()) {
      IndexWriterConfig config =
          new IndexWriterConfig().setMergePolicy(new LogByteSizeMergePolicy());
      final int numDocs = perSegment * 10 + 3;

      try (IndexWriter w = new IndexWriter(dir, config)) {
        for (int i = 0; i < numDocs; i++) {
          Document doc = new Document();
          doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
          w.addDocument(doc);
        }
        w.commit();
        w.forceMerge(1);
        w.commit();
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(numDocs, reader.numDocs());
        final int minSegments = (numDocs + perSegment - 1) / perSegment;
        assertTrue(
            "force-merge must not coalesce below "
                + minSegments
                + " segments; got "
                + reader.leaves().size(),
            reader.leaves().size() >= minSegments);
        assertPerSegmentLimit(reader, perSegment);
      }
    } finally {
      IndexWriter.setMaxDocsPerSegment(IndexWriter.MAX_DOCS);
    }
  }

  /**
   * The per-segment cap lives in the stock merge policies, not in the writer, so a merge policy
   * that does not honor it (a custom one, or the randomized test policy) can still select a merge
   * whose live docs exceed the per-segment limit. Indexing never produces such a segment, so this
   * uses a merge policy that forces every segment into one merge. The writer must catch that at
   * merge time and fail with a clear error instead of overflowing the int maxDoc. No deletes are
   * involved, matching the reported failure.
   */
  public void testMergeBeyondPerSegmentLimitFailsFast() throws Exception {
    final int perSegment = 8;
    IndexWriter.setMaxDocsPerSegment(perSegment);
    try (Directory dir = newDirectory()) {
      IndexWriterConfig config =
          new IndexWriterConfig()
              .setMergePolicy(new MergeEverythingPolicy())
              .setMergeScheduler(new SerialMergeScheduler());
      final int numDocs = perSegment * 10 + 3;

      try (IndexWriter w = new IndexWriter(dir, config)) {
        for (int i = 0; i < numDocs; i++) {
          Document doc = new Document();
          doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
          w.addDocument(doc);
        }
        w.commit();

        IllegalArgumentException e =
            expectThrows(IllegalArgumentException.class, () -> w.forceMerge(1));
        assertTrue(
            "expected the per-segment limit message, got: " + e,
            e.getMessage().contains("number of documents in a merged segment cannot exceed"));
      }
    } finally {
      IndexWriter.setMaxDocsPerSegment(IndexWriter.MAX_DOCS);
    }
  }

  /**
   * A merge policy that ignores the per-segment doc limit and forces every segment into a single
   * merge, standing in for any merge policy that does not honor the cap.
   */
  private static class MergeEverythingPolicy extends TieredMergePolicy {
    @Override
    public MergeSpecification findForcedMerges(
        SegmentInfos infos,
        int maxSegmentCount,
        Map<SegmentCommitInfo, Boolean> segmentsToMerge,
        MergeContext mergeContext) {
      List<SegmentCommitInfo> toMerge = new ArrayList<>();
      for (SegmentCommitInfo info : infos) {
        if (mergeContext.getMergingSegments().contains(info) == false) {
          toMerge.add(info);
        }
      }
      if (toMerge.size() <= 1) {
        return null;
      }
      MergeSpecification spec = new MergeSpecification();
      spec.add(new OneMerge(toMerge));
      return spec;
    }
  }

  private static void assertPerSegmentLimit(DirectoryReader reader, int perSegment) {
    for (LeafReaderContext ctx : reader.leaves()) {
      assertTrue(
          "segment maxDoc=" + ctx.maxLeafDoc() + " exceeds per-segment cap " + perSegment,
          ctx.maxLeafDoc() <= perSegment);
    }
  }
}
