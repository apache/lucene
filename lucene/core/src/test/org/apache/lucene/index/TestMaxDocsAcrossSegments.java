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
          assertEquals(numDocs, reader.totalMaxDoc());
          assertEquals(numDocs, reader.totalNumDocs());
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
        assertEquals(numDocs, reader.totalNumDocs());
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

  private static void assertPerSegmentLimit(DirectoryReader reader, int perSegment) {
    for (LeafReaderContext ctx : reader.leaves()) {
      assertTrue(
          "segment maxDoc=" + ctx.reader().maxDoc() + " exceeds per-segment cap " + perSegment,
          ctx.reader().maxDoc() <= perSegment);
    }
  }
}
