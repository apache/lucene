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

package org.apache.lucene.search;

import java.io.IOException;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestDistinctDocKnnCollector extends LuceneTestCase {

  /**
   * Verifies that multiple ordinals mapping to the same document are deduplicated: only one entry
   * per document in the underlying heap, and topDocs reflects the best score.
   */
  public void testDeduplication() throws IOException {
    int d = 2;
    TopKnnCollector underlying = new TopKnnCollector(d, Integer.MAX_VALUE);

    // ord 0,1,2 -> Doc 100; ord 3 -> Doc 200
    MockKnnVectorValues vectorValues = new MockKnnVectorValues(new int[] {100, 100, 100, 200});
    DistinctDocKnnCollector collector = new DistinctDocKnnCollector(underlying, vectorValues);

    // Collect three ordinals for Doc 100
    collector.collect(0, 0.5f);
    collector.collect(1, 0.9f); // better
    collector.collect(2, 0.7f); // worse than 0.9

    // Collect one ordinal for Doc 200
    collector.collect(3, 0.8f);

    assertEquals(2, collector.distinctDocCount());

    TopDocs topDocs = collector.topDocs();
    assertEquals(2, topDocs.scoreDocs.length);

    // Doc 100 should be first with score 0.9, best ordinal = 1
    assertEquals(1, topDocs.scoreDocs[0].doc);
    assertEquals(0.9f, topDocs.scoreDocs[0].score, 0.0001f);

    // Doc 200 should be second with score 0.8, ordinal = 3
    assertEquals(3, topDocs.scoreDocs[1].doc);
    assertEquals(0.8f, topDocs.scoreDocs[1].score, 0.0001f);
  }

  /** Verifies minCompetitiveSimilarity transitions from NEGATIVE_INFINITY once D docs are found. */
  public void testMinCompetitiveSimilarity() throws IOException {
    int d = 2;
    TopKnnCollector underlying = new TopKnnCollector(d, Integer.MAX_VALUE);

    // ord 0 -> Doc 100, ord 1 -> Doc 200, ord 2 -> Doc 300
    MockKnnVectorValues vectorValues = new MockKnnVectorValues(new int[] {100, 200, 300});
    DistinctDocKnnCollector collector = new DistinctDocKnnCollector(underlying, vectorValues);

    // No docs yet — should accept everything
    assertEquals(Float.NEGATIVE_INFINITY, collector.minCompetitiveSimilarity(), 0f);

    // One doc — still not full
    collector.collect(0, 0.5f);
    assertEquals(Float.NEGATIVE_INFINITY, collector.minCompetitiveSimilarity(), 0f);

    // Two docs — now full, threshold should rise
    collector.collect(1, 0.8f);
    assertTrue(collector.minCompetitiveSimilarity() > Float.NEGATIVE_INFINITY);
  }

  /**
   * Verifies that collecting a second vector for an already-tracked document does not consume
   * another slot in the underlying heap.
   */
  public void testDuplicateDocsDoNotConsumeHeapSlots() throws IOException {
    int d = 2;
    TopKnnCollector underlying = new TopKnnCollector(d, Integer.MAX_VALUE);

    // ord 0,1 -> Doc 100; ord 2 -> Doc 200; ord 3 -> Doc 300
    MockKnnVectorValues vectorValues = new MockKnnVectorValues(new int[] {100, 100, 200, 300});
    DistinctDocKnnCollector collector = new DistinctDocKnnCollector(underlying, vectorValues);

    // Collect two ordinals for Doc 100 — only one should go to underlying heap
    collector.collect(0, 0.5f);
    collector.collect(1, 0.9f);

    // Only 1 unique document so far
    assertEquals(1, collector.distinctDocCount());
    assertEquals(Float.NEGATIVE_INFINITY, collector.minCompetitiveSimilarity(), 0f);

    // Add Doc 200 — now D=2 docs, threshold should activate
    collector.collect(2, 0.6f);
    assertEquals(2, collector.distinctDocCount());
    assertTrue(collector.minCompetitiveSimilarity() > Float.NEGATIVE_INFINITY);
  }

  /** Verifies that topDocs returns at most D results even if more documents are encountered. */
  public void testTopDocsLimitedToD() throws IOException {
    int d = 2;
    TopKnnCollector underlying = new TopKnnCollector(d, Integer.MAX_VALUE);

    // 4 ordinals, each a different document
    MockKnnVectorValues vectorValues = new MockKnnVectorValues(new int[] {100, 200, 300, 400});
    DistinctDocKnnCollector collector = new DistinctDocKnnCollector(underlying, vectorValues);

    collector.collect(0, 0.3f);
    collector.collect(1, 0.9f);
    collector.collect(2, 0.7f);
    collector.collect(3, 0.5f);

    TopDocs topDocs = collector.topDocs();
    assertEquals(d, topDocs.scoreDocs.length);

    // Top 2 by score should be 0.9 and 0.7
    assertEquals(0.9f, topDocs.scoreDocs[0].score, 0.0001f);
    assertEquals(0.7f, topDocs.scoreDocs[1].score, 0.0001f);
  }

  private static class MockKnnVectorValues extends KnnVectorValues {
    private final int[] ordToDoc;

    MockKnnVectorValues(int[] ordToDoc) {
      this.ordToDoc = ordToDoc;
    }

    @Override
    public int ordToDoc(int ord) {
      return ordToDoc[ord];
    }

    @Override
    public int dimension() {
      return 0;
    }

    @Override
    public int size() {
      return ordToDoc.length;
    }

    @Override
    public KnnVectorValues copy() {
      return this;
    }

    @Override
    public VectorEncoding getEncoding() {
      return VectorEncoding.FLOAT32;
    }
  }
}
