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
import java.util.Random;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.InfoStream;

/**
 * Tests that merging segments under the default codec reuses their existing HNSW graphs instead of
 * rebuilding the merged graph from scratch, both when the source segments are deletion-free and
 * when the base segment carries deletions below the delete percentage threshold. The default codec
 * wires vector readers through {@code PerFieldKnnVectorsFormat.FieldsReader}, the wrapper that
 * {@code IncrementalHnswGraphMerger#addReader} must unwrap for reuse to trigger.
 *
 * <p>The assertions key on the InfoStream message of {@code MergingHnswGraphBuilder}, used by the
 * single-threaded merger that the default format selects ({@code numMergeWorkers} is 1).
 */
public class TestHnswGraphReuseDuringMerge extends LuceneTestCase {

  private static final int DIM = 8;
  private static final int SEGMENTS = 2;

  /**
   * Above the tiny-segment cutoff of {@code Lucene99HnswVectorsWriter#shouldCreateGraph} (about 650
   * vectors at the default threshold), so graphs are built at both flush and merge time.
   */
  private static final int DOCS_PER_SEGMENT = 1500;

  public void testMergeReusesGraphsUnderDefaultCodec() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig cfg = new IndexWriterConfig();
      cfg.setCodec(TestUtil.getDefaultCodec());
      cfg.setMergePolicy(NoMergePolicy.INSTANCE);
      try (IndexWriter w = new IndexWriter(dir, cfg)) {
        for (int s = 0; s < SEGMENTS; s++) {
          addVectorDocs(w, DOCS_PER_SEGMENT, null);
          w.flush();
        }
      }

      List<String> hnswMessages = new ArrayList<>();
      IndexWriterConfig cfg2 = new IndexWriterConfig();
      cfg2.setCodec(TestUtil.getDefaultCodec());
      cfg2.setInfoStream(hnswInfoStream(hnswMessages));
      try (IndexWriter w2 = new IndexWriter(dir, cfg2)) {
        w2.forceMerge(1);
      }

      synchronized (hnswMessages) {
        assertTrue(
            "expected the merged graph to be built by joining the "
                + SEGMENTS
                + " deletion-free source graphs, but HNSW messages were: "
                + hnswMessages,
            hnswMessages.stream()
                .anyMatch(m -> m.startsWith("build graph from merging " + SEGMENTS + " graphs")));
      }
    }
  }

  /**
   * A graph with deletions below the delete percentage threshold of {@code
   * IncrementalHnswGraphMerger} is still selected as the base graph, so the merged graph is built
   * by joining both source graphs rather than rebuilding from scratch.
   */
  public void testMergeReusesBaseGraphWithDeletions() throws Exception {
    // enough docs that the base segment's live count after the deletions still exceeds the
    // other segment's, keeping it the base graph
    int baseSegmentDocs = 2000;
    try (Directory dir = newDirectory()) {
      IndexWriterConfig cfg = new IndexWriterConfig();
      cfg.setCodec(TestUtil.getDefaultCodec());
      cfg.setMergePolicy(NoMergePolicy.INSTANCE);
      try (IndexWriter w = new IndexWriter(dir, cfg)) {
        addVectorDocs(w, baseSegmentDocs, "d");
        w.flush();
        addVectorDocs(w, DOCS_PER_SEGMENT, null);
        w.flush();
      }

      List<String> hnswMessages = new ArrayList<>();
      IndexWriterConfig cfg2 = new IndexWriterConfig();
      cfg2.setCodec(TestUtil.getDefaultCodec());
      cfg2.setInfoStream(hnswInfoStream(hnswMessages));
      try (IndexWriter w2 = new IndexWriter(dir, cfg2)) {
        // delete 20% of the base segment, below the 40% delete percentage threshold
        for (int i = 0; i < baseSegmentDocs; i += 5) {
          w2.deleteDocuments(new Term("id", "d" + i));
        }
        w2.forceMerge(1);
      }

      // ensure the deletions actually applied, otherwise this degenerates into the clean case
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(
            "deletions were not applied",
            baseSegmentDocs - baseSegmentDocs / 5 + DOCS_PER_SEGMENT,
            reader.numDocs());
      }

      synchronized (hnswMessages) {
        assertTrue(
            "expected the merged graph to be built by joining the base graph with deletions and"
                + " the clean graph, but HNSW messages were: "
                + hnswMessages,
            hnswMessages.stream().anyMatch(m -> m.startsWith("build graph from merging 2 graphs")));
      }
    }
  }

  private static void addVectorDocs(IndexWriter w, int numDocs, String idPrefix)
      throws IOException {
    Random r = random();
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      float[] v = new float[DIM];
      for (int j = 0; j < DIM; j++) {
        v[j] = r.nextFloat();
      }
      doc.add(new KnnFloatVectorField("v", v));
      if (idPrefix != null) {
        doc.add(new StringField("id", idPrefix + i, Field.Store.NO));
      }
      w.addDocument(doc);
    }
  }

  private static InfoStream hnswInfoStream(List<String> sink) {
    return new InfoStream() {
      @Override
      public void message(String component, String message) {
        if ("HNSW".equals(component)) {
          synchronized (sink) {
            sink.add(message);
          }
        }
      }

      @Override
      public boolean isEnabled(String component) {
        return "HNSW".equals(component);
      }

      @Override
      public void close() {}
    };
  }
}
