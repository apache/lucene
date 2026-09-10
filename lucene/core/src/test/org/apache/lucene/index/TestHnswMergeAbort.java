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
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * Tests that aborting a merge (e.g. via {@link IndexWriter#rollback()}) interrupts HNSW graph
 * construction instead of building the entire graph.
 *
 * <p>The merge thread is held at the start of the graph build until the merge is marked aborted,
 * and the test then asserts that the build never ran to completion, which holds at any segment
 * size. The abort exception is deliberately not asserted on: it is swallowed as expected control
 * flow, and a small unchecked build can finish without ever throwing.
 */
public class TestHnswMergeAbort extends LuceneTestCase {

  private static final int DIM = 96;
  private static final int SEGMENTS = 4;
  private static final int DOCS_PER_SEGMENT = 1_000;
  private static final int BEAM_WIDTH = 100;
  // always build a graph, no matter how small the segment is
  private static final int TINY_SEGMENTS_THRESHOLD = 0;
  private static final int LIVE_DOCS_AFTER_DELETES = SEGMENTS * DOCS_PER_SEGMENT / 2;

  /**
   * Every segment carries more than {@code IncrementalHnswGraphMerger#DELETE_PCT_THRESHOLD}
   * deletions, so no source graph is eligible as a base and the merged graph is rebuilt from
   * scratch via {@code HnswGraphBuilder#addVectors}.
   */
  public void testRollbackDuringFullRebuildMerge() throws Exception {
    doTestRollbackDuringMerge(
        true,
        new Lucene99HnswVectorsFormat(16, BEAM_WIDTH, TINY_SEGMENTS_THRESHOLD),
        "build graph from " + LIVE_DOCS_AFTER_DELETES + " vectors");
  }

  /**
   * All segments are deletion-free, so the merged graph is produced by joining the source graphs
   * via {@code MergingHnswGraphBuilder}.
   */
  public void testRollbackDuringGraphJoinMerge() throws Exception {
    doTestRollbackDuringMerge(
        false,
        new Lucene99HnswVectorsFormat(16, BEAM_WIDTH, TINY_SEGMENTS_THRESHOLD),
        "build graph from merging " + SEGMENTS + " graphs");
  }

  /**
   * The merged graph is built by {@code HnswConcurrentMergeBuilder} workers ({@code numMergeWorkers
   * > 1}), which must forward the abort check to every worker. The first worker to insert a node
   * trips the forwarded check.
   */
  public void testRollbackDuringConcurrentMerge() throws Exception {
    ExecutorService mergeExec =
        Executors.newFixedThreadPool(2, new NamedThreadFactory("hnsw-merge-worker"));
    try {
      doTestRollbackDuringMerge(
          true,
          new Lucene99HnswVectorsFormat(16, BEAM_WIDTH, 2, mergeExec, TINY_SEGMENTS_THRESHOLD),
          "build graph from " + LIVE_DOCS_AFTER_DELETES + " vectors, with 2 workers");
    } finally {
      mergeExec.shutdown();
      assertTrue(mergeExec.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  private void doTestRollbackDuringMerge(
      boolean withDeletes, KnnVectorsFormat format, String expectedBuildStart) throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig cfg = new IndexWriterConfig();
      cfg.setCodec(TestUtil.alwaysKnnVectorsFormat(format));
      cfg.setMergePolicy(NoMergePolicy.INSTANCE);
      try (IndexWriter w = new IndexWriter(dir, cfg)) {
        Random r = random();
        int id = 0;
        for (int s = 0; s < SEGMENTS; s++) {
          for (int i = 0; i < DOCS_PER_SEGMENT; i++, id++) {
            Document doc = new Document();
            doc.add(new StringField("id", Integer.toString(id), Field.Store.NO));
            float[] v = new float[DIM];
            for (int j = 0; j < DIM; j++) {
              v[j] = r.nextFloat();
            }
            doc.add(new KnnFloatVectorField("v", v));
            w.addDocument(doc);
          }
          w.flush();
        }
        if (withDeletes) {
          // delete half of every segment, above the 40% base-graph eligibility threshold
          for (int d = 0; d < id; d += 2) {
            w.deleteDocuments(new Term("id", Integer.toString(d)));
          }
        }
        w.commit();
      }

      CountDownLatch buildStarted = new CountDownLatch(1);
      CountDownLatch mergeAborted = new CountDownLatch(1);
      AtomicBoolean releasedAfterAbort = new AtomicBoolean();
      List<String> hnswMessages = new ArrayList<>();
      InfoStream latching =
          new InfoStream() {
            @Override
            public void message(String component, String message) {
              if ("HNSW".equals(component)) {
                synchronized (hnswMessages) {
                  hnswMessages.add(message);
                }
                if (message.startsWith("build graph") && buildStarted.getCount() > 0) {
                  buildStarted.countDown();
                  // hold the merge thread until the merge is marked aborted, so the abort
                  // always lands mid-build
                  try {
                    releasedAfterAbort.set(mergeAborted.await(2, TimeUnit.MINUTES));
                  } catch (InterruptedException e) {
                    throw new ThreadInterruptedException(e);
                  }
                }
              } else if ("IW".equals(component) && message.startsWith("now wait for")) {
                // IndexWriter#abortMerges emits this after marking every running merge aborted
                mergeAborted.countDown();
              }
            }

            @Override
            public boolean isEnabled(String component) {
              return "HNSW".equals(component) || "IW".equals(component);
            }

            @Override
            public void close() {}
          };

      IndexWriterConfig cfg2 = new IndexWriterConfig();
      cfg2.setCodec(TestUtil.alwaysKnnVectorsFormat(format));
      cfg2.setMergeScheduler(new ConcurrentMergeScheduler());
      cfg2.setInfoStream(latching);
      IndexWriter w2 = new IndexWriter(dir, cfg2);
      AtomicReference<Throwable> mergeFailure = new AtomicReference<>();
      Thread merger =
          new Thread(
              () -> {
                try {
                  w2.forceMerge(1);
                } catch (Throwable t) {
                  mergeFailure.set(t);
                }
              });
      merger.start();
      try {
        assertTrue(
            "HNSW graph construction never started", buildStarted.await(120, TimeUnit.SECONDS));
        w2.rollback();
        assertTrue("merge thread was not released by the abort signal", releasedAfterAbort.get());
        synchronized (hnswMessages) {
          String buildStart = null;
          for (String message : hnswMessages) {
            if (message.startsWith("build graph")) {
              buildStart = message;
              break;
            }
          }
          assertNotNull("no graph build started", buildStart);
          assertTrue(
              "merge took an unexpected graph build path: " + buildStart,
              buildStart.startsWith(expectedBuildStart));
          for (String message : hnswMessages) {
            assertFalse(
                "HNSW graph build ran to completion despite the aborted merge: " + message,
                message.startsWith("addVectors [") || message.startsWith("merge completed:"));
          }
        }
      } finally {
        merger.join(TimeUnit.MINUTES.toMillis(5));
        assertFalse("merge thread did not terminate", merger.isAlive());
      }
    }
  }
}
