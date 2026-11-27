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
package org.apache.lucene.util.hnsw;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.FilterMergePolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * A concurrent HNSW merge that starts while a sibling merge holds {@link
 * ConcurrentMergeScheduler}'s spare thread must use that thread once the sibling finishes.
 *
 * <p>The intra-merge executor runs a command on the calling thread unless {@code maxThreadCount -
 * mergeThreads.size() - 1} exceeds the threads it already lent. With a {@code maxThreadCount} of 3,
 * it has no spare thread while two merges run, and one once a single merge is left.
 *
 * <p>The vector merge inserts three batches of vectors. The sibling merge holds its merge thread
 * until the vector merge has run its first batch, then finishes. The vector merge must then recruit
 * a worker on the freed thread, and that worker must run the last batch.
 */
public class TestHnswConcurrentMergeRecruitment extends LuceneTestCase {

  private static final String FIELD = "vector";
  private static final int DIMS = 4;

  /** Three of the batches of 2048 vectors that {@link HnswConcurrentMergeBuilder} inserts. */
  private static final int VECTORS_PER_SEGMENT = 3 * 2048 / 2;

  /** High enough that the flushed segments build no graph, so the merge inserts every vector. */
  private static final int TINY_SEGMENTS_THRESHOLD = 500;

  public void testMergeRecruitsThreadThatSiblingMergeFrees() throws Exception {
    Merges merges = new Merges();
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc =
          new IndexWriterConfig()
              .setCodec(codec())
              .setMergePolicy(merges.policy)
              .setMergeScheduler(merges.scheduler)
              .setInfoStream(merges);
      try (IndexWriter writer = new IndexWriter(dir, iwc)) {
        // one document per segment for the sibling merge
        for (int i = 0; i < 2; i++) {
          writer.addDocument(new Document());
          writer.commit();
        }
        for (int s = 0; s < 2; s++) {
          for (int i = 0; i < VECTORS_PER_SEGMENT; i++) {
            Document doc = new Document();
            doc.add(
                new KnnFloatVectorField(FIELD, randomVector(), VectorSimilarityFunction.EUCLIDEAN));
            writer.addDocument(doc);
          }
          writer.commit();
        }
        merges.armed.set(true);
        writer.maybeMerge();
      } // waits for both merges
    }
    assertTrue(
        "the vector merge never ran a batch while the sibling merge held its thread",
        merges.siblingReleased);
    assertEquals(
        "no batch of the vector merge ran on the thread the sibling merge freed",
        0,
        merges.recruitRanBatch.getCount());
  }

  private static float[] randomVector() {
    float[] vector = new float[DIMS];
    for (int i = 0; i < DIMS; i++) {
      vector[i] = random().nextFloat();
    }
    return vector;
  }

  private static Lucene104Codec codec() {
    return new Lucene104Codec() {
      private final KnnVectorsFormat format =
          new Lucene99HnswVectorsFormat(
              Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN,
              Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH,
              2, // merge workers
              null, // use the scheduler's intra-merge executor
              TINY_SEGMENTS_THRESHOLD);

      @Override
      public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return format;
      }
    };
  }

  /**
   * Schedules the sibling merge and the vector merge together, and steps them through the
   * interleaving above from the {@link InfoStream} messages that {@link HnswGraphBuilder} logs
   * after each batch.
   */
  private static final class Merges extends InfoStream {
    final AtomicBoolean armed = new AtomicBoolean();
    final CountDownLatch siblingRunning = new CountDownLatch(1);
    final CountDownLatch releaseSibling = new CountDownLatch(1);
    final CountDownLatch recruitRanBatch = new CountDownLatch(1);
    final AtomicInteger mergeThreadBatches = new AtomicInteger();
    volatile MergePolicy.OneMerge sibling;
    volatile Thread siblingThread;
    volatile Thread vectorMergeThread;
    volatile boolean siblingReleased;

    /** Once armed, merges the segments of one document, and separately the vector segments. */
    final MergePolicy policy =
        new FilterMergePolicy(NoMergePolicy.INSTANCE) {
          @Override
          public MergeSpecification findMerges(
              MergeTrigger trigger, SegmentInfos infos, MergeContext context) {
            if (armed.compareAndSet(true, false) == false) {
              return null;
            }
            List<SegmentCommitInfo> plain = new ArrayList<>();
            List<SegmentCommitInfo> vectors = new ArrayList<>();
            for (SegmentCommitInfo info : infos) {
              (info.info.maxDoc() == 1 ? plain : vectors).add(info);
            }
            sibling = new OneMerge(plain);
            MergeSpecification spec = new MergeSpecification();
            spec.add(sibling);
            spec.add(new OneMerge(vectors));
            return spec;
          }
        };

    final ConcurrentMergeScheduler scheduler =
        new ConcurrentMergeScheduler() {
          {
            setMaxMergesAndThreads(3, 3);
          }

          @Override
          public Executor getIntraMergeExecutor(MergePolicy.OneMerge merge) {
            return intraMergeExecutor; // also for merges this small
          }

          @Override
          protected void doMerge(MergeSource mergeSource, MergePolicy.OneMerge merge)
              throws IOException {
            if (merge == sibling) {
              siblingThread = Thread.currentThread();
              siblingRunning.countDown();
              await(releaseSibling, "the vector merge to run a batch");
            } else {
              vectorMergeThread = Thread.currentThread();
            }
            super.doMerge(mergeSource, merge);
          }
        };

    @Override
    public void message(String component, String message) {
      if (message.startsWith("addVectors") == false) {
        return;
      }
      if (Thread.currentThread() != vectorMergeThread) {
        recruitRanBatch.countDown();
        return;
      }
      switch (mergeThreadBatches.incrementAndGet()) {
        case 1 -> {
          // the merge thread ran this batch alone, since the sibling holds the spare thread
          await(siblingRunning, "the sibling merge to start");
          releaseSibling.countDown();
          try {
            siblingThread.join();
          } catch (InterruptedException e) {
            throw new ThreadInterruptedException(e);
          }
          siblingReleased = true;
        }
        case 2 -> {
          // A worker recruited before this batch claimed the last one. Wait for it to run that
          // batch, rather than let this thread claim it first. Without recruitment this times out.
          try {
            recruitRanBatch.await(30, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            throw new ThreadInterruptedException(e);
          }
        }
        default -> {}
      }
    }

    private static void await(CountDownLatch latch, String what) {
      try {
        if (latch.await(30, TimeUnit.SECONDS) == false) {
          throw new AssertionError("timed out waiting for " + what);
        }
      } catch (InterruptedException e) {
        throw new ThreadInterruptedException(e);
      }
    }

    @Override
    public boolean isEnabled(String component) {
      return HnswGraphBuilder.HNSW_COMPONENT.equals(component);
    }

    @Override
    public void close() {}
  }
}
