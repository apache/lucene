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

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.InfoStream;

/**
 * Concurrent HNSW graph merges lose their parallelism when they start while sibling merges hold
 * {@link ConcurrentMergeScheduler}'s intra-merge threads, and cannot recover it when those threads
 * free up.
 *
 * <p>{@link HnswConcurrentMergeBuilder} hands work out through one shared batch counter and submits
 * all of its workers through {@code TaskExecutor#invokeAll} before any of them runs. {@code
 * ConcurrentMergeScheduler.CachedExecutor} runs a command on the calling thread whenever {@code
 * maxThreadCount - mergeThreads.size() - 1 <= 0}. When that happens to the first submission, the
 * worker drains the counter before the submit loop can offer the remaining workers, so those
 * workers arrive to find no work and the merge is single-threaded for its whole duration.
 *
 * <p>Nothing here substitutes for Lucene's own machinery: a real {@link IndexWriter} runs real
 * merges under a stock {@link ConcurrentMergeScheduler}, and every measurement comes from {@link
 * InfoStream} messages that {@link HnswConcurrentMergeBuilder} already emits. Merges must exceed
 * {@code ConcurrentMergeScheduler#MIN_BIG_MERGE_MB} to be given intra-merge threads at all, which
 * is why this has to move enough data to be a {@code @Monster}.
 *
 * <p>Whether a given merge loses its threads depends on how the scheduler happens to interleave
 * them, so this is repeated and every iteration must reproduce it.
 */
@LuceneTestCase.Monster("indexes ~380MB of vectors and runs multi-second HNSW graph merges")
public class TestHnswConcurrentMergeSubmission extends LuceneTestCase {

  private static final int DIMS = 1024;
  private static final int SEGMENTS = 12;
  private static final int DOCS_PER_SEGMENT = 8_192;
  private static final int MERGE_WORKERS = 8;
  private static final int MAX_THREAD_COUNT = 6;
  private static final double MIN_BIG_MERGE_MB = 50.0;
  private static final String FIELD = "vec";

  /** Free-thread window below which we do not expect a merge to have enlisted help. */
  private static final long MIN_SOLE_MS = 500;

  @Repeat(iterations = 3)
  public void testMergeUsesIntraMergeThreadsThatFreeUpMidMerge() throws Exception {
    MergeRecorder recorder = new MergeRecorder();
    try (Directory dir = newFSDirectory(createTempDir())) {
      buildSegments(dir);

      ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();
      cms.setMaxMergesAndThreads(MAX_THREAD_COUNT * 2, MAX_THREAD_COUNT);
      TieredMergePolicy mergePolicy = new TieredMergePolicy();
      mergePolicy.setSegmentsPerTier(2);
      mergePolicy.setMaxMergedSegmentMB(Integer.MAX_VALUE);

      IndexWriterConfig iwc = new IndexWriterConfig();
      iwc.setCodec(concurrentMergeCodec());
      iwc.setMergePolicy(mergePolicy);
      iwc.setMergeScheduler(cms);
      iwc.setInfoStream(recorder);

      IndexWriter writer = new IndexWriter(dir, iwc);
      writer.maybeMerge();
      writer.close(); // waits for the background merges
    }

    List<MergeRecorder.Merge> eligible = recorder.eligibleMerges(DIMS, MIN_BIG_MERGE_MB);
    assumeTrue(
        "the merge policy did not produce at least two merges over MIN_BIG_MERGE_MB, so no merge ever "
            + "contended for intra-merge threads and there is nothing to assert",
        eligible.size() >= 2);

    // A merge that outlives its siblings gets its intra-merge budget back, because CachedExecutor
    // derives availability from maxThreadCount - mergeThreads.size() - 1. Any merge with a
    // meaningful stretch of the run to itself should therefore end up using more than the merge
    // thread.
    for (MergeRecorder.Merge merge : eligible) {
      long soleFrom = merge.startMs;
      for (MergeRecorder.Merge other : recorder.merges()) {
        if (other != merge && other.endMs > soleFrom && other.startMs < merge.endMs) {
          soleFrom = Math.max(soleFrom, other.endMs);
        }
      }
      long soleMs = merge.endMs - soleFrom;
      if (soleMs < MIN_SOLE_MS) {
        continue; // never had threads to itself for long enough to expect it to pick any up
      }
      assertTrue(
          String.format(
              Locale.ROOT,
              "merge of %d vectors (%.1fMB) requested %d workers and ran %dms as the only merge in "
                  + "the system, so intra-merge threads were free, yet it did all its work on %d "
                  + "thread(s) and reported %.2fx:%s",
              merge.vectors,
              MergeRecorder.mb(merge, DIMS),
              merge.requestedWorkers,
              soleMs,
              merge.batchesByThread.size(),
              merge.reportedConcurrency,
              recorder.summary(DIMS)),
          merge.batchesByThread.size() > 1);
    }
  }

  private void buildSegments(Directory dir) throws Exception {
    IndexWriterConfig iwc = new IndexWriterConfig();
    iwc.setCodec(concurrentMergeCodec());
    iwc.setMergePolicy(NoMergePolicy.INSTANCE);
    iwc.setRAMBufferSizeMB(IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB * 64);
    try (IndexWriter writer = new IndexWriter(dir, iwc)) {
      float[] buffer = new float[DIMS];
      for (int segment = 0; segment < SEGMENTS; segment++) {
        for (int doc = 0; doc < DOCS_PER_SEGMENT; doc++) {
          for (int dim = 0; dim < DIMS; dim++) {
            buffer[dim] = random().nextFloat();
          }
          Document document = new Document();
          document.add(
              new KnnFloatVectorField(FIELD, buffer, VectorSimilarityFunction.DOT_PRODUCT));
          writer.addDocument(document);
        }
        writer.commit(); // one segment per batch, so the merge policy pairs them predictably
      }
    }
  }

  /** The default codec asks for one merge worker, which never reaches the concurrent merger. */
  private static Lucene104Codec concurrentMergeCodec() {
    return new Lucene104Codec() {
      private final KnnVectorsFormat format =
          new Lucene99HnswVectorsFormat(
              Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN,
              Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH,
              MERGE_WORKERS,
              null); // null mergeExec, so the codec uses MergeState#intraMergeTaskExecutor

      @Override
      public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return format;
      }
    };
  }

  /**
   * Reconstructs what each merge achieved from the {@link InfoStream} messages {@link
   * HnswConcurrentMergeBuilder} emits. Those messages are emitted from the worker thread, so the
   * emitting thread identifies who did the work.
   */
  private static class MergeRecorder extends InfoStream {

    static class Merge {
      int vectors;
      int requestedWorkers;
      long startMs;
      long endMs;
      double reportedConcurrency;
      final Map<String, Integer> batchesByThread = new ConcurrentHashMap<>();
    }

    private final Map<String, Merge> inFlight = new ConcurrentHashMap<>();
    private final List<Merge> completed = new ArrayList<>();

    @Override
    public synchronized void message(String component, String message) {
      String thread = Thread.currentThread().getName();
      long now = System.nanoTime() / 1_000_000L;
      if (message.startsWith("build graph from") && message.contains("workers")) {
        Merge merge = new Merge();
        merge.startMs = now;
        String[] parts = message.split(" ");
        merge.vectors = Integer.parseInt(parts[3]);
        merge.requestedWorkers = Integer.parseInt(parts[parts.length - 2]);
        inFlight.put(thread, merge);
      } else if (message.startsWith("addVectors")) {
        Merge merge = inFlight.get(thread);
        if (merge == null && inFlight.size() == 1) {
          // a helper thread, unambiguous only while a single merge is building
          merge = inFlight.values().iterator().next();
        }
        if (merge != null) {
          merge.batchesByThread.merge(thread, 1, Integer::sum);
        }
      } else if (message.startsWith("merge completed")) {
        Merge merge = inFlight.remove(thread);
        if (merge != null) {
          merge.endMs = now;
          int tail = message.lastIndexOf(", ");
          merge.reportedConcurrency =
              Double.parseDouble(
                  message.substring(tail + 2).replace("x effective concurrency", "").trim());
          completed.add(merge);
        }
      }
    }

    synchronized List<Merge> merges() {
      List<Merge> copy = new ArrayList<>(completed);
      copy.sort(Comparator.comparingLong(m -> m.startMs));
      return copy;
    }

    List<Merge> eligibleMerges(int dims, double minMB) {
      List<Merge> eligible = new ArrayList<>();
      for (Merge merge : merges()) {
        if (mb(merge, dims) >= minMB) {
          eligible.add(merge);
        }
      }
      return eligible;
    }

    static double mb(Merge merge, int dims) {
      return merge.vectors * (double) dims * Float.BYTES / (1024 * 1024);
    }

    String summary(int dims) {
      StringBuilder sb = new StringBuilder();
      List<Merge> all = merges();
      long base = all.isEmpty() ? 0 : all.get(0).startMs;
      for (Merge merge : all) {
        sb.append(
            String.format(
                Locale.ROOT,
                "%n  %d vectors, %.1fMB, %d->%dms, requested %d workers, reported %.2fx, threads %s",
                merge.vectors,
                mb(merge, dims),
                merge.startMs - base,
                merge.endMs - base,
                merge.requestedWorkers,
                merge.reportedConcurrency,
                merge.batchesByThread));
      }
      return sb.toString();
    }

    @Override
    public boolean isEnabled(String component) {
      return HnswGraphBuilder.HNSW_COMPONENT.equals(component);
    }

    @Override
    public void close() {}
  }
}
