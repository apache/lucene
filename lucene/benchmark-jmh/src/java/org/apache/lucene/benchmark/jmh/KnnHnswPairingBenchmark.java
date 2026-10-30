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
package org.apache.lucene.benchmark.jmh;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergePolicy.OneMerge;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.sandbox.index.ProactiveCleanMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.IOUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Frozen-index HNSW merge cost. Dirty segments delete {@link #DIRTY_DELETE_PCT}% of docs, which is
 * above {@code IncrementalHnswGraphMerger}'s 40% base-graph cutoff: a dirty graph cannot be the
 * merge base. A sibling 0-delete graph still can; if every graph is over that cutoff the merge
 * rebuilds from scratch.
 *
 * <p>Pairing uses {@code forceMerge(1)} of exactly two segments. Policy comparison runs {@code
 * maybeMerge()} then {@code forceMerge(1)} so both land on one segment: natural selection still
 * goes through the policy, the finish step uses {@code findForcedMerges} (not intercepted). A lone
 * {@code forceMerge(1)} would ignore {@link ProactiveCleanMergePolicy}. Indexing is {@code @Setup};
 * the timed region is copy + merge + close.
 *
 * <p>Run with:
 *
 * <pre>
 *   ./gradlew :lucene:benchmark-jmh:assemble
 *   java -jar lucene/benchmark-jmh/build/benchmarks/lucene-benchmark-jmh-*.jar KnnHnswPairingBenchmark -f 1 -wi 0 -i 3 -foe false
 * </pre>
 */
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 0)
@Measurement(iterations = 3)
@Fork(
    value = 1,
    jvmArgsAppend = {"-Xmx8g", "-Xms8g", "-XX:+AlwaysPreTouch"})
public class KnnHnswPairingBenchmark {

  private static final int MAX_CONN = 16;
  private static final int BEAM_WIDTH = 100;
  private static final int DIM = 128;
  private static final int DOCS_PER_SEG = 20_000;

  /** Must be {@code > 40} so the dirty HNSW graph cannot be selected as merge base. */
  private static final int DIRTY_DELETE_PCT = 50;

  @Param({"1"})
  public int numMergeWorkers;

  private float[][] vectors;
  private Path root;
  private Path twoClean;
  private Path cleanPlusHeavy;
  private Path twoHeavy;
  private Path mixedSix;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    Random random = new Random(42);
    int maxDocs = 6 * DOCS_PER_SEG;
    vectors = new float[maxDocs][];
    for (int i = 0; i < maxDocs; i++) {
      vectors[i] = randomUnitVector(random);
    }
    root = Files.createTempDirectory("knnHnswPairingBenchmark");
    twoClean = root.resolve("twoClean");
    cleanPlusHeavy = root.resolve("cleanPlusHeavy");
    twoHeavy = root.resolve("twoHeavy");
    mixedSix = root.resolve("mixed6");
    buildIndex(twoClean, 2, 0);
    buildIndex(cleanPlusHeavy, 2, 1);
    buildIndex(twoHeavy, 2, 2);
    buildIndex(mixedSix, 6, 1);
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    IOUtils.rm(root);
  }

  /** forceMerge(1) of 2 zero-delete HNSW segments (join-set / largest-graph base). */
  @Benchmark
  public int mergeTwoClean() throws IOException {
    return mergeCopied(twoClean, true, null);
  }

  /**
   * forceMerge(1) of 1 clean + 1 50%-deleted segment. Dirty graph cannot be base; live dirty
   * vectors are inserted into the clean graph.
   */
  @Benchmark
  public int mergeCleanPlusHeavyDirty() throws IOException {
    return mergeCopied(cleanPlusHeavy, true, null);
  }

  /**
   * forceMerge(1) of 2 segments that are both 50% deleted. Neither graph can be base, so the merger
   * rebuilds from scratch.
   */
  @Benchmark
  public int mergeTwoHeavyDirty() throws IOException {
    return mergeCopied(twoHeavy, true, null);
  }

  /** maybeMerge then forceMerge(1) of 5 clean + 1 50%-deleted under over-budget TMP. */
  @Benchmark
  public int mergeToOneTmp() throws IOException {
    return mergeCopied(mixedSix, false, "tmp");
  }

  /** Same finish line under proactive-then-TMP. */
  @Benchmark
  public int mergeToOneProactive() throws IOException {
    return mergeCopied(mixedSix, false, "proactive");
  }

  private int mergeCopied(Path src, boolean pairingForceMerge, String policy) throws IOException {
    Path runPath = Files.createTempDirectory(root, "run");
    CountingMergeScheduler scheduler = new CountingMergeScheduler();
    try {
      copyIndex(src, runPath);
      IndexWriterConfig iwc = new IndexWriterConfig();
      iwc.setCodec(codec());
      iwc.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
      iwc.setMergeScheduler(scheduler);
      iwc.setUseCompoundFile(false);
      if (pairingForceMerge) {
        iwc.setMergePolicy(overBudgetTmp());
      } else {
        iwc.setMergePolicy(naturalPolicy(policy));
      }
      int checksum;
      try (Directory dir = new MMapDirectory(runPath);
          IndexWriter w = new IndexWriter(dir, iwc)) {
        if (pairingForceMerge) {
          w.forceMerge(1);
        } else {
          // Natural phase uses findMerges (policy). Finish uses findForcedMerges (inner TMP).
          w.maybeMerge();
          w.forceMerge(1);
        }
        checksum = w.getDocStats().maxDoc;
      }
      int segments;
      try (Directory dir = new MMapDirectory(runPath);
          DirectoryReader reader = DirectoryReader.open(dir)) {
        segments = reader.leaves().size();
      }
      System.out.println(
          "pairing="
              + src.getFileName()
              + " pairingForceMerge="
              + pairingForceMerge
              + " policy="
              + policy
              + " workers="
              + numMergeWorkers
              + " merges="
              + scheduler.mergeCount.get()
              + " fanIn="
              + scheduler.fanIn
              + " segments="
              + segments);
      return checksum;
    } finally {
      IOUtils.rm(runPath);
    }
  }

  private void buildIndex(Path dest, int numSegs, int dirtySegCount) throws IOException {
    Files.createDirectories(dest);
    IndexWriterConfig iwc = new IndexWriterConfig();
    iwc.setCodec(codec());
    iwc.setMergePolicy(NoMergePolicy.INSTANCE);
    iwc.setMergeScheduler(new SerialMergeScheduler());
    iwc.setMaxBufferedDocs(DOCS_PER_SEG);
    iwc.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    iwc.setUseCompoundFile(false);
    try (Directory dir = new MMapDirectory(dest);
        IndexWriter w = new IndexWriter(dir, iwc)) {
      Document doc = new Document();
      StringField idField = new StringField("id", "", Field.Store.NO);
      KnnFloatVectorField vecField =
          new KnnFloatVectorField("vec", new float[DIM], VectorSimilarityFunction.DOT_PRODUCT);
      doc.add(idField);
      doc.add(vecField);
      int total = numSegs * DOCS_PER_SEG;
      for (int i = 0; i < total; i++) {
        idField.setStringValue(Integer.toString(i));
        vecField.setVectorValue(vectors[i]);
        w.addDocument(doc);
      }
      if (dirtySegCount > 0) {
        int nDelete = DOCS_PER_SEG * DIRTY_DELETE_PCT / 100;
        for (int s = 0; s < dirtySegCount; s++) {
          int start = s * DOCS_PER_SEG;
          Term[] terms = new Term[nDelete];
          for (int i = 0; i < nDelete; i++) {
            terms[i] = new Term("id", Integer.toString(start + i));
          }
          w.deleteDocuments(terms);
        }
      }
      w.commit();
    }
  }

  private MergePolicy naturalPolicy(String policy) {
    TieredMergePolicy tmp = overBudgetTmp();
    switch (policy) {
      case "tmp":
        return tmp;
      case "proactive":
        ProactiveCleanMergePolicy proactive = new ProactiveCleanMergePolicy(tmp);
        proactive.setMinProactiveSegmentSize(0);
        return proactive;
      default:
        throw new IllegalArgumentException("unknown policy: " + policy);
    }
  }

  private static TieredMergePolicy overBudgetTmp() {
    TieredMergePolicy tmp = new TieredMergePolicy();
    tmp.setSegmentsPerTier(2);
    tmp.setTargetSearchConcurrency(1);
    return tmp;
  }

  private Lucene104Codec codec() {
    final int workers = numMergeWorkers;
    return new Lucene104Codec() {
      private final KnnVectorsFormat knn =
          new Lucene99HnswVectorsFormat(MAX_CONN, BEAM_WIDTH, workers, null);

      @Override
      public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return knn;
      }
    };
  }

  private static void copyIndex(Path src, Path dest) throws IOException {
    Files.createDirectories(dest);
    try (Directory from = FSDirectory.open(src);
        Directory to = FSDirectory.open(dest)) {
      for (String file : from.listAll()) {
        if ("write.lock".equals(file)) {
          continue;
        }
        to.copyFrom(from, file, file, IOContext.DEFAULT);
      }
    }
  }

  private static float[] randomUnitVector(Random random) {
    float[] v = new float[DIM];
    float sumSquares = 0f;
    for (int i = 0; i < DIM; i++) {
      float x = random.nextFloat() * 2 - 1;
      v[i] = x;
      sumSquares += x * x;
    }
    float norm = (float) Math.sqrt(sumSquares);
    for (int i = 0; i < DIM; i++) {
      v[i] /= norm;
    }
    return v;
  }

  private static final class CountingMergeScheduler extends ConcurrentMergeScheduler {
    final AtomicInteger mergeCount = new AtomicInteger();
    final List<Integer> fanIn = new ArrayList<>();

    @Override
    public void merge(MergeSource mergeSource, MergeTrigger trigger) throws IOException {
      super.merge(new CountingMergeSource(mergeSource, mergeCount, fanIn), trigger);
    }
  }

  private static final class CountingMergeSource implements MergeScheduler.MergeSource {
    private final MergeScheduler.MergeSource in;
    private final AtomicInteger mergeCount;
    private final List<Integer> fanIn;

    CountingMergeSource(
        MergeScheduler.MergeSource in, AtomicInteger mergeCount, List<Integer> fanIn) {
      this.in = in;
      this.mergeCount = mergeCount;
      this.fanIn = fanIn;
    }

    @Override
    public OneMerge getNextMerge() {
      OneMerge merge = in.getNextMerge();
      if (merge != null) {
        mergeCount.incrementAndGet();
        fanIn.add(merge.segments.size());
      }
      return merge;
    }

    @Override
    public void onMergeFinished(OneMerge merge) {
      in.onMergeFinished(merge);
    }

    @Override
    public boolean hasPendingMerges() {
      return in.hasPendingMerges();
    }

    @Override
    public void merge(OneMerge merge) throws IOException {
      in.merge(merge);
    }
  }
}
