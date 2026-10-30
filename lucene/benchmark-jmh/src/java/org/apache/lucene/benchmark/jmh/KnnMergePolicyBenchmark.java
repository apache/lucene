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
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.sandbox.index.ProactiveCleanMergePolicy;
import org.apache.lucene.store.Directory;
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
 * Natural flush+merge cost of HNSW indexing under {@link TieredMergePolicy} vs {@link
 * ProactiveCleanMergePolicy}. {@code deletePct=0} must match {@code tmp} (append-only bypass). The
 * expected win is at {@code 20} / {@code 35}. {@link #indexThenForceMerge} must match across
 * policies (forced merges are not intercepted).
 *
 * <p>Indexing is two-phase: the first half of the documents are flushed, deletes are applied to
 * those older docs and flushed, then the second half is appended. That leaves a mix of dirty and
 * clean segments so {@code proactive} can fire. {@code deletePct=0} skips deletes (append-only
 * bypass).
 *
 * <p>Run with:
 *
 * <pre>
 *   ./gradlew clean :lucene:benchmark-jmh:assemble
 *   java -jar lucene/benchmark-jmh/build/benchmarks/lucene-benchmark-jmh-*.jar KnnMergePolicyBenchmark -f 1 -wi 1 -i 3 -foe false
 * </pre>
 */
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
@Fork(
    value = 1,
    jvmArgsAppend = {"-Xmx8g", "-Xms8g", "-XX:+AlwaysPreTouch"})
public class KnnMergePolicyBenchmark {

  private static final int MAX_CONN = 16;
  private static final int BEAM_WIDTH = 100;

  /** Batches large enough that vectors alone exceed TMP's 16MB floor (40k × 128 × 4 = 20MB). */
  private static final int DOCS_PER_FLUSH = 40_000;

  @Param({"tmp", "proactive"})
  public String policy;

  @Param({"0", "20", "35"})
  public int deletePct;

  @Param({"1", "8"})
  public int numMergeWorkers;

  @Param({"240000"})
  public int docCount;

  @Param({"128"})
  public int dim;

  private float[][] vectors;
  private Path path;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    Random random = new Random(42);
    vectors = new float[docCount][];
    for (int i = 0; i < docCount; i++) {
      vectors[i] = randomUnitVector(dim, random);
    }
    path = Files.createTempDirectory("knnMergePolicyBenchmark");
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    IOUtils.rm(path);
  }

  @Benchmark
  public int indexWithNaturalMerges() throws IOException {
    return index(false);
  }

  @Benchmark
  public int indexThenForceMerge() throws IOException {
    return index(true);
  }

  private int index(boolean forceMerge) throws IOException {
    Path runPath = Files.createTempDirectory(path, policy);
    CountingMergeScheduler scheduler = new CountingMergeScheduler();
    try (Directory dir = new MMapDirectory(runPath)) {
      IndexWriterConfig iwc = new IndexWriterConfig();
      iwc.setCodec(codec());
      iwc.setMergePolicy(mergePolicy());
      iwc.setMergeScheduler(scheduler);
      iwc.setMaxBufferedDocs(DOCS_PER_FLUSH);
      iwc.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
      iwc.setUseCompoundFile(false);
      int checksum;
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        Document doc = new Document();
        StringField idField = new StringField("id", "", Field.Store.NO);
        KnnFloatVectorField vecField =
            new KnnFloatVectorField("vec", new float[dim], VectorSimilarityFunction.DOT_PRODUCT);
        doc.add(idField);
        doc.add(vecField);
        int mid = docCount / 2;
        addDocs(w, doc, idField, vecField, 0, mid);
        applyDeletes(w, mid);
        w.flush();
        addDocs(w, doc, idField, vecField, mid, docCount);
        if (forceMerge) {
          w.forceMerge(1);
        }
        checksum = w.getDocStats().maxDoc;
      }
      int segments;
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        segments = reader.leaves().size();
      }
      System.out.println(
          "policy="
              + policy
              + " deletePct="
              + deletePct
              + " workers="
              + numMergeWorkers
              + " forceMerge="
              + forceMerge
              + " merges="
              + scheduler.mergeCount.get()
              + " segments="
              + segments);
      return checksum;
    } finally {
      IOUtils.rm(runPath);
    }
  }

  private void addDocs(
      IndexWriter w,
      Document doc,
      StringField idField,
      KnnFloatVectorField vecField,
      int from,
      int to)
      throws IOException {
    for (int i = from; i < to; i++) {
      idField.setStringValue(Integer.toString(i));
      vecField.setVectorValue(vectors[i]);
      w.addDocument(doc);
    }
  }

  private void applyDeletes(IndexWriter w, int cutoff) throws IOException {
    if (deletePct == 0) {
      return;
    }
    int threshold = deletePct * 2;
    int n = 0;
    for (int i = 0; i < cutoff; i++) {
      if ((i % 100) < threshold) {
        n++;
      }
    }
    Term[] terms = new Term[n];
    int upto = 0;
    for (int i = 0; i < cutoff; i++) {
      if ((i % 100) < threshold) {
        terms[upto] = new Term("id", Integer.toString(i));
        upto++;
      }
    }
    w.deleteDocuments(terms);
  }

  private MergePolicy mergePolicy() {
    TieredMergePolicy tmp = new TieredMergePolicy();
    switch (policy) {
      case "tmp":
        return tmp;
      case "proactive":
        return new ProactiveCleanMergePolicy(tmp);
      default:
        throw new IllegalArgumentException("unknown policy: " + policy);
    }
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

  private static float[] randomUnitVector(int dim, Random random) {
    float[] v = new float[dim];
    float sumSquares = 0f;
    for (int i = 0; i < dim; i++) {
      float x = random.nextFloat() * 2 - 1;
      v[i] = x;
      sumSquares += x * x;
    }
    float norm = (float) Math.sqrt(sumSquares);
    for (int i = 0; i < dim; i++) {
      v[i] /= norm;
    }
    return v;
  }

  private static final class CountingMergeScheduler extends ConcurrentMergeScheduler {
    final AtomicInteger mergeCount = new AtomicInteger();

    @Override
    public void merge(MergeSource mergeSource, MergeTrigger trigger) throws IOException {
      super.merge(new CountingMergeSource(mergeSource, mergeCount), trigger);
    }
  }

  private static final class CountingMergeSource implements MergeScheduler.MergeSource {
    private final MergeScheduler.MergeSource in;
    private final AtomicInteger mergeCount;

    CountingMergeSource(MergeScheduler.MergeSource in, AtomicInteger mergeCount) {
      this.in = in;
      this.mergeCount = mergeCount;
    }

    @Override
    public OneMerge getNextMerge() {
      OneMerge merge = in.getNextMerge();
      if (merge != null) {
        mergeCount.incrementAndGet();
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
