/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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
import java.util.Locale;
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
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.MergePolicy.OneMerge;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.hnsw.HnswGraphBuilder;
import org.apache.lucene.util.hnsw.NeighborQueue;
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
 * One {@code forceMerge(1)} of K frozen HNSW segments: join-set vs individual insert.
 *
 * <p>Indexing is {@code @Setup}. Timed region is copy + forceMerge(1) + recall@10 + close. Pin one
 * {@code segmentType} per JVM — two types share no Trial state and would index twice. HNSW
 * wall-clock is the InfoStream {@code merge completed} line; JMH ms includes recall.
 *
 * <p>{@code clean} (0% deletes) on {@code sequential}: {@code IncrementalHnswGraphMerger} join-set
 * ({@code "build graph from merging K graphs"}). {@code dirty} (25% deletes, under the 40% base
 * cutoff) is not a full rebuild: largest graph is still the base, non-base graphs are dropped from
 * {@code graphReaders}, remaining live nodes insert one-by-one ({@code "merging 1 graphs"}). Full
 * rebuild ({@code "build graph from X vectors"}) needs every graph over 40% deletes.
 *
 * <p>{@code concurrent} uses {@code ConcurrentHnswMerger}: copy largest, insert the rest. No
 * join-set. Needs {@code ConcurrentMergeScheduler} so intra-merge workers are not SameThread. CMS
 * returns SameThread when estimated merge bytes are under 50MB — default 5 × 100k × 128 is above
 * that; tiny {@code -p} values silently lose workers.
 *
 * <p>Run with:
 *
 * <pre>
 *   ./gradlew :lucene:benchmark-jmh:assemble
 *   java -jar lucene/benchmark-jmh/build/benchmarks/lucene-benchmark-jmh-*.jar \
 *     KnnJoinSetMergeBenchmark -f 1 -wi 0 -i 1 -foe false \
 *     -p segmentType=clean -p numSegments=5 -p vectorsPerSegment=100000 \
 *     -p dim=128 -p mergerType=sequential
 *   java -jar lucene/benchmark-jmh/build/benchmarks/lucene-benchmark-jmh-*.jar \
 *     KnnJoinSetMergeBenchmark -f 1 -wi 0 -i 1 -foe false \
 *     -p segmentType=clean -p mergerType=concurrent
 * </pre>
 */
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 0)
@Measurement(iterations = 1)
@Fork(
    value = 1,
    jvmArgsAppend = {"-Xmx8g", "-Xms8g", "-XX:+AlwaysPreTouch"})
public class KnnJoinSetMergeBenchmark {

  private static final int MAX_CONN = 16;
  private static final int CONCURRENT_WORKERS = 8;
  private static final int RECALL_QUERIES = 100;
  private static final int RECALL_K = 10;
  private static final long RECALL_SEED = 12345L;

  @Param({"clean"})
  public String segmentType;

  @Param({"5"})
  public int numSegments;

  @Param({"100000"})
  public int vectorsPerSegment;

  @Param({"128"})
  public int dim;

  @Param({"100"})
  public int beamWidth;

  @Param({"sequential"})
  public String mergerType;

  private Path root;
  private Path frozenIndex;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    switch (segmentType) {
      case "clean":
      case "dirty":
        break;
      default:
        throw new IllegalArgumentException("unknown segmentType: " + segmentType);
    }
    switch (mergerType) {
      case "sequential":
      case "concurrent":
        break;
      default:
        throw new IllegalArgumentException("unknown mergerType: " + mergerType);
    }
    root = Files.createTempDirectory("knnJoinSetMergeBenchmark");
    frozenIndex = root.resolve("frozen");
    buildIndex(frozenIndex);
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    IOUtils.rm(root);
  }

  @Benchmark
  public int forceMergeSinglePass() throws IOException {
    Path runPath = Files.createTempDirectory(root, "run");
    MergeStats stats = new MergeStats();
    MergeScheduler scheduler = newScheduler(stats);
    try {
      copyIndex(frozenIndex, runPath);
      IndexWriterConfig iwc = new IndexWriterConfig();
      iwc.setCodec(codec(mergeWorkers()));
      iwc.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
      iwc.setMergeScheduler(scheduler);
      iwc.setMergePolicy(new TieredMergePolicy());
      iwc.setUseCompoundFile(false);
      iwc.setInfoStream(new HnswOnlyInfoStream());
      int checksum;
      int maxDoc;
      int numDocs;
      try (Directory dir = new MMapDirectory(runPath);
          IndexWriter w = new IndexWriter(dir, iwc)) {
        w.forceMerge(1);
        IndexWriter.DocStats docStats = w.getDocStats();
        maxDoc = docStats.maxDoc;
        numDocs = docStats.numDocs;
        checksum = maxDoc + 31 * numDocs;
      }
      int segments;
      float recall;
      try (Directory dir = new MMapDirectory(runPath);
          DirectoryReader reader = DirectoryReader.open(dir)) {
        segments = reader.leaves().size();
        if (segments != 1) {
          throw new IllegalStateException(
              "expected 1 leftover segment after forceMerge(1), got " + segments);
        }
        recall = measureRecall(reader);
      }
      if (stats.mergeCount.get() != 1 || stats.fanIn.equals(List.of(numSegments)) == false) {
        throw new IllegalStateException(
            "expected one "
                + numSegments
                + "-way merge, got merges="
                + stats.mergeCount.get()
                + " fanIn="
                + stats.fanIn);
      }
      return checksum;
    } finally {
      IOUtils.rm(runPath);
    }
  }

  private float measureRecall(DirectoryReader reader) throws IOException {
    IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setQueryCache(null);
    LeafReader leaf = reader.leaves().get(0).reader();
    FloatVectorValues values = leaf.getFloatVectorValues("vec");
    if (values == null) {
      throw new IllegalStateException("no vec field after merge");
    }
    int size = values.size();
    if (size != reader.numDocs()) {
      throw new IllegalStateException(
          "vec size=" + size + " numDocs=" + reader.numDocs() + " after forceMerge(1)");
    }
    float[][] corpus = new float[size][];
    int[] docs = new int[size];
    KnnVectorValues.DocIndexIterator it = values.iterator();
    int n = 0;
    for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
      docs[n] = doc;
      corpus[n] = values.vectorValue(it.index()).clone();
      n++;
    }
    if (n != size) {
      throw new IllegalStateException("vector iterator size=" + n + " values.size=" + size);
    }
    Random random = new Random(RECALL_SEED);
    NeighborQueue brute = new NeighborQueue(RECALL_K, false);
    float[] query = new float[dim];
    // Collector k is HNSW search-list size. k=10 on a beamWidth=100 graph under-explores
    // and prints ~0.08 "recall" that is not graph quality.
    int searchK = Math.max(RECALL_K, beamWidth);
    double hits = 0d;
    for (int q = 0; q < RECALL_QUERIES; q++) {
      fillUnitVector(random, query);
      brute.clear();
      for (int i = 0; i < size; i++) {
        brute.insertWithOverflow(
            docs[i], VectorSimilarityFunction.DOT_PRODUCT.compare(query, corpus[i]));
      }
      TopDocs knn = searcher.search(new KnnFloatVectorQuery("vec", query, searchK), searchK);
      int[] bruteDocs = brute.nodes();
      int found = 0;
      int nReturned = Math.min(RECALL_K, knn.scoreDocs.length);
      for (int r = 0; r < nReturned; r++) {
        int doc = knn.scoreDocs[r].doc;
        for (int b = 0; b < bruteDocs.length; b++) {
          if (bruteDocs[b] == doc) {
            found++;
            break;
          }
        }
      }
      hits += found;
    }
    return (float) (hits / (RECALL_QUERIES * (double) RECALL_K));
  }

  private void buildIndex(Path dest) throws IOException {
    Files.createDirectories(dest);
    IndexWriterConfig iwc = new IndexWriterConfig();
    iwc.setCodec(codec(1));
    iwc.setMergePolicy(NoMergePolicy.INSTANCE);
    iwc.setMergeScheduler(new SerialMergeScheduler());
    iwc.setMaxBufferedDocs(vectorsPerSegment);
    iwc.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    iwc.setUseCompoundFile(false);
    Random random = new Random(42);
    float[] vec = new float[dim];
    int total = numSegments * vectorsPerSegment;
    try (Directory dir = new MMapDirectory(dest);
        IndexWriter w = new IndexWriter(dir, iwc)) {
      Document doc = new Document();
      StringField idField = new StringField("id", "", Field.Store.NO);
      KnnFloatVectorField vecField =
          new KnnFloatVectorField("vec", vec, VectorSimilarityFunction.DOT_PRODUCT);
      doc.add(idField);
      doc.add(vecField);
      for (int i = 0; i < total; i++) {
        idField.setStringValue(Integer.toString(i));
        fillUnitVector(random, vec);
        vecField.setVectorValue(vec.clone());
        w.addDocument(doc);
      }
      if ("dirty".equals(segmentType)) {
        applyDeletes(w);
      }
      w.commit();
    }
    try (Directory dir = new MMapDirectory(dest);
        DirectoryReader reader = DirectoryReader.open(dir)) {
      int frozenLeaves = reader.leaves().size();
      if (frozenLeaves != numSegments) {
        throw new IllegalStateException(
            "expected " + numSegments + " frozen leaves, got " + frozenLeaves);
      }
    }
  }

  private void applyDeletes(IndexWriter w) throws IOException {
    // 25% deletes, under IncrementalHnswGraphMerger's 40% base-graph cutoff.
    for (int s = 0; s < numSegments; s++) {
      int start = s * vectorsPerSegment;
      List<Term> terms = new ArrayList<>();
      for (int i = 0; i < vectorsPerSegment; i++) {
        int id = start + i;
        if (id % 4 == 0) {
          terms.add(new Term("id", Integer.toString(id)));
        }
      }
      w.deleteDocuments(terms.toArray(new Term[0]));
    }
  }

  private int mergeWorkers() {
    switch (mergerType) {
      case "sequential":
        return 1;
      case "concurrent":
        return CONCURRENT_WORKERS;
      default:
        throw new IllegalArgumentException("unknown mergerType: " + mergerType);
    }
  }

  private MergeScheduler newScheduler(MergeStats stats) {
    switch (mergerType) {
      case "sequential":
        return new CountingSerialMergeScheduler(stats);
      case "concurrent":
        CountingConcurrentMergeScheduler cms = new CountingConcurrentMergeScheduler(stats);
        // CMS intra-merge pool is maxThreadCount - mergeThreads - 1; +2 leaves 8 workers.
        cms.setMaxMergesAndThreads(CONCURRENT_WORKERS + 2, CONCURRENT_WORKERS + 2);
        return cms;
      default:
        throw new IllegalArgumentException("unknown mergerType: " + mergerType);
    }
  }

  private Lucene104Codec codec(int workers) {
    final int maxConn = MAX_CONN;
    final int beam = beamWidth;
    return new Lucene104Codec() {
      private final KnnVectorsFormat knn =
          new Lucene99HnswVectorsFormat(maxConn, beam, workers, null);

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

  private static void fillUnitVector(Random random, float[] v) {
    float sumSquares = 0f;
    for (int i = 0; i < v.length; i++) {
      float x = random.nextFloat() * 2 - 1;
      v[i] = x;
      sumSquares += x * x;
    }
    float norm = (float) Math.sqrt(sumSquares);
    for (int i = 0; i < v.length; i++) {
      v[i] /= norm;
    }
  }

  private static final class MergeStats {
    final AtomicInteger mergeCount = new AtomicInteger();
    final List<Integer> fanIn = new ArrayList<>();
  }

  private static final class CountingSerialMergeScheduler extends SerialMergeScheduler {
    private final MergeStats stats;

    CountingSerialMergeScheduler(MergeStats stats) {
      this.stats = stats;
    }

    @Override
    public synchronized void merge(MergeSource mergeSource, MergeTrigger trigger)
        throws IOException {
      super.merge(new CountingMergeSource(mergeSource, stats.mergeCount, stats.fanIn), trigger);
    }
  }

  private static final class CountingConcurrentMergeScheduler extends ConcurrentMergeScheduler {
    private final MergeStats stats;

    CountingConcurrentMergeScheduler(MergeStats stats) {
      this.stats = stats;
    }

    @Override
    public void merge(MergeSource mergeSource, MergeTrigger trigger) throws IOException {
      super.merge(new CountingMergeSource(mergeSource, stats.mergeCount, stats.fanIn), trigger);
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

  /** HNSW graph-build lines only. Full IW InfoStream drowns the join-set vs insert signature. */
  private static final class HnswOnlyInfoStream extends InfoStream {
    @Override
    public boolean isEnabled(String component) {
      return HnswGraphBuilder.HNSW_COMPONENT.equals(component);
    }

    @Override
    public void message(String component, String message) {
      // enabled but silent — forces HNSW merge to compute and log counters
    }

    @Override
    public void close() {}
  }
}
