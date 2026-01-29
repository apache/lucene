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
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.TopDocs;
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

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(
    value = 1,
    jvmArgsAppend = {"-Xmx4g", "-Xms4g", "-XX:+AlwaysPreTouch"})
@org.apache.lucene.util.SuppressForbidden(reason = "Benchmark code needs to print to stdout/stderr")
public class SpannVsHNSWBenchmark {

  @Param({"128"})
  public int dim;

  @Param({"10000"})
  public int numDocs;

  private Directory hnswDir;
  private Directory spannDir;
  private DirectoryReader hnswReader;
  private DirectoryReader spannReader;
  private IndexSearcher hnswSearcher;
  private IndexSearcher spannSearcher;
  private float[][] queries;
  private int queryIdx = 0;

  private int[][] groundTruth;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    Path tempDir = Files.createTempDirectory("SpannBenchmark");
    hnswDir = new MMapDirectory(tempDir.resolve("hnsw"));
    spannDir = new MMapDirectory(tempDir.resolve("spann"));

    Random random = new Random(42);
    float[][] vectors = new float[numDocs][dim];
    for (int i = 0; i < numDocs; i++) {
      vectors[i] = new float[dim];
      for (int j = 0; j < dim; j++) {
        vectors[i][j] = random.nextFloat();
      }
    }

    queries = new float[100][dim];
    for (int i = 0; i < 100; i++) {
      queries[i] = new float[dim];
      for (int j = 0; j < dim; j++) {
        queries[i][j] = random.nextFloat();
      }
    }

    // Brute Force
    System.out.println("Calculating Ground Truth for " + queries.length + " queries...");
    groundTruth = new int[queries.length][10];
    for (int q = 0; q < queries.length; q++) {
      groundTruth[q] = computeBruteForceTopK(vectors, queries[q], 10);
    }

    index(hnswDir, vectors, new Lucene104Codec());

    // Index SPANN using registered SpannBenchmarkCodec
    // Heuristic: 1000 vectors per partition, nProbe=10, replicationFactor=2
    int partitions = Math.max(10, numDocs / 1000);
    Codec spannCodec = new SpannBenchmarkCodec(10, partitions, 16384, 2);
    index(spannDir, vectors, spannCodec);

    hnswReader = DirectoryReader.open(hnswDir);
    hnswSearcher = new IndexSearcher(hnswReader);

    spannReader = DirectoryReader.open(spannDir);
    spannSearcher = new IndexSearcher(spannReader);

    System.out.println("\n--- RAM Usage Report ---");
    calculateRam("HNSW", ((MMapDirectory) hnswDir).getDirectory(), hnswReader);
    calculateRam("SPANN", ((MMapDirectory) spannDir).getDirectory(), spannReader);
    System.out.println("------------------------\n");
  }

  private int[] computeBruteForceTopK(float[][] vectors, float[] query, int k) {
    class ScoredDoc implements Comparable<ScoredDoc> {
      int id;
      float score;

      public ScoredDoc(int id, float score) {
        this.id = id;
        this.score = score;
      }

      @Override
      public int compareTo(ScoredDoc other) {
        return Float.compare(other.score, this.score);
      }
    }

    ScoredDoc[] all = new ScoredDoc[vectors.length];
    for (int i = 0; i < vectors.length; i++) {
      float distSq = 0;
      for (int d = 0; d < query.length; d++) {
        float diff = query[d] - vectors[i][d];
        distSq += diff * diff;
      }
      float luceneScore = 1.0f / (1.0f + distSq);
      all[i] = new ScoredDoc(i, luceneScore);
    }
    Arrays.sort(all);

    int[] topK = new int[k];
    for (int i = 0; i < k; i++) {
      topK[i] = all[i].id;
    }
    return topK;
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    System.out.println("\n--- Accuracy Report (Recall@10) ---");
    measureRecall("HNSW", hnswSearcher);
    measureRecall("SPANN", spannSearcher);
    System.out.println("----------------------------------\n");
    IOUtils.close(hnswReader, spannReader, hnswDir, spannDir);
  }

  private void measureRecall(String label, IndexSearcher searcher) throws IOException {
    int totalHits = 0;
    int totalRelevant = 0;

    for (int i = 0; i < queries.length; i++) {
      // Use k=100 for KnnFloatVectorQuery to ensure higher recall (effective ef=100)
      TopDocs results = searcher.search(new KnnFloatVectorQuery("field", queries[i], 100), 10);

      java.util.Set<Integer> resultIds = new java.util.HashSet<>();
      for (var sd : results.scoreDocs) {
        // We need to map docId back to vector index.
        // Since we indexed sequentially 0..N, docId is effectively the vector index
        resultIds.add(sd.doc);
      }

      for (int trueId : groundTruth[i]) {
        if (resultIds.contains(trueId)) {
          totalHits++;
        }
        totalRelevant++;
      }
    }

    double recall = (double) totalHits / totalRelevant;
    System.out.printf("%s Recall@10: %.4f%n", label, recall);
  }

  /**
   * Estimates memory footprint by aggregating heap usage from {@link
   * org.apache.lucene.util.Accountable} and categorizing disk usage by file structure.
   *
   * <p>We distinguish between "hot" structures (graph, vectors) that drive RSS and "cold" data
   * (postings) that relies on the OS page cache for efficient streaming.
   */
  private void calculateRam(String label, Path dir, DirectoryReader reader) {
    long heapRam = 0;
    try {
      for (LeafReaderContext context : reader.leaves()) {
        if (context.reader() instanceof CodecReader codecReader) {
          KnnVectorsReader vectorReader = codecReader.getVectorReader();
          if (vectorReader instanceof org.apache.lucene.util.Accountable accountable) {
            heapRam += accountable.ramBytesUsed();
          }
        }
      }

      System.out.println("___ File Listing for " + label + " ___");
      try (Stream<Path> walk = Files.walk(dir)) {
        walk.filter(Files::isRegularFile)
            .forEach(
                p ->
                    System.out.println(
                        "  " + p.getFileName() + " (" + p.toFile().length() + " bytes)"));
      }

      long totalDisk = calculateDirSize(dir);
      long graphSize = calculateExtensionSize(dir, ".vex");
      long vectorSize = calculateExtensionSize(dir, ".vec");
      long spannDataSize = calculateExtensionSize(dir, ".spad");
      long hotIndexSize = graphSize + vectorSize; // Structures that require random access / caching

      System.out.println("--- " + label + " Analysis ---");
      System.out.printf("Heap Usage (Java Objects):      %.2f MB %n", heapRam / 1024.0 / 1024.0);
      System.out.printf(
          "Hot Index Structure (RSS):      %.2f MB (.vex graph + .vec vectors)%n",
          hotIndexSize / 1024.0 / 1024.0);
      System.out.println("  - Graph (.vex):               " + graphSize + " bytes");
      System.out.println("  - Vectors (.vec):             " + vectorSize + " bytes");
      System.out.printf(
          "Cold Data (Streamed from Disk): %.2f MB (.spad)%n", spannDataSize / 1024.0 / 1024.0);
      System.out.printf("Total Index On-Disk:            %.2f MB%n", totalDisk / 1024.0 / 1024.0);
      System.out.println();
    } catch (IOException e) {
      System.err.println("Error calculating RAM for " + label + ": " + e.getMessage());
    }
  }

  private long calculateDirSize(Path path) throws IOException {
    try (Stream<Path> walk = Files.walk(path)) {
      return walk.filter(Files::isRegularFile)
          .mapToLong(
              p -> {
                try {
                  return Files.size(p);
                } catch (IOException _) {
                  return 0L;
                }
              })
          .sum();
    }
  }

  private long calculateExtensionSize(Path path, String extension) throws IOException {
    try (Stream<Path> walk = Files.walk(path)) {
      return walk.filter(p -> p.toString().endsWith(extension))
          .mapToLong(
              p -> {
                try {
                  return Files.size(p);
                } catch (IOException _) {
                  return 0L;
                }
              })
          .sum();
    }
  }

  private void index(Directory dir, float[][] vectors, Codec codec) throws IOException {
    IndexWriterConfig iwc = new IndexWriterConfig().setCodec(codec).setUseCompoundFile(false);
    try (IndexWriter writer = new IndexWriter(dir, iwc)) {
      for (float[] vector : vectors) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("field", vector, VectorSimilarityFunction.EUCLIDEAN));
        writer.addDocument(doc);
      }
      writer.forceMerge(1);
    }
  }

  @Benchmark
  public TopDocs searchHNSW() throws IOException {
    float[] query = queries[queryIdx % 100];
    queryIdx++;
    return hnswSearcher.search(new KnnFloatVectorQuery("field", query, 100), 10);
  }

  @Benchmark
  public TopDocs searchSPANN() throws IOException {
    float[] query = queries[queryIdx % 100];
    queryIdx++;
    return spannSearcher.search(new KnnFloatVectorQuery("field", query, 100), 10);
  }

  @Benchmark
  public void openReaderHNSW() throws IOException {
    try (DirectoryReader reader = DirectoryReader.open(hnswDir)) {
      reader.getVersion();
    }
  }

  @Benchmark
  public void openReaderSPANN() throws IOException {
    try (DirectoryReader reader = DirectoryReader.open(spannDir)) {
      reader.getVersion();
    }
  }

  public static void main(String[] args) throws Exception {
    org.openjdk.jmh.Main.main(args);
  }
}
