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
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
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
 * Compares the cost of changing a stored embedding two ways:
 *
 * <ul>
 *   <li>{@link IndexWriter#updateFloatVectorValue} — the in-place vector update API: rewrites only
 *       the vector column and rebuilds the field's HNSW graph for the affected segment(s), leaving
 *       the rest of each document untouched.
 *   <li>{@link IndexWriter#updateDocument} — the status quo: delete-by-term + re-add the entire
 *       document (re-analyzing/re-posting/re-storing every field).
 * </ul>
 *
 * <p>A base index of {@code numDocs} documents is built ONCE per {@code (numDocs, dim,
 * otherFields)} and cached for the whole fork, then byte-copied into a fresh directory before each
 * measured shot ({@link Level#Iteration}), so each update starts from an identical clean base
 * without paying to rebuild (and, for eager, re-graph) the whole index each time. Each shot
 * re-embeds a batch of {@code batchSize} distinct existing documents and then {@code commit()}s
 * once, so the work (per-segment generation write / HNSW rebuild for the in-place path, or
 * delete+add+flush for the whole-document path) is actually flushed to disk and timed.
 *
 * <p>Parameters that drive the expected crossover:
 *
 * <ul>
 *   <li>{@code batchSize} — how many docs are re-embedded per commit. <b>Key:</b> the in-place path
 *       rewrites the field's entire flat vector column (and, when eager, rebuilds the whole graph)
 *       once per commit, so its cost scales with {@code numDocs} and is nearly <em>flat</em> in
 *       {@code batchSize}; the whole-document path re-indexes every field of every updated doc, so
 *       its cost scales <em>linearly</em> with {@code batchSize}. At small batches the
 *       whole-document path is cheaper; as the batch grows the in-place (deferred) path overtakes
 *       it.
 *   <li>{@code numDocs} — segment size. Raises the in-place path's flat per-commit cost
 *       (full-column rewrite / graph build), shifting the crossover point.
 *   <li>{@code otherFields} — document "richness" (extra indexed text + stored fields). The
 *       whole-document path pays to re-index all of it; the in-place path does not.
 *   <li>{@code deferGraphRebuild} — eager (rebuild graph each commit) vs deferred (skip graph,
 *       exact scan until next merge) for the in-place path.
 * </ul>
 *
 * <p><b>Note on semantics being compared:</b> this measures raw write cost. It does <em>not</em>
 * credit the whole-document path for the deferred merge cost it incurs later (rebuilding graphs to
 * reclaim the documents it deleted), nor does it require the caller to already possess every other
 * field's source value (which the in-place path does not need). Both matter in practice.
 */
// SingleShotTime: each measured iteration runs exactly ONE update+commit. This avoids JMH calling
// the benchmark method (and thus copying the base index) many times per iteration, which would make
// per-op index copying — not the update — dominate the timing for an expensive, must-reset op.
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@Fork(
    value = 1,
    jvmArgsAppend = {"-Xmx8g", "-Xms8g", "-XX:+AlwaysPreTouch"})
public class VectorUpdateBenchmark {

  private static final String ID = "id";
  private static final String VEC = "vec";
  private static final long SEED = 0xCAFED00DL;

  // Base index templates are built ONCE per (numDocs, dim, otherFields) and cached for the whole
  // fork, so the expensive 100k/500k * 768-dim eager-graph build happens once and is reused by all
  // batchSize / deferGraphRebuild trials (which just byte-copy it). Keyed string -> template dir.
  private static final java.util.Map<String, Path> TEMPLATES = new java.util.HashMap<>();

  // Per-segment vector count. The default is modest so a bare run finishes quickly; pass larger
  // values on the command line (e.g. -p numDocs=100000,500000) to see the O(numDocs) full-column
  // rewrite cost and the batch-size crossover at realistic segment sizes.
  @Param({"20000"})
  public int numDocs;

  @Param({"128", "768"})
  public int dim;

  /** Number of extra indexed-text + stored fields per doc (models document "richness"). */
  @Param({"0", "8"})
  public int otherFields;

  /** How many distinct documents are re-embedded per commit (the realistic "re-embed" batch). */
  @Param({"1", "1000", "10000"})
  public int batchSize;

  /**
   * Whether the in-place path defers the HNSW graph rebuild to merge time (exact-scan until then).
   * Only affects {@link #updateFloatVector}; the whole-document path always defers graph work to a
   * later merge anyway.
   */
  @Param({"false", "true"})
  public boolean deferGraphRebuild;

  private Codec codec;
  private Random random;
  private Path templatePath; // cached base index for this (numDocs, dim, otherFields)

  // per-iteration state (fully isolated: each measured shot gets its own copied directory + writer)
  private Path indexPath;
  private Directory dir;
  private IndexWriter writer;

  @Setup(Level.Trial)
  public void setUpTrial() throws IOException {
    random = new Random(SEED);

    // Use the stock Lucene104 codec: it is SPI-registered (so the on-disk index can be reopened
    // after we copy it), and its default KNN format is the unquantized Lucene99HnswVectorsFormat —
    // exactly the (and only) format the in-place vector update supports. A custom anonymous codec
    // would not be resolvable by name when reopening the copied index.
    codec = new Lucene104Codec();
    templatePath = buildOrGetTemplate();
  }

  /**
   * Builds the base index for this (numDocs, dim, otherFields) once per fork, or returns cached.
   */
  private synchronized Path buildOrGetTemplate() throws IOException {
    String key = numDocs + "_" + dim + "_" + otherFields;
    Path cached = TEMPLATES.get(key);
    if (cached != null) {
      return cached;
    }
    Path path = Files.createTempDirectory("vectorUpdateBenchTemplate_" + key + "_");
    try (Directory d = new MMapDirectory(path);
        IndexWriter w = new IndexWriter(d, new IndexWriterConfig().setCodec(codec))) {
      for (int i = 0; i < numDocs; i++) {
        w.addDocument(makeDoc(i, vectorFor(i)));
      }
      // NOTE: intentionally NOT forceMerge(1). At high numDocs a single forced HNSW graph build
      // dominates setup and exceeds JMH's iteration timeout. The natural multi-segment layout is
      // cheaper to build and more representative; both benchmark methods see the same base.
      w.commit();
    }
    TEMPLATES.put(key, path);
    return path;
  }

  /**
   * Copy the prebuilt base index into a fresh directory before each measured shot, then open a
   * writer with the desired defer setting. The copy is a plain byte copy (no graph rebuild), so
   * setup stays cheap even for large numDocs, and each measured update starts from an identical
   * clean base. {@code Level.Iteration} pairs with {@link Mode#SingleShotTime} (one shot per
   * iteration), so the copy is excluded from the measured time and not repeated per op.
   */
  @Setup(Level.Iteration)
  public void buildIndex() throws IOException {
    indexPath = Files.createTempDirectory("vectorUpdateBench");
    dir = new MMapDirectory(indexPath);
    try (Directory templateDir = new MMapDirectory(templatePath)) {
      for (String file : templateDir.listAll()) {
        dir.copyFrom(templateDir, file, file, org.apache.lucene.store.IOContext.DEFAULT);
      }
    }
    writer =
        new IndexWriter(
            dir,
            new IndexWriterConfig().setCodec(codec).setDeferVectorGraphRebuild(deferGraphRebuild));
  }

  @TearDown(Level.Iteration)
  public void closeIndex() throws IOException {
    IOUtils.close(writer, dir);
    writer = null;
    dir = null;
    deleteDir(indexPath);
    indexPath = null;
  }

  private static void deleteDir(Path path) throws IOException {
    if (path == null) {
      return;
    }
    try (var paths = Files.walk(path)) {
      paths
          .sorted(java.util.Comparator.reverseOrder())
          .forEach(
              p -> {
                try {
                  Files.deleteIfExists(p);
                } catch (IOException _) {
                  // best effort
                }
              });
    }
  }

  /**
   * In-place vector update: only the vector column + HNSW graph for the affected segment change.
   */
  @Benchmark
  public long updateFloatVector() throws IOException {
    long seqNo = 0;
    for (int doc : pickBatch()) {
      seqNo = writer.updateFloatVectorValue(new Term(ID, "doc-" + doc), VEC, newRandomVector());
    }
    writer.commit();
    return seqNo;
  }

  /** Status quo: delete by term and re-add the entire document (all fields re-indexed). */
  @Benchmark
  public long updateWholeDocument() throws IOException {
    long seqNo = 0;
    for (int doc : pickBatch()) {
      seqNo = writer.updateDocument(new Term(ID, "doc-" + doc), makeDoc(doc, newRandomVector()));
    }
    writer.commit();
    return seqNo;
  }

  /** Picks {@code batchSize} distinct doc ids to re-embed this invocation. */
  private int[] pickBatch() {
    int n = Math.min(batchSize, numDocs);
    int[] ids = new int[n];
    // simple distinct selection: start at a random offset and stride through the id space
    int start = random.nextInt(numDocs);
    int stride = Math.max(1, numDocs / n);
    for (int i = 0; i < n; i++) {
      ids[i] = (start + i * stride) % numDocs;
    }
    return ids;
  }

  private Document makeDoc(int id, float[] vector) {
    Document d = new Document();
    d.add(new StringField(ID, "doc-" + id, Store.YES));
    d.add(new KnnFloatVectorField(VEC, vector, VectorSimilarityFunction.DOT_PRODUCT));
    for (int f = 0; f < otherFields; f++) {
      // both an indexed-text field (postings) and a stored copy, to model real re-index cost
      d.add(new TextField("text" + f, textFor(id, f), Store.YES));
    }
    return d;
  }

  /** Deterministic vector for a docID (so the template build needs no cached arrays). */
  private float[] vectorFor(int id) {
    Random r = new Random(SEED ^ (0x9E3779B97F4A7C15L * (id + 1)));
    return normalizedRandomVector(r, dim);
  }

  /** Deterministic text for (docID, field). */
  private String textFor(int id, int field) {
    Random r = new Random(SEED ^ (0xC2B2AE3D27D4EB4FL * (id + 1)) ^ (0x165667B19E3779F9L * field));
    StringBuilder sb = new StringBuilder();
    int words = 20 + r.nextInt(40);
    for (int i = 0; i < words; i++) {
      if (i > 0) {
        sb.append(' ');
      }
      sb.append("term").append(r.nextInt(5000));
    }
    return sb.toString();
  }

  /** A fresh random unit vector (used for the NEW value written by each update). */
  private float[] newRandomVector() {
    return normalizedRandomVector(random, dim);
  }

  private static float[] normalizedRandomVector(Random r, int d) {
    float[] v = new float[d];
    double norm = 0;
    for (int i = 0; i < d; i++) {
      v[i] = r.nextFloat() * 2 - 1;
      norm += v[i] * v[i];
    }
    // normalize (DOT_PRODUCT requires unit-length vectors)
    float inv = (float) (1.0 / Math.sqrt(Math.max(norm, 1e-9)));
    for (int i = 0; i < d; i++) {
      v[i] *= inv;
    }
    return v;
  }
}
