/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.DiversifyingChildrenFloatKnnVectorQuery;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.VectorUtil;
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
import org.openjdk.jmh.infra.Blackhole;

/**
 * End-to-end {@link DiversifyingChildrenFloatKnnVectorQuery} search on a single-segment block-join
 * index (children + parent marker per block), using the default HNSW approximate path ({@code
 * childFilter == null}).
 *
 * <p>The {@code rescoreBlocks} parameter switches the feature on/off so both modes can be compared
 * in a single run (see <a href="https://github.com/apache/lucene/issues/15839">LUCENE-15839</a>).
 * Extra work scales roughly with {@code topK * childrenPerParent}.
 *
 * <p>Indicative results — 3 forks, 5 warmup / 10 measurement iterations, JDK 25, {@code -Xmx2g},
 * dim=96, topK=64, 4096 parent blocks (lower is better):
 *
 * <pre>
 * childrenPerParent  rescoreBlocks=false    rescoreBlocks=true
 * 8                  0.117 ± 0.002 ms/op    0.149 ± 0.002 ms/op  (+27%)
 * 32                 0.237 ± 0.006 ms/op    0.326 ± 0.009 ms/op  (+38%)
 * 64                 0.259 ± 0.005 ms/op    0.426 ± 0.013 ms/op  (+64%)
 * </pre>
 *
 * <p>Overhead grows with block width (and with {@code topK}).
 *
 * <p>Example:
 *
 * <pre>{@code
 * ./gradlew :lucene:benchmark-jmh:assemble
 * cd lucene/benchmark-jmh/build/benchmarks
 * java -jar lucene-benchmark-jmh-*-SNAPSHOT.jar DiversifyingChildrenFloatKnnJoin \\
 *     -f 3 -wi 5 -i 10 -tu ms
 * }</pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(
    value = 3,
    jvmArgsAppend = {
      "-Xmx2g",
      "-Xms2g",
      "-XX:+AlwaysPreTouch",
      "--add-modules=jdk.incubator.vector"
    })
public class DiversifyingChildrenFloatKnnJoinBenchmark {

  /** Approximate neighbors per diversified parent bucket. */
  @Param({"64"})
  public int topK;

  /**
   * Children with vectors per parent block. Post-HNSW block rescoring iterates sibling children in
   * each retained block, so incremental cost rises with this parameter.
   */
  @Param({"8", "32", "64"})
  public int childrenPerParent;

  @Param({"96"})
  public int dimension;

  /**
   * Whether to enable post-HNSW block rescoring. When {@code true}, after HNSW search all children
   * in each found parent's block are scored to guarantee the best child is returned. Compare {@code
   * false} (baseline / no rescoring) against {@code true} (rescoring enabled) to measure latency
   * overhead.
   */
  @Param({"false", "true"})
  public boolean rescoreBlocks;

  private Directory directory;
  private IndexSearcher searcher;
  private Query diversifyingJoinQuery;

  static Document parentDoc() {
    Document d = new Document();
    d.add(new StringField("docType", "_parent", Field.Store.NO));
    return d;
  }

  /** Fixed corpus size for stable HNSW behavior; must be >= topK. */
  private static final int NUM_PARENT_BLOCKS = 4096;

  private static float[] randomUnitVector(Random random, int dim, float[] scratch) {
    for (int i = 0; i < dim; i++) {
      scratch[i] = random.nextFloat() * 2f - 1f;
    }
    return VectorUtil.l2normalize(scratch, false);
  }

  @Setup(Level.Trial)
  public void setupTrial() throws IOException {
    if (topK > NUM_PARENT_BLOCKS) {
      throw new IllegalStateException("topK must be <= NUM_PARENT_BLOCKS");
    }
    directory = new ByteBuffersDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig();
    long randomSeed = 0xC0FFEE42F00DL ^ ((long) childrenPerParent << 32) ^ dimension;
    Random random = new Random(randomSeed);
    float[] scratch = new float[dimension];
    try (IndexWriter w = new IndexWriter(directory, iwc)) {
      for (int p = 0; p < NUM_PARENT_BLOCKS; p++) {
        List<Document> block = new ArrayList<>(childrenPerParent + 1);
        for (int c = 0; c < childrenPerParent; c++) {
          Document child = new Document();
          child.add(
              new KnnFloatVectorField(
                  "vec",
                  randomUnitVector(random, dimension, scratch),
                  VectorSimilarityFunction.DOT_PRODUCT));
          block.add(child);
        }
        block.add(parentDoc());
        w.addDocuments(block);
      }
      w.forceMerge(1);
    }

    var reader = DirectoryReader.open(directory);
    searcher = new IndexSearcher(reader);
    BitSetProducer parentsFilter =
        new QueryBitSetProducer(new TermQuery(new Term("docType", "_parent")));
    // [1, 0, ..., 0] is already L2-normalized.
    float[] queryVector = new float[dimension];
    queryVector[0] = 1f;
    diversifyingJoinQuery =
        new DiversifyingChildrenFloatKnnVectorQuery(
            "vec",
            queryVector,
            null,
            topK,
            parentsFilter,
            KnnSearchStrategy.Hnsw.DEFAULT,
            rescoreBlocks);
  }

  @TearDown(Level.Trial)
  public void tearDownTrial() throws IOException {
    if (searcher != null) {
      searcher.getIndexReader().close();
    }
    if (directory != null) {
      directory.close();
    }
  }

  @Benchmark
  public void searchDiversifyingJoinHnsw(Blackhole bh) throws IOException {
    TopDocs hits = searcher.search(diversifyingJoinQuery, topK);
    bh.consume(hits.scoreDocs.length);
    bh.consume(hits.totalHits.value());
    if (hits.scoreDocs.length > 0) {
      bh.consume(hits.scoreDocs[0].doc);
      bh.consume(hits.scoreDocs[0].score);
    }
  }
}
