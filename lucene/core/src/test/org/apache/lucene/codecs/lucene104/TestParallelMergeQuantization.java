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
package org.apache.lucene.codecs.lucene104;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues.ScalarEncoding;

/** Quantizing merged vectors on several workers writes exactly the records of the serial path. */
public class TestParallelMergeQuantization extends LuceneTestCase {

  private static ExecutorService executor;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    executor = Executors.newFixedThreadPool(4, new NamedThreadFactory("merge-quantize-test"));
  }

  @Override
  public void tearDown() throws Exception {
    executor.shutdown();
    assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));
    super.tearDown();
  }

  public void testIndexRecordsSameBytesAsSerial() throws Exception {
    int dim = 1 + random().nextInt(200);
    VectorSimilarityFunction sim = randomSimilarity();
    List<float[]> vectors = randomVectors(1 + random().nextInt(3000), dim, sim);
    float[] centroid = centroid(vectors, dim, sim);
    for (ScalarEncoding encoding : ScalarEncoding.values()) {
      try (Directory dir = newDirectory()) {
        try (IndexOutput serial = dir.createOutput("serial", IOContext.DEFAULT);
            IndexOutput parallel = dir.createOutput("parallel", IOContext.DEFAULT)) {
          Lucene104ScalarQuantizedVectorsWriter.writeVectorData(
              serial,
              new Lucene104ScalarQuantizedVectorsWriter.QuantizedFloatVectorValues(
                  FloatVectorValues.fromFloats(copy(vectors), dim),
                  new OptimizedScalarQuantizer(sim),
                  encoding,
                  centroid));
          Lucene104ScalarQuantizedVectorsWriter.writeVectorDataInParallel(
              parallel,
              FloatVectorValues.fromFloats(copy(vectors), dim),
              new OptimizedScalarQuantizer(sim),
              encoding,
              centroid,
              new TaskExecutor(executor),
              2 + random().nextInt(7),
              1 + random().nextInt(300));
        }
        assertArrayEquals(encoding.toString(), readAll(dir, "serial"), readAll(dir, "parallel"));
      }
    }
  }

  public void testQueryRecordsSameBytesAsSerial() throws Exception {
    int dim = 1 + random().nextInt(200);
    VectorSimilarityFunction sim = randomSimilarity();
    List<float[]> vectors = randomVectors(1 + random().nextInt(3000), dim, sim);
    float[] centroid = centroid(vectors, dim, sim);
    for (ScalarEncoding encoding : ScalarEncoding.values()) {
      if (encoding.isAsymmetric() == false) {
        continue;
      }
      try (Directory dir = newDirectory()) {
        DocsWithFieldSet serialDocs = new DocsWithFieldSet();
        DocsWithFieldSet parallelDocs = new DocsWithFieldSet();
        try (IndexOutput index = dir.createOutput("serial.index", IOContext.DEFAULT);
            IndexOutput query = dir.createOutput("serial.query", IOContext.DEFAULT)) {
          Lucene104ScalarQuantizedVectorsWriter.writeVectorAndQueryRecords(
              index,
              query,
              FloatVectorValues.fromFloats(copy(vectors), dim),
              new OptimizedScalarQuantizer(sim),
              encoding,
              centroid,
              serialDocs);
        }
        try (IndexOutput index = dir.createOutput("parallel.index", IOContext.DEFAULT);
            IndexOutput query = dir.createOutput("parallel.query", IOContext.DEFAULT)) {
          Lucene104ScalarQuantizedVectorsWriter.writeVectorAndQueryRecordsInParallel(
              index,
              query,
              FloatVectorValues.fromFloats(copy(vectors), dim),
              new OptimizedScalarQuantizer(sim),
              encoding,
              centroid,
              parallelDocs,
              new TaskExecutor(executor),
              2 + random().nextInt(7),
              1 + random().nextInt(300));
        }
        for (String side : new String[] {".index", ".query"}) {
          assertArrayEquals(
              encoding + side, readAll(dir, "serial" + side), readAll(dir, "parallel" + side));
        }
        assertEquals(serialDocs.cardinality(), parallelDocs.cardinality());
      }
    }
  }

  /**
   * A force merge with merge workers, through the format's executor or the merge's own, stores the
   * same quantized vectors as a merge without them.
   */
  public void testForceMergeWithWorkersStoresSameVectors() throws Exception {
    int dim = 1 + random().nextInt(16);
    // more than one block, so that the merge quantizes in parallel
    int numDocs =
        Lucene104ScalarQuantizedVectorsWriter.QUANTIZE_BLOCK_SIZE + 1 + random().nextInt(500);
    VectorSimilarityFunction sim = randomSimilarity();
    List<float[]> vectors = randomVectors(numDocs, dim, sim);
    ScalarEncoding encoding = RandomPicks.randomFrom(random(), ScalarEncoding.values());
    boolean formatExecutor = random().nextBoolean();
    try (Directory serial = newDirectory();
        Directory parallel = newDirectory()) {
      index(
          serial,
          new Lucene104HnswScalarQuantizedVectorsFormat(encoding, 8, 20, 1, null),
          vectors,
          sim);
      index(
          parallel,
          new Lucene104HnswScalarQuantizedVectorsFormat(
              encoding, 8, 20, 4, formatExecutor ? executor : null),
          vectors,
          sim);
      try (IndexReader a = DirectoryReader.open(serial);
          IndexReader b = DirectoryReader.open(parallel)) {
        QuantizedByteVectorValues x = quantizedValues(getOnlyLeafReader(a));
        QuantizedByteVectorValues y = quantizedValues(getOnlyLeafReader(b));
        assertEquals(numDocs, x.size());
        assertEquals(x.size(), y.size());
        assertArrayEquals(x.getCentroid(), y.getCentroid(), 0f);
        for (int ord = 0; ord < x.size(); ord++) {
          assertArrayEquals("ord " + ord, x.vectorValue(ord), y.vectorValue(ord));
          assertEquals("ord " + ord, x.getCorrectiveTerms(ord), y.getCorrectiveTerms(ord));
        }
      }
    }
  }

  private static void index(
      Directory dir,
      Lucene104HnswScalarQuantizedVectorsFormat format,
      List<float[]> vectors,
      VectorSimilarityFunction sim)
      throws IOException {
    // three segments of whole flushes and no merge until forceMerge
    LogDocMergePolicy mergePolicy = new LogDocMergePolicy();
    mergePolicy.setMergeFactor(1000);
    IndexWriterConfig config =
        new IndexWriterConfig()
            .setCodec(TestUtil.alwaysKnnVectorsFormat(format))
            .setMergePolicy(mergePolicy)
            .setMaxBufferedDocs(vectors.size() / 3 + 1);
    try (IndexWriter w = new IndexWriter(dir, config)) {
      for (float[] v : vectors) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("f", v, sim));
        w.addDocument(doc);
      }
      w.forceMerge(1);
    }
  }

  private static QuantizedByteVectorValues quantizedValues(LeafReader reader) throws IOException {
    return ((Lucene104ScalarQuantizedVectorsReader.ScalarQuantizedVectorValues)
            reader.getFloatVectorValues("f"))
        .getQuantizedVectorValues();
  }

  private static VectorSimilarityFunction randomSimilarity() {
    return RandomPicks.randomFrom(random(), VectorSimilarityFunction.values());
  }

  /** Random vectors, unit length for COSINE, as the quantizer expects them. */
  private static List<float[]> randomVectors(int count, int dim, VectorSimilarityFunction sim) {
    List<float[]> vectors = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      float[] v = new float[dim];
      for (int d = 0; d < dim; d++) {
        v[d] = random().nextFloat() * 2 - 1;
      }
      vectors.add(sim == VectorSimilarityFunction.COSINE ? VectorUtil.l2normalize(v) : v);
    }
    return vectors;
  }

  private static float[] centroid(List<float[]> vectors, int dim, VectorSimilarityFunction sim) {
    float[] centroid = new float[dim];
    for (float[] v : vectors) {
      for (int d = 0; d < dim; d++) {
        centroid[d] += v[d] / vectors.size();
      }
    }
    return sim == VectorSimilarityFunction.COSINE ? VectorUtil.l2normalize(centroid) : centroid;
  }

  private static byte[] readAll(Directory dir, String name) throws IOException {
    try (IndexInput in = dir.openInput(name, IOContext.DEFAULT)) {
      byte[] b = new byte[(int) in.length()];
      in.readBytes(b, 0, b.length);
      return b;
    }
  }

  private static List<float[]> copy(List<float[]> in) {
    List<float[]> out = new ArrayList<>(in.size());
    for (float[] v : in) {
      out.add(v.clone());
    }
    return out;
  }
}
