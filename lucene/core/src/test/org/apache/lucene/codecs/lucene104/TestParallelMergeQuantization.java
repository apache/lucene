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
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsWriter.MergedRecordWriter;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsWriter.SourceIterator;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloat16VectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues.ScalarEncoding;

/** Quantizing merged vectors on several workers writes exactly the records of the serial path. */
public class TestParallelMergeQuantization extends LuceneTestCase {

  private ExecutorService executor;

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

  /** Same records as the serial path, for any encoding and merge order, one write per block. */
  public void testSameBytesAsSerial() throws Exception {
    int dim = 1 + random().nextInt(200);
    VectorSimilarityFunction sim = randomSimilarity();
    int blockSize = 1 + random().nextInt(300);
    List<float[]> vectors = randomVectors(1 + random().nextInt(3000), dim, sim);
    float[] centroid = randomVectors(1, dim, sim).get(0); // any centroid will do
    int workers = random().nextBoolean() ? 1 : 2 + random().nextInt(7);
    // 3 segments, concatenated or interleaved as by an index sort, with deleted docs (-1)
    boolean sorted = random().nextBoolean();
    List<List<Integer>> segmentDocs =
        List.of(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
    for (int i = 0; i < vectors.size(); i++) {
      // interleaved: any segment; concatenated: the first, second or last third of the vectors
      int segment = sorted ? random().nextInt(3) : 3 * i / vectors.size();
      List<Integer> docs = segmentDocs.get(segment);
      if (random().nextInt(5) == 0) {
        docs.add(-1);
      }
      docs.add(i);
    }
    List<FloatVectorValues> segmentValues = new ArrayList<>();
    for (List<Integer> docs : segmentDocs) {
      // a deleted doc's vector is never read, so any vector will do
      List<float[]> segment = docs.stream().map(d -> vectors.get(Math.max(d, 0))).toList();
      FloatVectorValues values = FloatVectorValues.fromFloats(segment, dim);
      if (workers == 1) {
        // a single task reads the source values themselves, so it must not copy them
        values = noCopy(values);
      }
      segmentValues.add(values);
    }
    OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(sim);
    for (ScalarEncoding encoding : ScalarEncoding.values()) {
      boolean queries = encoding.isAsymmetric() && random().nextBoolean();
      try (Directory dir = newDirectory()) {
        try (IndexOutput index = dir.createOutput("serial.index", IOContext.DEFAULT);
            IndexOutput queryFile = dir.createOutput("serial.query", IOContext.DEFAULT)) {
          // the records of the vectors in merged order, one at a time
          IndexOutput query = queries ? queryFile : null;
          for (float[] v : vectors) {
            encodeOne(quantizer, encoding, centroid, v.clone(), index, query);
          }
        }
        try (IndexOutput vectorData = dir.createOutput("parallel.index", IOContext.DEFAULT);
            IndexOutput queryFile = dir.createOutput("parallel.query", IOContext.DEFAULT)) {
          IndexOutput queryData = queries ? queryFile : null;
          // a merger consumes its sources, so each writer gets its own
          List<SourceIterator> sources = new ArrayList<>();
          for (int s = 0; s < segmentValues.size(); s++) {
            sources.add(new SourceIterator(segmentDocs.get(s)::get, s, segmentValues.get(s)));
          }
          TaskExecutor taskExecutor = new TaskExecutor(executor);
          MergedRecordWriter records =
              new MergedRecordWriter(
                  taskExecutor, workers, sources, sorted, quantizer, encoding, centroid, blockSize);
          DocsWithFieldSet docsWithField = new DocsWithFieldSet();
          records.write(vectorData, queryData, docsWithField);
          // every live document, in merged order
          DocIdSetIterator written = docsWithField.iterator();
          for (int doc = 0; doc < vectors.size(); doc++) {
            assertEquals(doc, written.nextDoc());
          }
          assertEquals(DocIdSetIterator.NO_MORE_DOCS, written.nextDoc());
        }
        String message =
            encoding + " queries=" + queries + " sorted=" + sorted + " workers=" + workers;
        assertArrayEquals(message, readAll(dir, "serial.index"), readAll(dir, "parallel.index"));
        assertArrayEquals(message, readAll(dir, "serial.query"), readAll(dir, "parallel.query"));
      }
    }
  }

  /**
   * A force merge with merge workers, through the format's executor or the merge's own, stores the
   * same quantized vectors and centroid as a merge without them, with deleted documents and a
   * source segment that has the field but no vectors.
   */
  public void testForceMergeWithWorkersStoresSameVectors() throws Exception {
    int dim = 1 + random().nextInt(16);
    // more than one block of live vectors, so that the merge quantizes in parallel
    int numDocs =
        2 * Lucene104ScalarQuantizedVectorsWriter.QUANTIZE_BLOCK_SIZE + random().nextInt(500);
    VectorSimilarityFunction sim = randomSimilarity();
    List<float[]> vectors = randomVectors(numDocs, dim, sim);
    ScalarEncoding encoding = RandomPicks.randomFrom(random(), ScalarEncoding.values());
    boolean formatExecutor = random().nextBoolean();
    // fp16 is read through Float16AsFloatVectorValues and its copies on the workers
    boolean float16 = random().nextBoolean();
    try (Directory serial = newDirectory();
        Directory parallel = newDirectory()) {
      // the same documents and deletions in both indexes; only the number of merge workers differs
      index(serial, encoding, 1, null, vectors, sim, float16);
      // null: the merge's own executor
      ExecutorService mergeExec = formatExecutor ? executor : null;
      index(parallel, encoding, 4, mergeExec, vectors, sim, float16);
      try (IndexReader a = DirectoryReader.open(serial);
          IndexReader b = DirectoryReader.open(parallel)) {
        QuantizedByteVectorValues x = quantizedValues(getOnlyLeafReader(a));
        QuantizedByteVectorValues y = quantizedValues(getOnlyLeafReader(b));
        // every 10th document was deleted, and the empty segment adds no vectors
        assertEquals(numDocs - (numDocs + 9) / 10, x.size());
        assertEquals(x.size(), y.size());
        // the deletions make both merges recompute the centroid, which must have the same bits
        assertArrayEquals(x.getCentroid(), y.getCentroid(), 0f);
        for (int ord = 0; ord < x.size(); ord++) {
          assertArrayEquals("ord " + ord, x.vectorValue(ord), y.vectorValue(ord));
          assertEquals("ord " + ord, x.getCorrectiveTerms(ord), y.getCorrectiveTerms(ord));
        }
      }
    }
  }

  /**
   * The centroid sums skip deleted vectors, match a plain double sum across chunk boundaries, and
   * have the same bits for any number of workers.
   */
  public void testSumVectorsSkipsDeletedAcrossChunks() throws Exception {
    int dim = 1 + random().nextInt(32);
    int count =
        3 * Lucene104ScalarQuantizedVectorsWriter.CENTROID_CHUNK_SIZE + random().nextInt(100);
    List<float[]> vectors = randomVectors(count, dim, VectorSimilarityFunction.EUCLIDEAN);
    FixedBitSet liveDocs = new FixedBitSet(count);
    double[] expected = new double[dim];
    int live = 0;
    for (int i = 0; i < count; i++) {
      if (random().nextInt(4) != 0) {
        liveDocs.set(i);
        live++;
        for (int d = 0; d < dim; d++) {
          expected[d] += vectors.get(i)[d];
        }
      }
    }
    FloatVectorValues values = FloatVectorValues.fromFloats(vectors, dim);
    double[] serial = new double[dim];
    double[] parallel = new double[dim];
    TaskExecutor sameThread = new TaskExecutor(Runnable::run);
    TaskExecutor taskExecutor = new TaskExecutor(executor);
    int workers = 2 + random().nextInt(3);
    assertEquals(
        live,
        Lucene104ScalarQuantizedVectorsWriter.sumVectors(values, liveDocs, serial, sameThread, 1));
    assertEquals(
        live,
        Lucene104ScalarQuantizedVectorsWriter.sumVectors(
            values, liveDocs, parallel, taskExecutor, workers));
    assertArrayEquals(serial, parallel, 0);
    assertArrayEquals(expected, serial, 1e-9);
  }

  private static void index(
      Directory dir,
      ScalarEncoding encoding,
      int workers,
      ExecutorService mergeExec,
      List<float[]> vectors,
      VectorSimilarityFunction sim,
      boolean float16)
      throws IOException {
    var format = new Lucene104HnswScalarQuantizedVectorsFormat(encoding, 8, 20, workers, mergeExec);
    // three segments of whole flushes and no merge until forceMerge
    LogDocMergePolicy mergePolicy = new LogDocMergePolicy();
    mergePolicy.setMergeFactor(1000);
    IndexWriterConfig config =
        new IndexWriterConfig()
            .setCodec(TestUtil.alwaysKnnVectorsFormat(format))
            .setMergePolicy(mergePolicy)
            .setMaxBufferedDocs(vectors.size() / 3 + 1);
    try (IndexWriter w = new IndexWriter(dir, config)) {
      for (int i = 0; i < vectors.size(); i++) {
        Document doc = new Document();
        doc.add(vectorField(vectors.get(i), sim, float16));
        doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
        w.addDocument(doc);
      }
      // deleted documents: the merge must skip their vectors and recompute the centroid
      for (int i = 0; i < vectors.size(); i += 10) {
        w.deleteDocuments(new Term("id", Integer.toString(i)));
      }
      // a fourth segment that has the field but no vectors; its vector values cannot be copied
      try (Directory empty = emptyVectorSegment(vectors.get(0).length, sim, float16)) {
        w.addIndexes(empty);
      }
      w.forceMerge(1);
    }
  }

  /**
   * Returns an index of one segment that has the field "f" but no vectors. Its only vector is
   * deleted, and the force merge drops it, while the document without a vector keeps the segment.
   */
  private static Directory emptyVectorSegment(
      int dim, VectorSimilarityFunction sim, boolean float16) throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig config =
        new IndexWriterConfig()
            .setCodec(
                TestUtil.alwaysKnnVectorsFormat(new Lucene104HnswScalarQuantizedVectorsFormat()));
    try (IndexWriter w = new IndexWriter(dir, config)) {
      float[] vector = new float[dim];
      vector[0] = 1;
      Document doc = new Document();
      doc.add(vectorField(vector, sim, float16));
      doc.add(new StringField("id", "gone", Field.Store.NO));
      w.addDocument(doc);
      w.addDocument(new Document());
      w.deleteDocuments(new Term("id", "gone"));
      w.forceMerge(1);
    }
    return dir;
  }

  /** Writes the records of one vector, as the serial merge did, quantizing it in place. */
  private static void encodeOne(
      OptimizedScalarQuantizer quantizer,
      ScalarEncoding encoding,
      float[] centroid,
      float[] vector,
      DataOutput index,
      DataOutput query)
      throws IOException {
    int dims = encoding.getDiscreteDimensions(vector.length);
    byte[] indexQuantized = new byte[dims];
    byte[] queryQuantized = new byte[dims];
    OptimizedScalarQuantizer.QuantizationResult indexCorrections;
    if (query == null) {
      indexCorrections =
          quantizer.scalarQuantize(vector, indexQuantized, encoding.getBits(), centroid);
    } else {
      byte[][] destinations = {indexQuantized, queryQuantized};
      byte[] bits = {encoding.getBits(), encoding.getQueryBits()};
      OptimizedScalarQuantizer.QuantizationResult[] corrections =
          quantizer.multiScalarQuantize(vector, destinations, bits, centroid);
      indexCorrections = corrections[0];
      byte[] queryPacked = new byte[encoding.getQueryPackedLength(dims)];
      Lucene104ScalarQuantizedVectorsFormat.writeQueryRecord(
          query, queryQuantized, queryPacked, corrections[1]);
    }
    byte[] indexPacked =
        switch (encoding) {
          case UNSIGNED_BYTE, SEVEN_BIT -> indexQuantized;
          case PACKED_NIBBLE, SINGLE_BIT_QUERY_NIBBLE, DIBIT_QUERY_NIBBLE ->
              new byte[encoding.getDocPackedLength(dims)];
        };
    Lucene104ScalarQuantizedVectorsWriter.packIndexRecord(encoding, indexQuantized, indexPacked);
    index.writeBytes(indexPacked, indexPacked.length);
    Lucene104ScalarQuantizedVectorsFormat.writeCorrections(index, indexCorrections);
  }

  /** A view of {@code values} that cannot be copied. */
  private static FloatVectorValues noCopy(FloatVectorValues values) {
    return new FloatVectorValues() {
      @Override
      public float[] vectorValue(int ord) throws IOException {
        return values.vectorValue(ord);
      }

      @Override
      public int dimension() {
        return values.dimension();
      }

      @Override
      public int size() {
        return values.size();
      }

      @Override
      public DocIndexIterator iterator() {
        return values.iterator();
      }

      @Override
      public FloatVectorValues copy() {
        throw new UnsupportedOperationException();
      }
    };
  }

  /** A vector field "f", stored as fp16 or as fp32. */
  private static Field vectorField(float[] vector, VectorSimilarityFunction sim, boolean float16) {
    if (float16 == false) {
      return new KnnFloatVectorField("f", vector, sim);
    }
    short[] halves = new short[vector.length];
    for (int d = 0; d < vector.length; d++) {
      halves[d] = Float.floatToFloat16(vector[d]);
    }
    return new KnnFloat16VectorField("f", halves, sim);
  }

  private static QuantizedByteVectorValues quantizedValues(LeafReader reader) throws IOException {
    KnnVectorsReader vectorsReader =
        ((CodecReader) reader).getVectorReader().unwrapReaderForField("f");
    FlatVectorsReader flatReader =
        ((Lucene99HnswVectorsReader) vectorsReader).getFlatVectorsReader();
    return ((Lucene104ScalarQuantizedVectorsReader) flatReader).getQuantizedVectorValues("f");
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
      if (sim == VectorSimilarityFunction.COSINE) {
        VectorUtil.l2normalize(v);
      }
      vectors.add(v);
    }
    return vectors;
  }

  private static byte[] readAll(Directory dir, String name) throws IOException {
    try (IndexInput in = dir.openInput(name, IOContext.DEFAULT)) {
      byte[] b = new byte[(int) in.length()];
      in.readBytes(b, 0, b.length);
      return b;
    }
  }
}
