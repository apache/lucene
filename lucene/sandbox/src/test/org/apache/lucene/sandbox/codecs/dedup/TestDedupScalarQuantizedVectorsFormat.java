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
package org.apache.lucene.sandbox.codecs.dedup;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN;
import static org.apache.lucene.index.VectorEncoding.BYTE;
import static org.apache.lucene.index.VectorEncoding.FLOAT32;
import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;
import static org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;
import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.index.VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;
import static org.hamcrest.Matchers.instanceOf;

import java.util.Arrays;
import java.util.List;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues.ScalarEncoding;

/**
 * Tests that {@link DedupHnswScalarQuantizedVectorsFormat} stores each distinct vector once, in
 * both raw and quantized form. General de-duplication behavior is covered by {@link
 * TestDedupFlatVectorsFormat}; this test focuses on the quantized side.
 */
public class TestDedupScalarQuantizedVectorsFormat extends LuceneTestCase {

  private static final ScalarEncoding ENCODING = ScalarEncoding.UNSIGNED_BYTE;

  private static IndexWriterConfig config() {
    return newIndexWriterConfig()
        .setCodec(TestUtil.alwaysKnnVectorsFormat(new DedupHnswScalarQuantizedVectorsFormat()));
  }

  /** Size in bytes of one quantized record for the given dimension. */
  private static long quantizedRecordSize(int dimension) {
    return ENCODING.getDocPackedLength(dimension)
        + DedupScalarQuantizedVectorValues.CORRECTIVE_BYTES;
  }

  /** Repeated vectors are stored once, raw and quantized, but still read back per document. */
  public void testDuplicatesStoredOnce() throws Exception {
    float[] a = {1, 2, 3, 4};
    float[] b = {5, 6, 7, 8};
    float[][] docVectors = {a, b, a, b, a, b}; // 3 copies each of 2 vectors
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, config())) {
      for (int ord = 0; ord < docVectors.length; ord++) {
        Document doc = new Document();
        doc.add(new NumericDocValuesField("id", ord));
        doc.add(new KnnFloatVectorField("f", docVectors[ord], EUCLIDEAN));
        w.addDocument(doc);
      }
      w.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader leafReader = getOnlyLeafReader(reader);
        FloatVectorValues values = leafReader.getFloatVectorValues("f");
        assertEquals(docVectors.length, values.size()); // one entry per document
        assertEquals(2, groupNumVectors(values)); // only two distinct vectors stored

        // raw vectors read back exactly, resolved per document
        for (int ord = 0; ord < values.size(); ord++) {
          float[] vector = values.vectorValue(ord);
          assertTrue(Arrays.equals(a, vector) || Arrays.equals(b, vector));
        }

        // the quantized block also holds exactly two records
        DedupScalarQuantizedVectorsReader sqReader = getQuantizedReader(leafReader, "f");
        DedupScalarQuantizedVectorsReader.FieldEntry entry = sqReader.getEntry("f", FLOAT32);
        assertEquals(2 * quantizedRecordSize(a.length), entry.quantizedBlock().quantizedDataSize());
      }
    }
  }

  /**
   * Fields with the same dimension and encoding share one raw copy of a vector; their quantized
   * records are stored per {@link DedupQuantizer.Flavor}, shared across fields whose similarity
   * functions map to the same flavor.
   */
  public void testDuplicatesAcrossFieldsShareQuantizedGroup() throws Exception {
    float[] shared = {9, 8, 7, 6};
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, config())) {
      Document doc = new Document();
      doc.add(new KnnFloatVectorField("f1", shared, EUCLIDEAN));
      doc.add(new KnnFloatVectorField("f2", shared, DOT_PRODUCT)); // different function
      doc.add(new KnnFloatVectorField("f3", shared, COSINE)); // different function
      doc.add(new KnnFloatVectorField("f4", shared, MAXIMUM_INNER_PRODUCT)); // same flavor as f2
      w.addDocument(doc);
      w.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader leaf = getOnlyLeafReader(reader);

        DedupScalarQuantizedVectorsReader sqReader = getQuantizedReader(leaf, "f1");
        assertEquals(sqReader, getQuantizedReader(leaf, "f2"));
        assertEquals(sqReader, getQuantizedReader(leaf, "f3"));
        assertEquals(sqReader, getQuantizedReader(leaf, "f4"));

        // all fields resolve to the same raw group, with one distinct vector
        DedupScalarQuantizedVectorsReader.FieldEntry e1 = sqReader.getEntry("f1", FLOAT32);
        DedupScalarQuantizedVectorsReader.FieldEntry e2 = sqReader.getEntry("f2", FLOAT32);
        DedupScalarQuantizedVectorsReader.FieldEntry e3 = sqReader.getEntry("f3", FLOAT32);
        DedupScalarQuantizedVectorsReader.FieldEntry e4 = sqReader.getEntry("f4", FLOAT32);
        assertEquals(e1.groupInfo(), e2.groupInfo());
        assertEquals(e1.groupInfo(), e3.groupInfo());
        assertEquals(e1.groupInfo(), e4.groupInfo());
        assertEquals(1, e1.groupInfo().groupNumVectors());

        // quantized data is split by flavor: EUCLIDEAN (f1), DOT_PRODUCT (f2 + f4, shared) and
        // NORMALIZED (f3), one record each
        assertNotEquals(e1.quantizedBlock(), e2.quantizedBlock());
        assertNotEquals(e1.quantizedBlock(), e3.quantizedBlock());
        assertNotEquals(e2.quantizedBlock(), e3.quantizedBlock());
        assertEquals(e2.quantizedBlock(), e4.quantizedBlock());
        for (DedupScalarQuantizedVectorsReader.FieldEntry entry : List.of(e1, e2, e3, e4)) {
          assertEquals(
              quantizedRecordSize(shared.length), entry.quantizedBlock().quantizedDataSize());
        }

        for (String field : new String[] {"f1", "f2", "f3", "f4"}) {
          FloatVectorValues values = leaf.getFloatVectorValues(field);
          assertEquals(1, groupNumVectors(values));
          assertArrayEquals(shared, values.vectorValue(0), 0f);
        }
      }
    }
  }

  /** Duplicates spanning multiple segments collapse to a single quantized copy when merged. */
  public void testDuplicatesAcrossSegmentsDedupOnMerge() throws Exception {
    float[] a = {1, 1, 1, 1};
    float[] b = {2, 2, 2, 2};
    float[][] docVectors = {a, b, a}; // 3 docs across 3 segments, 2 distinct
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, config())) {
      for (float[] docVector : docVectors) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("f", docVector, EUCLIDEAN));
        w.addDocument(doc);
        w.commit(); // one segment per document
      }
      w.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader leafReader = getOnlyLeafReader(reader);
        FloatVectorValues values = leafReader.getFloatVectorValues("f");
        assertEquals(docVectors.length, values.size());
        assertEquals(2, groupNumVectors(values)); // a's duplicate collapsed across segments

        DedupScalarQuantizedVectorsReader sqReader = getQuantizedReader(leafReader, "f");
        DedupScalarQuantizedVectorsReader.FieldEntry entry = sqReader.getEntry("f", FLOAT32);
        assertEquals(2 * quantizedRecordSize(a.length), entry.quantizedBlock().quantizedDataSize());
      }
    }
  }

  /** Documents sharing a vector get identical scores, ranking above unrelated documents. */
  public void testSearchDuplicatesShareScores() throws Exception {
    int dimension = 16;
    float[] a = randomVector(dimension);
    float[] b = randomVector(dimension);
    for (int i = 0; i < dimension; i++) {
      b[i] = -a[i]; // far away from a
    }
    for (VectorSimilarityFunction function : VectorSimilarityFunction.values()) {
      try (Directory dir = newDirectory();
          IndexWriter w = new IndexWriter(dir, config())) {
        int numA = 3, numB = 3;
        for (int i = 0; i < numA; i++) {
          Document doc = new Document();
          doc.add(new KnnFloatVectorField("f", a, function));
          w.addDocument(doc);
        }
        for (int i = 0; i < numB; i++) {
          Document doc = new Document();
          doc.add(new KnnFloatVectorField("f", b, function));
          w.addDocument(doc);
        }
        w.forceMerge(1);
        try (DirectoryReader reader = DirectoryReader.open(w)) {
          LeafReader leafReader = getOnlyLeafReader(reader);
          TopDocs topDocs =
              leafReader.searchNearestVectors(
                  "f",
                  a,
                  numA + numB,
                  AcceptDocs.fromLiveDocs(null, leafReader.maxDoc()),
                  Integer.MAX_VALUE);
          assertEquals(numA + numB, topDocs.scoreDocs.length);
          // duplicates of a rank first with identical scores
          float scoreA = topDocs.scoreDocs[0].score;
          for (int i = 0; i < numA; i++) {
            assertEquals("function=" + function, scoreA, topDocs.scoreDocs[i].score, 0f);
          }
          for (int i = numA; i < numA + numB; i++) {
            ScoreDoc scoreDoc = topDocs.scoreDocs[i];
            assertTrue("function=" + function, scoreDoc.score < scoreA);
          }
        }
      }
    }
  }

  /** Quantized scores are close to true scores for every similarity function. */
  public void testScoreAccuracy() throws Exception {
    int dimension = 16;
    for (VectorSimilarityFunction function : VectorSimilarityFunction.values()) {
      float[] docVector = randomVector(dimension);
      float[] query = randomVector(dimension);
      if (function == DOT_PRODUCT) { // requires normalized vectors
        VectorUtil.l2normalize(docVector);
        VectorUtil.l2normalize(query);
      }
      try (Directory dir = newDirectory();
          IndexWriter w = new IndexWriter(dir, config())) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("f", docVector, function));
        w.addDocument(doc);
        w.forceMerge(1);
        try (DirectoryReader reader = DirectoryReader.open(w)) {
          LeafReader leafReader = getOnlyLeafReader(reader);
          float trueScore = function.compare(docVector, query);
          TopDocs topDocs =
              leafReader.searchNearestVectors(
                  "f",
                  query,
                  1,
                  AcceptDocs.fromLiveDocs(null, leafReader.maxDoc()),
                  Integer.MAX_VALUE);
          assertEquals(1, topDocs.totalHits.value());
          // NOTE: data-blind quantization is less accurate than centroid-centered quantization,
          // so allow a small relative tolerance
          float tolerance = Math.max(0.05f, 0.05f * Math.abs(trueScore));
          assertEquals("function=" + function, trueScore, topDocs.scoreDocs[0].score, tolerance);
        }
      }
    }
  }

  /**
   * Cosine similarity ignores vector magnitude: since vectors are stored unnormalized (to share
   * quantized bytes across fields), the norm-based correction must equalize scaled duplicates.
   */
  public void testCosineIgnoresMagnitude() throws Exception {
    float[] small = {1, 2, 3, 4};
    float[] large = new float[small.length];
    for (int i = 0; i < small.length; i++) {
      large[i] = small[i] * 1000;
    }
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, config())) {
      for (float[] docVector : new float[][] {small, large}) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("f", docVector, COSINE));
        w.addDocument(doc);
      }
      w.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader leafReader = getOnlyLeafReader(reader);
        TopDocs topDocs =
            leafReader.searchNearestVectors(
                "f",
                small,
                2,
                AcceptDocs.fromLiveDocs(null, leafReader.maxDoc()),
                Integer.MAX_VALUE);
        assertEquals(2, topDocs.scoreDocs.length);
        // both docs point in the same direction: cosine similarity ~1 -> score ~1
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
          assertEquals(1f, scoreDoc.score, 0.01f);
        }
      }
    }
  }

  /** BYTE fields pass through to raw de-duplicated storage, without a quantized block. */
  public void testByteVectorsStoredRawOnly() throws Exception {
    byte[] a = {1, 2, 3, 4};
    byte[] b = {5, 6, 7, 8};
    byte[][] docVectors = {a, b, a, b}; // 2 copies each of 2 vectors
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, config())) {
      for (byte[] docVector : docVectors) {
        Document doc = new Document();
        doc.add(new KnnByteVectorField("f", docVector, EUCLIDEAN));
        w.addDocument(doc);
      }
      w.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader leafReader = getOnlyLeafReader(reader);
        ByteVectorValues values = leafReader.getByteVectorValues("f");
        assertEquals(docVectors.length, values.size());
        assertEquals(2, groupNumVectors(values)); // still de-duplicated

        DedupScalarQuantizedVectorsReader sqReader = getQuantizedReader(leafReader, "f");
        DedupScalarQuantizedVectorsReader.FieldEntry entry = sqReader.getEntry("f", BYTE);
        assertNull(entry.quantizedBlock()); // no quantized data
      }
    }
  }

  /**
   * Every encoding survives a merge with graph building forced (exercising the merge-time scorer
   * suppliers, including the temporary query-vectors file of asymmetric encodings) and searches
   * sanely afterwards.
   */
  public void testAllEncodingsMergeAndSearch() throws Exception {
    int dimension = 32;
    int numDistinct = 20;
    float[][] distinct = new float[numDistinct][];
    for (int i = 0; i < numDistinct; i++) {
      distinct[i] = randomVector(dimension);
    }
    for (ScalarEncoding encoding : ScalarEncoding.values()) {
      for (VectorSimilarityFunction function : VectorSimilarityFunction.values()) {
        IndexWriterConfig config =
            newIndexWriterConfig()
                .setCodec(
                    TestUtil.alwaysKnnVectorsFormat(
                        new DedupHnswScalarQuantizedVectorsFormat(
                            encoding,
                            DEFAULT_MAX_CONN,
                            DEFAULT_BEAM_WIDTH,
                            1,
                            null,
                            0))); // always build graphs
        try (Directory dir = newDirectory();
            IndexWriter w = new IndexWriter(dir, config)) {
          for (int segment = 0; segment < 2; segment++) { // duplicates across segments
            for (int i = 0; i < numDistinct; i++) {
              Document doc = new Document();
              float[] vector =
                  function == DOT_PRODUCT
                      ? VectorUtil.l2normalize(distinct[i].clone())
                      : distinct[i];
              doc.add(new KnnFloatVectorField("f", vector, function));
              w.addDocument(doc);
            }
            w.commit();
          }
          w.forceMerge(1); // exercises the merge-time graph construction scorer

          try (DirectoryReader reader = DirectoryReader.open(w)) {
            LeafReader leafReader = getOnlyLeafReader(reader);
            FloatVectorValues values = leafReader.getFloatVectorValues("f");
            assertEquals(2 * numDistinct, values.size());
            assertEquals(numDistinct, groupNumVectors(values)); // de-duplicated across segments

            float[] query =
                function == DOT_PRODUCT ? VectorUtil.l2normalize(distinct[0].clone()) : distinct[0];
            TopDocs topDocs =
                leafReader.searchNearestVectors(
                    "f",
                    query,
                    2,
                    AcceptDocs.fromLiveDocs(null, leafReader.maxDoc()),
                    Integer.MAX_VALUE);
            String context = "encoding=" + encoding + ", function=" + function;
            assertEquals(context, 2, topDocs.scoreDocs.length);
            // the two duplicates of the query vector share the (top) score
            assertEquals(context, topDocs.scoreDocs[0].score, topDocs.scoreDocs[1].score, 0f);
          }
        }
      }
    }
  }

  private float[] randomVector(int dimension) {
    float[] vector = new float[dimension];
    for (int i = 0; i < dimension; i++) {
      vector[i] = random().nextFloat(-1, 1);
    }
    return vector;
  }

  /** Number of distinct vectors physically stored for a field's group. */
  private static int groupNumVectors(KnnVectorValues values) {
    assertThat(values, instanceOf(DedupVectorValues.class));
    return ((DedupVectorValues) values).getGroupView().size();
  }

  /** Get underlying quantized dedup vector reader instance. */
  private static DedupScalarQuantizedVectorsReader getQuantizedReader(
      LeafReader leafReader, String fieldName) {
    assertThat(leafReader, instanceOf(CodecReader.class));
    KnnVectorsReader knnVectorsReader = ((CodecReader) leafReader).getVectorReader();
    knnVectorsReader = knnVectorsReader.unwrapReaderForField(fieldName);

    assertThat(knnVectorsReader, instanceOf(Lucene99HnswVectorsReader.class));
    FlatVectorsReader flatReader =
        ((Lucene99HnswVectorsReader) knnVectorsReader).getFlatVectorsReader();

    assertThat(flatReader, instanceOf(DedupScalarQuantizedVectorsReader.class));
    return (DedupScalarQuantizedVectorsReader) flatReader;
  }
}
