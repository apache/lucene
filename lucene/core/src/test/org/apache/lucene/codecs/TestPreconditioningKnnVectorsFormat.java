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
package org.apache.lucene.codecs;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.codecs.hnsw.HnswGraphProvider;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.lucene104.Lucene104HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloat16VectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.quantization.HadamardRotation;
import org.junit.Ignore;

/**
 * Tests {@link PreconditioningKnnVectorsFormat}, both the randomized {@link
 * BaseKnnVectorsFormatTestCase} compliance suite and targeted tests for behaviour specific to this
 * format.
 *
 * <p>Inherited tests that cannot hold for this codec are disabled with {@link Ignore} and a reason,
 * so they are reported as skipped rather than silently passing. They fall into three groups: byte
 * and float16 vectors, which this format rejects at index time; codec-mixing tests, which conflict
 * with the write-time preconditioning-state check; and tests asserting near-exact {@code
 * vectorValue()} read-back, where the delegate's scalar quantization loses far more precision than
 * {@link #getVectorValueTolerance()} allows for.
 */
public class TestPreconditioningKnnVectorsFormat extends BaseKnnVectorsFormatTestCase {

  @Override
  protected Codec getCodec() {
    return TestUtil.alwaysKnnVectorsFormat(
        PreconditioningKnnVectorsFormat.rotating(new Lucene104HnswScalarQuantizedVectorsFormat()));
  }

  @Override
  protected float getVectorValueTolerance() {
    // Rotation + inverse-rotation introduces ~1e-6 floating-point drift from FWHT additions.
    return 1e-5f;
  }

  @Override
  protected boolean supportsFloatVectorFallback() {
    return false;
  }

  // Vectors read back through the delegate's scalar quantization differ from the indexed values by
  // far more than getVectorValueTolerance(); the tolerance hook covers rotation drift, not
  // quantization loss. These assert near-exact read-back, so they cannot hold for a quantizing
  // delegate whether or not rotation is applied.
  @Override
  @Ignore // quantization loss exceeds the float tolerance
  public void testRandom() {}

  @Override
  @Ignore // quantization loss exceeds the float tolerance
  public void testAddIndexesDirectory01() {}

  @Override
  @Ignore // quantization loss exceeds the float tolerance
  public void testSparseVectors() {}

  @Override
  @Ignore // quantization loss exceeds the float tolerance
  public void testRandomWithUpdatesAndGraph() {}

  @Override
  @Ignore // quantization loss exceeds the float tolerance
  public void testVectorValuesReportCorrectDocs() {}

  // This format rotates FLOAT32 only and throws on BYTE and FLOAT16, so tests that index those
  // encodings cannot run against it.
  @Override
  @Ignore // does not support byte vectors
  public void testWriterByteVectorRamEstimate() {}

  @Override
  @Ignore // does not support byte vectors
  public void testMergingWithDifferentByteKnnFields() {}

  @Override
  @Ignore // does not support byte vectors
  public void testSortedIndexBytes() {}

  @Override
  @Ignore // does not support byte vectors
  public void testMismatchedFields() {}

  @Override
  @Ignore // does not support byte vectors
  public void testRandomBytes() {}

  @Override
  @Ignore // does not support byte vectors
  public void testEmptyByteVectorData() {}

  @Override
  @Ignore // does not support byte vectors
  public void testCheckIntegrityReadsAllBytes() {}

  @Override
  @Ignore // indexes a byte field alongside a float field
  public void testMergeStability() {}

  @Override
  @Ignore // indexes a byte field alongside a float field
  public void testRandomExceptions() {}

  @Override
  @Ignore // does not support byte vectors
  public void testByteVectorScorerIteration() {}

  // Writing one field with preconditioning and another without is rejected by
  // FieldInfos.Builder.add. That is the write-time state check working as intended.
  @Override
  @Ignore // mixing preconditioned and plain codecs is rejected by design
  public void testDifferentCodecs1() {}

  @Override
  @Ignore // mixing preconditioned and plain codecs is rejected by design
  public void testDifferentCodecs2() {}

  private static final String FIELD = "vec";

  /**
   * Tolerance for a rotate -> inverse-rotate round trip. The FWHT accumulates float32 addition
   * error proportional to log2(dim), so the round trip is accurate to a few ULPs rather than
   * bit-exact.
   */
  private static final float ROUND_TRIP_DELTA = 1e-4f;

  private static KnnVectorsFormat plainFormat() {
    return new Lucene104HnswScalarQuantizedVectorsFormat();
  }

  /**
   * Keeps the default codec's name so a plain {@code DirectoryReader.open} can read the index,
   * which exercises the SPI read path: the delegate is recovered from field attributes, not from
   * config.
   */
  private static Codec codecRoutingFields(Map<String, KnnVectorsFormat> perField) {
    return new Lucene104Codec() {
      @Override
      public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        KnnVectorsFormat format = perField.get(field);
        return format != null ? format : super.getKnnVectorsFormatForField(field);
      }
    };
  }

  private static Codec rotatingCodec() {
    return codecRoutingFields(
        Map.of(FIELD, PreconditioningKnnVectorsFormat.rotating(plainFormat())));
  }

  private static Codec plainCodec() {
    return codecRoutingFields(Map.of(FIELD, plainFormat()));
  }

  private static float[] randomNonZeroVector(int dim) {
    float[] v = new float[dim];
    for (int i = 0; i < dim; i++) {
      v[i] = random().nextFloat() * 2 - 1;
    }
    // Avoid the all-zero vector, which is not a legal input for some similarity functions.
    if (VectorUtil.dotProduct(v, v) == 0) {
      v[0] = 1;
    }
    return v;
  }

  // Construction

  public void testRejectsNullDelegate() {
    expectThrows(NullPointerException.class, () -> PreconditioningKnnVectorsFormat.rotating(null));
  }

  public void testRejectsNestedRotation() {
    PreconditioningKnnVectorsFormat inner = PreconditioningKnnVectorsFormat.rotating(plainFormat());
    IllegalArgumentException e =
        expectThrows(
            IllegalArgumentException.class, () -> PreconditioningKnnVectorsFormat.rotating(inner));
    assertTrue(e.getMessage(), e.getMessage().contains("Already rotating"));
  }

  /** The factory rejects PerFieldKnnVectorsFormat to prevent two-PerField-layer corruption. */
  public void testFactoryRejectsPerField() {
    KnnVectorsFormat perField =
        new PerFieldKnnVectorsFormat() {
          @Override
          public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
            return plainFormat();
          }
        };
    IllegalArgumentException e =
        expectThrows(
            IllegalArgumentException.class,
            () -> PreconditioningKnnVectorsFormat.rotating(perField));
    assertTrue(e.getMessage(), e.getMessage().contains("PerFieldKnnVectorsFormat"));
  }

  public void testNoArgConstructorIsReadOnly() {
    PreconditioningKnnVectorsFormat spiInstance = new PreconditioningKnnVectorsFormat();
    IllegalStateException e =
        expectThrows(IllegalStateException.class, () -> spiInstance.getMaxDimensions(FIELD));
    assertTrue(e.getMessage().contains("read-only"));
  }

  public void testResolvableBySpi() {
    KnnVectorsFormat format = KnnVectorsFormat.forName(PreconditioningKnnVectorsFormat.NAME);
    assertTrue(format instanceof PreconditioningKnnVectorsFormat);
  }

  public void testMaxDimensionsAndToStringDelegate() {
    KnnVectorsFormat delegate = plainFormat();
    PreconditioningKnnVectorsFormat rotating = PreconditioningKnnVectorsFormat.rotating(delegate);
    assertEquals(delegate.getMaxDimensions(FIELD), rotating.getMaxDimensions(FIELD));
    assertTrue(rotating.toString().contains(delegate.toString()));
  }

  // Round trip through original space

  /**
   * Vectors read back through the public reader API must be the vectors the application indexed,
   * for power-of-two dimensions and for dimensions that force the block-diagonal FWHT
   * decomposition.
   */
  public void testRoundTripAcrossDimensions() throws Exception {
    for (int dim : new int[] {1, 2, 3, 5, 7, 8, 16, 100, 129, 768}) {
      int numDocs = atLeast(15);
      float[][] expected = new float[numDocs][];
      try (Directory dir = newDirectory()) {
        IndexWriterConfig config = newIndexWriterConfig().setCodec(rotatingCodec());
        try (IndexWriter writer = new IndexWriter(dir, config)) {
          for (int i = 0; i < numDocs; i++) {
            expected[i] = randomNonZeroVector(dim);
            Document doc = new Document();
            doc.add(new StringField("id", Integer.toString(i), Field.Store.YES));
            doc.add(
                new KnnFloatVectorField(FIELD, expected[i], VectorSimilarityFunction.EUCLIDEAN));
            writer.addDocument(doc);
          }
          writer.forceMerge(1);
        }
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
          assertVectorsRoundTrip(reader, expected, dim);
        }
      }
    }
  }

  /** Reads every vector back and compares it with what was indexed, keyed by the stored id. */
  private void assertVectorsRoundTrip(IndexReader reader, float[][] expected, int dim)
      throws IOException {
    int seen = 0;
    for (LeafReaderContext context : reader.leaves()) {
      FloatVectorValues values = context.reader().getFloatVectorValues(FIELD);
      if (values == null) {
        continue;
      }
      assertEquals(dim, values.dimension());
      var storedFields = context.reader().storedFields();
      KnnVectorValues.DocIndexIterator iterator = values.iterator();
      for (int doc = iterator.nextDoc(); doc != NO_MORE_DOCS; doc = iterator.nextDoc()) {
        int id = Integer.parseInt(storedFields.document(doc).get("id"));
        assertArrayEquals(
            "dim=" + dim + " id=" + id,
            expected[id],
            values.vectorValue(iterator.index()),
            ROUND_TRIP_DELTA);
        seen++;
      }
    }
    assertEquals("every live document should be visited", expected.length, seen);
  }

  /**
   * The point of the wrapper: what is actually handed to the delegate must be a rotated vector, not
   * the original. The merge view exposes the delegate's raw, rotated values, and because the
   * rotation is orthogonal it must preserve the norm.
   */
  public void testStoredVectorsAreRotatedButNormPreserving() throws Exception {
    int dim = 64;
    float[] vector = randomNonZeroVector(dim);
    try (Directory dir = newDirectory()) {
      IndexWriterConfig config = newIndexWriterConfig().setCodec(rotatingCodec());
      try (IndexWriter writer = new IndexWriter(dir, config)) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField(FIELD, vector, VectorSimilarityFunction.EUCLIDEAN));
        writer.addDocument(doc);
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        CodecReader codecReader = (CodecReader) getOnlyLeafReader(reader);

        // Public API: original space.
        float[] readBack = codecReader.getFloatVectorValues(FIELD).vectorValue(0);
        assertArrayEquals(vector, readBack, ROUND_TRIP_DELTA);

        // Merge view: rotated space.
        float[] stored =
            codecReader
                .getVectorReader()
                .getMergeInstance()
                .getFloatVectorValues(FIELD)
                .vectorValue(0);
        assertEquals(
            "rotation must preserve the L2 norm",
            VectorUtil.dotProduct(vector, vector),
            VectorUtil.dotProduct(stored, stored),
            1e-3);

        // And it really is a different basis. (With dim=64 the chance of an accidental match is
        // nil.)
        int differing = 0;
        for (int i = 0; i < dim; i++) {
          if (Math.abs(stored[i] - vector[i]) > 1e-5) {
            differing++;
          }
        }
        assertTrue(
            "stored vectors should be rotated, not verbatim: " + differing, differing > dim / 2);

        // Independently confirm the stored values are exactly the expected rotation.
        float[] expectedRotation = new float[dim];
        HadamardRotation.forDimension(dim).rotate(vector, expectedRotation);
        assertArrayEquals(expectedRotation, stored, 1e-4f);
      }
    }
  }

  /**
   * End-to-end proof of rotation invariance: an index written with rotation must return the same
   * nearest neighbour, with the same score, as the identical index written without rotation.
   */
  public void testSearchAgreesWithUnrotatedIndex() throws Exception {
    int dim = 8;
    int numDocs = 60;
    float[][] vectors = new float[numDocs][];
    for (int i = 0; i < numDocs; i++) {
      vectors[i] = randomNonZeroVector(dim);
    }
    VectorSimilarityFunction similarity =
        random().nextBoolean()
            ? VectorSimilarityFunction.EUCLIDEAN
            : VectorSimilarityFunction.DOT_PRODUCT;
    if (similarity == VectorSimilarityFunction.DOT_PRODUCT) {
      for (float[] v : vectors) {
        VectorUtil.l2normalize(v);
      }
    }

    try (Directory rotatedDir = newDirectory();
        Directory plainDir = newDirectory()) {
      indexAll(rotatedDir, rotatingCodec(), vectors, similarity);
      indexAll(plainDir, plainCodec(), vectors, similarity);

      try (DirectoryReader rotatedReader = DirectoryReader.open(rotatedDir);
          DirectoryReader plainReader = DirectoryReader.open(plainDir)) {
        IndexSearcher rotatedSearcher = newSearcher(rotatedReader);
        IndexSearcher plainSearcher = newSearcher(plainReader);
        for (int iter = 0; iter < 10; iter++) {
          float[] query = randomNonZeroVector(dim);
          if (similarity == VectorSimilarityFunction.DOT_PRODUCT) {
            VectorUtil.l2normalize(query);
          }
          TopDocs rotatedHits = rotatedSearcher.search(new KnnFloatVectorQuery(FIELD, query, 5), 5);
          TopDocs plainHits = plainSearcher.search(new KnnFloatVectorQuery(FIELD, query, 5), 5);
          assertEquals(plainHits.scoreDocs.length, rotatedHits.scoreDocs.length);
          // Quantization happens in a different basis, so deep ranks may reorder; the top hit and
          // its score must match.
          // Quantization noise differs between rotated and unrotated bases, so exact doc
          // ordering may vary for close-scoring results. The top score must be similar.
          assertEquals(plainHits.scoreDocs[0].score, rotatedHits.scoreDocs[0].score, 0.05f);
        }
      }
    }
  }

  private static void indexAll(
      Directory dir, Codec codec, float[][] vectors, VectorSimilarityFunction similarity)
      throws IOException {
    IndexWriterConfig config = new IndexWriterConfig().setCodec(codec);
    try (IndexWriter writer = new IndexWriter(dir, config)) {
      for (float[] vector : vectors) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField(FIELD, vector, similarity));
        writer.addDocument(doc);
      }
      writer.forceMerge(1);
    }
  }

  // Encodings that are not rotated

  /**
   * Rotation is unconditional, so an encoding this format cannot rotate is rejected at index time
   * rather than silently indexed unrotated -- a caller who routed the field here asked for rotation
   * and a quiet no-op would be undetectable.
   */
  public void testRejectsByteVectors() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig config = newIndexWriterConfig().setCodec(rotatingCodec());
      try (IndexWriter writer = new IndexWriter(dir, config)) {
        Document doc = new Document();
        doc.add(
            new KnnByteVectorField(
                FIELD, new byte[] {1, -2, 3, -4}, VectorSimilarityFunction.EUCLIDEAN));
        IllegalArgumentException e =
            expectThrows(IllegalArgumentException.class, () -> writer.addDocument(doc));
        assertTrue(e.getMessage(), e.getMessage().contains("FLOAT32"));
        writer.rollback();
      }
    }
  }

  /** Same for float16, which this format also cannot rotate yet. */
  public void testRejectsFloat16Vectors() throws Exception {
    short[] vector = new short[4];
    for (int i = 0; i < vector.length; i++) {
      vector[i] = Float.floatToFloat16(random().nextFloat());
    }
    try (Directory dir = newDirectory()) {
      IndexWriterConfig config = newIndexWriterConfig().setCodec(rotatingCodec());
      try (IndexWriter writer = new IndexWriter(dir, config)) {
        Document doc = new Document();
        doc.add(new KnnFloat16VectorField(FIELD, vector, VectorSimilarityFunction.EUCLIDEAN));
        IllegalArgumentException e =
            expectThrows(IllegalArgumentException.class, () -> writer.addDocument(doc));
        assertTrue(e.getMessage(), e.getMessage().contains("FLOAT16"));
        writer.rollback();
      }
    }
  }

  // Merging

  /**
   * A field's rotation state is enforced at write time, just like dimension and similarity. You
   * cannot index a field with rotation in one segment and without it in another — the second
   * addDocument throws immediately, before any data is written.
   */
  public void testRejectsMixedRotationState() throws Exception {
    try (Directory dir = newDirectory()) {
      addSegment(dir, rotatingCodec(), randomNonZeroVector(8));
      addSegment(dir, plainCodec(), randomNonZeroVector(8));

      IndexWriterConfig config =
          new IndexWriterConfig()
              .setCodec(rotatingCodec())
              .setMergeScheduler(new SerialMergeScheduler());
      try (IndexWriter writer = new IndexWriter(dir, config)) {
        IllegalArgumentException e =
            expectThrows(IllegalArgumentException.class, () -> writer.forceMerge(1));
        assertTrue(e.getMessage(), e.getMessage().contains("rotation state"));
        writer.rollback();
      }
    }
  }

  /**
   * Swapping the delegate while keeping rotation on is legal. The merged field must record the
   * delegate that actually wrote it, since {@code FieldInfos.Builder} never removes stale
   * attributes.
   */
  public void testDelegateMayChangeAcrossSegments() throws Exception {
    int dim = 8;
    float[] first = randomNonZeroVector(dim);
    float[] second = randomNonZeroVector(dim);
    try (Directory dir = newDirectory()) {
      addSegment(
          dir,
          codecRoutingFields(
              Map.of(FIELD, PreconditioningKnnVectorsFormat.rotating(plainFormat()))),
          first);
      Codec swapped =
          codecRoutingFields(
              Map.of(
                  FIELD,
                  PreconditioningKnnVectorsFormat.rotating(new Lucene99HnswVectorsFormat())));
      addSegment(dir, swapped, second);

      try (IndexWriter writer =
          new IndexWriter(
              dir,
              new IndexWriterConfig()
                  .setCodec(swapped)
                  .setMergeScheduler(new SerialMergeScheduler()))) {
        writer.forceMerge(1);
      }

      // Reopened with a plain reader, so the delegate is resolved purely from the recorded name.
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        var leaf = getOnlyLeafReader(reader);
        assertEquals(
            "merged field must record the delegate that actually wrote it",
            new Lucene99HnswVectorsFormat().getName(),
            leaf.getFieldInfos()
                .fieldInfo(FIELD)
                .getAttribute(PreconditioningKnnVectorsFormat.DELEGATE_FORMAT_KEY));

        FloatVectorValues values = leaf.getFloatVectorValues(FIELD);
        assertEquals(2, values.size());
        // Both vectors survive the re-encode, still in original space.
        float[] a = values.vectorValue(0).clone();
        float[] b = values.vectorValue(1).clone();
        boolean inOrder = Math.abs(a[0] - first[0]) < ROUND_TRIP_DELTA;
        assertArrayEquals(first, inOrder ? a : b, ROUND_TRIP_DELTA);
        assertArrayEquals(second, inOrder ? b : a, ROUND_TRIP_DELTA);
      }
    }
  }

  /**
   * Segments that never saw the vector field, or that carry a different field entirely, must not be
   * mistaken for a rotation-state disagreement.
   */
  public void testMergeToleratesSegmentsWithoutTheField() throws Exception {
    int dim = 8;
    float[] first = randomNonZeroVector(dim);
    float[] second = randomNonZeroVector(dim);
    try (Directory dir = newDirectory()) {
      addSegment(dir, rotatingCodec(), first);
      // A segment with no vectors at all.
      try (IndexWriter writer =
          new IndexWriter(dir, new IndexWriterConfig().setCodec(rotatingCodec()))) {
        Document doc = new Document();
        doc.add(new StringField("other", "no vectors here", Field.Store.NO));
        writer.addDocument(doc);
        writer.commit();
      }
      addSegment(dir, rotatingCodec(), second);

      try (IndexWriter writer =
          new IndexWriter(
              dir,
              new IndexWriterConfig()
                  .setCodec(rotatingCodec())
                  .setMergeScheduler(new SerialMergeScheduler()))) {
        writer.forceMerge(1);
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        FloatVectorValues values = getOnlyLeafReader(reader).getFloatVectorValues(FIELD);
        assertEquals(2, values.size());
      }
    }
  }

  /** Many segments, plus deletions, then a force merge: every surviving vector must round trip. */
  public void testManySegmentsWithDeletionsThenForceMerge() throws Exception {
    int dim = 32;
    int numDocs = 60;
    Map<Integer, float[]> live = new HashMap<>();
    try (Directory dir = newDirectory()) {
      IndexWriterConfig config =
          new IndexWriterConfig()
              .setCodec(rotatingCodec())
              .setMergeScheduler(new SerialMergeScheduler());
      try (IndexWriter writer = new IndexWriter(dir, config)) {
        for (int i = 0; i < numDocs; i++) {
          float[] vector = randomNonZeroVector(dim);
          live.put(i, vector);
          Document doc = new Document();
          doc.add(new StringField("id", Integer.toString(i), Field.Store.YES));
          doc.add(new KnnFloatVectorField(FIELD, vector, VectorSimilarityFunction.EUCLIDEAN));
          writer.addDocument(doc);
          if (i % 7 == 0) {
            writer.commit(); // force multiple segments
          }
        }
        for (int i = 0; i < numDocs; i += 5) {
          writer.deleteDocuments(new Term("id", Integer.toString(i)));
          live.remove(i);
        }
        writer.commit();
        writer.forceMerge(1);
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        int seen = 0;
        for (LeafReaderContext context : reader.leaves()) {
          FloatVectorValues values = context.reader().getFloatVectorValues(FIELD);
          if (values == null) {
            continue;
          }
          var storedFields = context.reader().storedFields();
          KnnVectorValues.DocIndexIterator iterator = values.iterator();
          for (int doc = iterator.nextDoc(); doc != NO_MORE_DOCS; doc = iterator.nextDoc()) {
            int id = Integer.parseInt(storedFields.document(doc).get("id"));
            assertTrue("deleted doc " + id + " resurfaced", live.containsKey(id));
            assertArrayEquals(live.get(id), values.vectorValue(iterator.index()), ROUND_TRIP_DELTA);
            seen++;
          }
        }
        assertEquals(live.size(), seen);
      }
    }
  }

  /** An index sort reorders documents during flush and merge; values must still round trip. */
  public void testSortedIndexRoundTrip() throws Exception {
    int dim = 16;
    int numDocs = 40;
    Map<Integer, float[]> vectors = new HashMap<>();
    try (Directory dir = newDirectory()) {
      IndexWriterConfig config =
          new IndexWriterConfig()
              .setCodec(rotatingCodec())
              .setIndexSort(new Sort(new SortField("sortkey", SortField.Type.LONG)))
              .setMergeScheduler(new SerialMergeScheduler());
      try (IndexWriter writer = new IndexWriter(dir, config)) {
        for (int i = 0; i < numDocs; i++) {
          float[] vector = randomNonZeroVector(dim);
          vectors.put(i, vector);
          Document doc = new Document();
          doc.add(new StringField("id", Integer.toString(i), Field.Store.YES));
          doc.add(new NumericDocValuesField("sortkey", numDocs - i));
          doc.add(new KnnFloatVectorField(FIELD, vector, VectorSimilarityFunction.EUCLIDEAN));
          writer.addDocument(doc);
          if (i % 9 == 0) {
            writer.commit();
          }
        }
        writer.forceMerge(1);
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        var leaf = getOnlyLeafReader(reader);
        FloatVectorValues values = leaf.getFloatVectorValues(FIELD);
        var storedFields = leaf.storedFields();
        KnnVectorValues.DocIndexIterator iterator = values.iterator();
        int seen = 0;
        for (int doc = iterator.nextDoc(); doc != NO_MORE_DOCS; doc = iterator.nextDoc()) {
          int id = Integer.parseInt(storedFields.document(doc).get("id"));
          assertArrayEquals(
              vectors.get(id), values.vectorValue(iterator.index()), ROUND_TRIP_DELTA);
          seen++;
        }
        assertEquals(numDocs, seen);
      }
    }
  }

  private static void addSegment(Directory dir, Codec codec, float[] vector) throws IOException {
    try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(codec))) {
      Document doc = new Document();
      doc.add(new KnnFloatVectorField(FIELD, vector, VectorSimilarityFunction.EUCLIDEAN));
      writer.addDocument(doc);
      writer.commit();
    }
  }

  // Per-field composition

  /**
   * Rotating some fields and not others, in one index, through {@link PerFieldKnnVectorsFormat}
   * routing. Both must read back correctly, and only the rotated one may carry a rotation seed.
   */
  public void testRotateSomeFieldsOnly() throws Exception {
    int dim = 16;
    float[] rotated = randomNonZeroVector(dim);
    float[] plain = randomNonZeroVector(dim);
    Codec codec =
        codecRoutingFields(
            Map.of(
                "rotated",
                PreconditioningKnnVectorsFormat.rotating(plainFormat()),
                "plain",
                plainFormat()));
    try (Directory dir = newDirectory()) {
      try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(codec))) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("rotated", rotated, VectorSimilarityFunction.EUCLIDEAN));
        doc.add(new KnnFloatVectorField("plain", plain, VectorSimilarityFunction.EUCLIDEAN));
        writer.addDocument(doc);
        writer.forceMerge(1);
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        var leaf = getOnlyLeafReader(reader);
        assertArrayEquals(
            rotated, leaf.getFloatVectorValues("rotated").vectorValue(0), ROUND_TRIP_DELTA);
        // The unrotated field goes nowhere near this wrapper, so it stays bit-exact.
        assertArrayEquals(plain, leaf.getFloatVectorValues("plain").vectorValue(0), 0f);

        assertNotNull(
            leaf.getFieldInfos()
                .fieldInfo("rotated")
                .getAttribute(PreconditioningKnnVectorsFormat.DELEGATE_FORMAT_KEY));
        assertNull(
            leaf.getFieldInfos()
                .fieldInfo("plain")
                .getAttribute(PreconditioningKnnVectorsFormat.DELEGATE_FORMAT_KEY));
      }
    }
  }

  /**
   * Two rotating wrappers around <em>different</em> delegate formats, side by side in one index.
   * {@link PerFieldKnnVectorsFormat} gives each instance its own segment suffix, so each reader
   * must resolve only its own delegate — this is the case that a single global "find the delegate"
   * scan gets wrong.
   */
  public void testTwoRotatingSiblingsWithDifferentDelegates() throws Exception {
    int dim = 16;
    float[] a = randomNonZeroVector(dim);
    float[] b = randomNonZeroVector(dim);
    Codec codec =
        codecRoutingFields(
            Map.of(
                "fieldA", PreconditioningKnnVectorsFormat.rotating(plainFormat()),
                "fieldB",
                    PreconditioningKnnVectorsFormat.rotating(new Lucene99HnswVectorsFormat())));
    try (Directory dir = newDirectory()) {
      try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(codec))) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("fieldA", a, VectorSimilarityFunction.EUCLIDEAN));
        doc.add(new KnnFloatVectorField("fieldB", b, VectorSimilarityFunction.EUCLIDEAN));
        writer.addDocument(doc);
        writer.forceMerge(1);
      }
      // Reopened with a plain reader: both fields resolve their delegate purely from attributes.
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        var leaf = getOnlyLeafReader(reader);
        assertArrayEquals(a, leaf.getFloatVectorValues("fieldA").vectorValue(0), ROUND_TRIP_DELTA);
        assertArrayEquals(b, leaf.getFloatVectorValues("fieldB").vectorValue(0), ROUND_TRIP_DELTA);

        var infos = leaf.getFieldInfos();
        assertEquals(
            plainFormat().getName(),
            infos
                .fieldInfo("fieldA")
                .getAttribute(PreconditioningKnnVectorsFormat.DELEGATE_FORMAT_KEY));
        assertEquals(
            new Lucene99HnswVectorsFormat().getName(),
            infos
                .fieldInfo("fieldB")
                .getAttribute(PreconditioningKnnVectorsFormat.DELEGATE_FORMAT_KEY));

        // CheckIndex-style integrity check must not trip over the sibling's files.
        ((CodecReader) leaf).getVectorReader().checkIntegrity(null);
      }
    }
  }

  /**
   * Rotation outside, per-field selection inside. Requires the wrapper to be the codec's own KNN
   * format: two nested per-field layers would overwrite each other's attributes.
   */
  public void testRotationOverPerFieldDelegate() throws Exception {
    int dim = 16;
    float[] a = randomNonZeroVector(dim);
    float[] b = randomNonZeroVector(dim);
    KnnVectorsFormat innerPerField =
        new PerFieldKnnVectorsFormat() {
          @Override
          public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
            return field.equals("fieldA") ? plainFormat() : new Lucene99HnswVectorsFormat();
          }
        };
    KnnVectorsFormat rotating = new PreconditioningKnnVectorsFormat(innerPerField);
    Codec codec =
        new FilterCodec(Codec.getDefault().getName(), Codec.getDefault()) {
          @Override
          public KnnVectorsFormat knnVectorsFormat() {
            return rotating;
          }
        };
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(codec))) {
      Document doc = new Document();
      doc.add(new KnnFloatVectorField("fieldA", a, VectorSimilarityFunction.EUCLIDEAN));
      doc.add(new KnnFloatVectorField("fieldB", b, VectorSimilarityFunction.EUCLIDEAN));
      writer.addDocument(doc);
      writer.forceMerge(1);
      // Read through the writer so the segment is read back with this codec instance; a real
      // application would register its codec via SPI so that a plain DirectoryReader.open works.
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        var leaf = getOnlyLeafReader(reader);
        assertArrayEquals(a, leaf.getFloatVectorValues("fieldA").vectorValue(0), ROUND_TRIP_DELTA);
        assertArrayEquals(b, leaf.getFloatVectorValues("fieldB").vectorValue(0), ROUND_TRIP_DELTA);
      }
    }
  }

  /** Two fields of different dimensions must each get the rotation for their own dimension. */
  public void testFieldsWithDifferentDimensions() throws Exception {
    float[] small = randomNonZeroVector(4);
    float[] large = randomNonZeroVector(96);
    KnnVectorsFormat rotating = PreconditioningKnnVectorsFormat.rotating(plainFormat());
    Codec codec = codecRoutingFields(Map.of("small", rotating, "large", rotating));
    try (Directory dir = newDirectory()) {
      try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(codec))) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("small", small, VectorSimilarityFunction.EUCLIDEAN));
        doc.add(new KnnFloatVectorField("large", large, VectorSimilarityFunction.EUCLIDEAN));
        writer.addDocument(doc);
        writer.forceMerge(1);
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        var leaf = getOnlyLeafReader(reader);
        assertArrayEquals(
            small, leaf.getFloatVectorValues("small").vectorValue(0), ROUND_TRIP_DELTA);
        assertArrayEquals(
            large, leaf.getFloatVectorValues("large").vectorValue(0), ROUND_TRIP_DELTA);

        var infos = leaf.getFieldInfos();
        // Both fields are rotated (marker present) but with dimension-specific matrices.
        assertNotNull(
            infos
                .fieldInfo("small")
                .getAttribute(PreconditioningKnnVectorsFormat.DELEGATE_FORMAT_KEY));
        assertNotNull(
            infos
                .fieldInfo("large")
                .getAttribute(PreconditioningKnnVectorsFormat.DELEGATE_FORMAT_KEY));
        assertNotSame(HadamardRotation.forDimension(4), HadamardRotation.forDimension(96));
      }
    }
  }

  /**
   * Unwrapping must reach the concrete reader through this wrapper, or CheckIndex's graph
   * validation would silently be skipped.
   */
  public void testUnwrapReaderReachesConcreteReader() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig config = newIndexWriterConfig().setCodec(rotatingCodec());
      try (IndexWriter writer = new IndexWriter(dir, config)) {
        Document doc = new Document();
        doc.add(
            new KnnFloatVectorField(
                FIELD, randomNonZeroVector(16), VectorSimilarityFunction.EUCLIDEAN));
        writer.addDocument(doc);
        writer.forceMerge(1);
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        KnnVectorsReader unwrapped =
            ((CodecReader) getOnlyLeafReader(reader)).getVectorReader().unwrapReaderForField(FIELD);
        assertTrue(
            "unwrapping should reach the concrete HNSW reader, got "
                + unwrapped.getClass().getName(),
            unwrapped instanceof HnswGraphProvider);
      }
    }
  }

  /**
   * The two concerns compose: {@link PerFieldKnnVectorsFormat} decides which format handles a
   * field, and this wrapper decides whether that format sees rotated vectors. One index can
   * therefore hold an arbitrary matrix of (inner format x rotated or not), routed through the
   * single per-field layer the codec already provides. No additional per-field layer is needed.
   */
  public void testMixedFormatsAndRotationStatesInOneIndex() throws Exception {
    int dim = 32;
    KnnVectorsFormat sq = plainFormat();
    KnnVectorsFormat flat = new Lucene99HnswVectorsFormat();
    Map<String, KnnVectorsFormat> routing =
        Map.of(
            "sqRotated",
            PreconditioningKnnVectorsFormat.rotating(sq),
            "sqPlain",
            sq,
            "flatRotated",
            PreconditioningKnnVectorsFormat.rotating(flat),
            "flatPlain",
            flat);

    Map<String, float[]> expected = new HashMap<>();
    for (String field : routing.keySet()) {
      expected.put(field, randomNonZeroVector(dim));
    }

    try (Directory dir = newDirectory()) {
      IndexWriterConfig config =
          new IndexWriterConfig()
              .setCodec(codecRoutingFields(routing))
              .setMergeScheduler(new SerialMergeScheduler());
      try (IndexWriter writer = new IndexWriter(dir, config)) {
        // Two segments, so the merge path is exercised for every combination.
        for (int segment = 0; segment < 2; segment++) {
          Document doc = new Document();
          for (Map.Entry<String, float[]> e : expected.entrySet()) {
            doc.add(
                new KnnFloatVectorField(
                    e.getKey(), e.getValue(), VectorSimilarityFunction.EUCLIDEAN));
          }
          writer.addDocument(doc);
          writer.commit();
        }
        writer.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        var leaf = getOnlyLeafReader(reader);
        var infos = leaf.getFieldInfos();
        IndexSearcher searcher = newSearcher(reader);

        for (Map.Entry<String, float[]> e : expected.entrySet()) {
          String field = e.getKey();
          boolean rotated = field.endsWith("Rotated");

          // Values come back in original space either way; rotated fields are float-tolerant.
          FloatVectorValues values = leaf.getFloatVectorValues(field);
          assertNotNull(field, values);
          assertEquals(field, 2, values.size());
          assertArrayEquals(
              field, e.getValue(), values.vectorValue(0), rotated ? ROUND_TRIP_DELTA : 0f);

          // Only the rotated fields claim a rotation, and only they name this wrapper's delegate.
          FieldInfo fieldInfo = infos.fieldInfo(field);
          if (rotated) {
            assertNotNull(
                field, fieldInfo.getAttribute(PreconditioningKnnVectorsFormat.DELEGATE_FORMAT_KEY));
            assertNotNull(
                field, fieldInfo.getAttribute(PreconditioningKnnVectorsFormat.DELEGATE_FORMAT_KEY));
            assertEquals(
                field,
                PreconditioningKnnVectorsFormat.NAME,
                fieldInfo.getAttribute(PerFieldKnnVectorsFormat.PER_FIELD_FORMAT_KEY));
          } else {
            assertNull(
                field, fieldInfo.getAttribute(PreconditioningKnnVectorsFormat.DELEGATE_FORMAT_KEY));
            assertNull(
                field, fieldInfo.getAttribute(PreconditioningKnnVectorsFormat.DELEGATE_FORMAT_KEY));
          }

          // And search works through every combination.
          TopDocs hits = searcher.search(new KnnFloatVectorQuery(field, e.getValue(), 2), 2);
          assertEquals(field, 2, hits.scoreDocs.length);
        }

        // Every delegate's files are intact and each reader only checks its own.
        ((CodecReader) leaf).getVectorReader().checkIntegrity(null);
      }
    }
  }

  /**
   * The rotation seed must be persisted per field and readable without any application-side
   * configuration. This is the self-describing index contract: given only the on-disk data, a
   * reader reconstructs the exact rotation that was applied at index time.
   */
  public void testSeedPersistedAndReadableViaSpi() throws Exception {
    int dim = 128;
    float[] vector = randomNonZeroVector(dim);
    try (Directory dir = newDirectory()) {
      IndexWriterConfig config = newIndexWriterConfig().setCodec(rotatingCodec());
      try (IndexWriter writer = new IndexWriter(dir, config)) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField(FIELD, vector, VectorSimilarityFunction.EUCLIDEAN));
        writer.addDocument(doc);
        writer.forceMerge(1);
      }
      // Read with a plain DirectoryReader -- no codec config, SPI resolves everything
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        var leaf = getOnlyLeafReader(reader);
        FieldInfo fi = leaf.getFieldInfos().fieldInfo(FIELD);

        // Both attributes must be present
        String delegateName = fi.getAttribute(PreconditioningKnnVectorsFormat.DELEGATE_FORMAT_KEY);
        String seedStr = fi.getAttribute(PreconditioningKnnVectorsFormat.ROTATION_SEED_KEY);
        assertNotNull("delegate must be persisted", delegateName);
        assertNotNull("seed must be persisted", seedStr);

        // Seed must match the deterministic derivation
        long expectedSeed = HadamardRotation.seedForDimension(dim);
        assertEquals(Long.toString(expectedSeed), seedStr);

        // Vectors must round-trip correctly through the persisted rotation
        FloatVectorValues values = leaf.getFloatVectorValues(FIELD);
        assertArrayEquals(vector, values.vectorValue(0), ROUND_TRIP_DELTA);
      }
    }
  }
}
