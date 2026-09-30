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

import static java.lang.String.format;
import static org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter.MergeScorerData;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsWriter;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.SameThreadExecutorService;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues.ScalarEncoding;
import org.apache.lucene.util.quantization.QuantizedVectorsReader;
import org.junit.Before;

public class TestLucene104HnswScalarQuantizedVectorsFormat extends BaseKnnVectorsFormatTestCase {

  private KnnVectorsFormat format;
  private ScalarEncoding encoding;

  @Before
  @Override
  public void setUp() throws Exception {
    var encodingValues = ScalarEncoding.values();
    encoding = encodingValues[random().nextInt(encodingValues.length)];
    format =
        new Lucene104HnswScalarQuantizedVectorsFormat(
            encoding,
            Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN,
            Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH,
            1,
            null);
    super.setUp();
  }

  @Override
  protected Codec getCodec() {
    return TestUtil.alwaysKnnVectorsFormat(format);
  }

  public void testToString() {
    FilterCodec customCodec =
        new FilterCodec("foo", Codec.getDefault()) {
          @Override
          public KnnVectorsFormat knnVectorsFormat() {
            return new Lucene104HnswScalarQuantizedVectorsFormat(
                ScalarEncoding.UNSIGNED_BYTE, 10, 20, 1, null);
          }
        };
    String expectedPattern =
        "Lucene104HnswScalarQuantizedVectorsFormat(name=Lucene104HnswScalarQuantizedVectorsFormat,"
            + " maxConn=10, beamWidth=20, tinySegmentsThreshold=100,"
            + " flatVectorFormat=Lucene104ScalarQuantizedVectorsFormat(name=Lucene104ScalarQuantizedVectorsFormat,"
            + " encoding=UNSIGNED_BYTE,"
            + " flatVectorScorer=Lucene104ScalarQuantizedVectorScorer(nonQuantizedDelegate=%s()),"
            + " rawVectorFormat=Lucene99FlatVectorsFormat(vectorsScorer=%s())))";

    var defaultScorer =
        format(Locale.ROOT, expectedPattern, "DefaultFlatVectorScorer", "DefaultFlatVectorScorer");
    var memSegScorer =
        format(
            Locale.ROOT,
            expectedPattern,
            "Lucene99MemorySegmentFlatVectorsScorer",
            "Lucene99MemorySegmentFlatVectorsScorer");
    assertThat(customCodec.knnVectorsFormat().toString(), is(oneOf(defaultScorer, memSegScorer)));
  }

  /**
   * A merge that builds a graph over an asymmetric encoding quantizes the merged vectors again for
   * the query side of the merge scorer. For COSINE those vectors must be normalized first, like the
   * index side was and like a search-time query is, so the merge scorer must score a pair exactly
   * like the search-time scorer does for the same vector. The base test case only feeds unit
   * vectors, for which the two agree by accident; this uses vectors that are not.
   */
  public void testMergeCosineWithNonUnitVectors() throws Exception {
    for (ScalarEncoding asymmetric : ScalarEncoding.values()) {
      if (asymmetric.isAsymmetric() == false) {
        continue;
      }
      // a threshold of 0 makes every merge build a graph, so the merge scorer is always requested
      KnnVectorsFormat graphFormat =
          new Lucene104HnswScalarQuantizedVectorsFormat(
              asymmetric,
              Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN,
              Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH,
              1,
              null,
              0);
      IndexWriterConfig config =
          newIndexWriterConfig()
              .setCodec(TestUtil.alwaysKnnVectorsFormat(graphFormat))
              .setMergePolicy(newLogMergePolicy());
      int dim = random().nextInt(8, 64);
      int numDocs = atLeast(50);
      float[][] vectors = new float[numDocs][];
      try (Directory dir = newDirectory();
          IndexWriter w = new IndexWriter(dir, config)) {
        for (int i = 0; i < numDocs; i++) {
          float[] vector = randomVector(dim);
          // scale away from the unit sphere
          float scale = random().nextFloat(2f, 10f);
          for (int j = 0; j < dim; j++) {
            vector[j] *= scale;
          }
          vectors[i] = vector;
          Document doc = new Document();
          doc.add(new KnnFloatVectorField("f", vector, VectorSimilarityFunction.COSINE));
          w.addDocument(doc);
          if (i == numDocs / 2) {
            w.commit(); // two segments, so the merge has something to merge
          }
        }
        w.commit();
        w.forceMerge(1);
        try (IndexReader reader = DirectoryReader.open(w)) {
          LeafReader r = getOnlyLeafReader(reader);
          SegmentReader segmentReader = (SegmentReader) FilterLeafReader.unwrap(r);
          FieldInfo fieldInfo = r.getFieldInfos().fieldInfo("f");
          KnnVectorsReader vectorsReader = segmentReader.getVectorReader();
          if (vectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader fieldsReader) {
            vectorsReader = fieldsReader.getFieldReader("f");
          }
          QuantizedVectorsReader quantizedReader = (QuantizedVectorsReader) vectorsReader;
          SegmentWriteState writeState =
              new SegmentWriteState(
                  InfoStream.getDefault(),
                  dir,
                  segmentReader.getSegmentInfo().info,
                  r.getFieldInfos(),
                  null,
                  IOContext.DEFAULT);
          KnnVectorValues quantizedValues = quantizedReader.getQuantizedVectorValues("f");
          FlatVectorsScorer searchScorer =
              new Lucene104ScalarQuantizedVectorScorer(
                  FlatVectorScorerUtil.getLucene99FlatVectorsScorer());
          try (CloseableRandomVectorScorerSupplier mergeScorers =
              quantizedReader.getRandomVectorScorerSupplierForMerge(fieldInfo, writeState)) {
            FloatVectorValues rawValues = r.getFloatVectorValues("f");
            UpdateableRandomVectorScorer mergeScorer = mergeScorers.scorer();
            for (int i = 0; i < 5; i++) {
              int queryOrd = random().nextInt(numDocs);
              int targetOrd = random().nextInt(numDocs);
              // the search-time scorer normalizes a COSINE query before quantizing it
              RandomVectorScorer expected =
                  searchScorer.getRandomVectorScorer(
                      VectorSimilarityFunction.COSINE,
                      quantizedValues,
                      rawValues.vectorValue(queryOrd));
              mergeScorer.setScoringOrdinal(queryOrd);
              assertEquals(
                  "encoding " + asymmetric + " query ord " + queryOrd + " target ord " + targetOrd,
                  expected.score(targetOrd),
                  mergeScorer.score(targetOrd),
                  1e-5f);
            }
          }
        }
      }
    }
  }

  public void testSingleVectorCase() throws Exception {
    float[] vector = randomVector(random().nextInt(12, 500));
    for (VectorSimilarityFunction similarityFunction : VectorSimilarityFunction.values()) {
      try (Directory dir = newDirectory();
          IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        Document doc = new Document();
        float[] docVector =
            similarityFunction == VectorSimilarityFunction.DOT_PRODUCT
                ? VectorUtil.l2normalize(ArrayUtil.copyArray(vector))
                : vector;
        doc.add(new KnnFloatVectorField("f", docVector, similarityFunction));
        w.addDocument(doc);
        w.commit();
        try (IndexReader reader = DirectoryReader.open(w)) {
          LeafReader r = getOnlyLeafReader(reader);
          FloatVectorValues vectorValues = r.getFloatVectorValues("f");
          KnnVectorValues.DocIndexIterator docIndexIterator = vectorValues.iterator();
          assert (vectorValues.size() == 1);
          while (docIndexIterator.nextDoc() != NO_MORE_DOCS) {
            assertArrayEquals(
                docVector, vectorValues.vectorValue(docIndexIterator.index()), 0.00001f);
          }
          float[] randomVector =
              similarityFunction == VectorSimilarityFunction.DOT_PRODUCT
                  ? randomNormalizedVector(vector.length)
                  : randomVector(vector.length);
          float trueScore = similarityFunction.compare(docVector, randomVector);
          TopDocs td =
              r.searchNearestVectors(
                  "f",
                  randomVector,
                  1,
                  AcceptDocs.fromLiveDocs(null, r.maxDoc()),
                  Integer.MAX_VALUE);
          assertEquals(1, td.totalHits.value());
          assertTrue(td.scoreDocs[0].score >= 0);
          // When it's the only vector in a segment, the score should be very close to the true
          // score
          assertEquals(trueScore, td.scoreDocs[0].score, 0.01f);
        }
      }
    }
  }

  public void testLimits() {
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene104HnswScalarQuantizedVectorsFormat(-1, 20));
    expectThrows(
        IllegalArgumentException.class, () -> new Lucene104HnswScalarQuantizedVectorsFormat(0, 20));
    expectThrows(
        IllegalArgumentException.class, () -> new Lucene104HnswScalarQuantizedVectorsFormat(20, 0));
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene104HnswScalarQuantizedVectorsFormat(20, -1));
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene104HnswScalarQuantizedVectorsFormat(512 + 1, 20));
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene104HnswScalarQuantizedVectorsFormat(20, 3201));
    expectThrows(
        IllegalArgumentException.class,
        () ->
            new Lucene104HnswScalarQuantizedVectorsFormat(
                ScalarEncoding.UNSIGNED_BYTE, 20, 100, 1, new SameThreadExecutorService()));
  }

  // Ensures that all expected vector similarity functions are translatable in the format.
  public void testVectorSimilarityFuncs() {
    // This does not necessarily have to be all similarity functions, but
    // differences should be considered carefully.
    var expectedValues = Arrays.stream(VectorSimilarityFunction.values()).toList();
    assertEquals(Lucene99HnswVectorsReader.SIMILARITY_FUNCTIONS, expectedValues);
  }

  public void testSimpleOffHeapSize() throws IOException {
    float[] vector = randomVector(random().nextInt(12, 500));
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new KnnFloatVectorField("f", vector, DOT_PRODUCT));
      w.addDocument(doc);
      w.commit();
      try (IndexReader reader = DirectoryReader.open(w)) {
        LeafReader r = getOnlyLeafReader(reader);
        if (r instanceof CodecReader codecReader) {
          KnnVectorsReader knnVectorsReader = codecReader.getVectorReader();
          if (knnVectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader fieldsReader) {
            knnVectorsReader = fieldsReader.getFieldReader("f");
          }
          var fieldInfo = r.getFieldInfos().fieldInfo("f");
          var offHeap = knnVectorsReader.getOffHeapByteSize(fieldInfo);
          assertEquals(vector.length * Float.BYTES, (long) offHeap.get("vec"));
          assertNotNull(offHeap.get("vex"));
          long corrections = Float.BYTES + Float.BYTES + Float.BYTES + Integer.BYTES;
          long expected = encoding.getDocPackedLength(fieldInfo.getVectorDimension()) + corrections;
          assertEquals(expected, (long) offHeap.get("veq"));
          assertEquals(3, offHeap.size());
        }
      }
    }
  }

  /**
   * Verifies where merging gets the quantized scorer supplier used to build the HNSW graph: from
   * query data the flat writer prepared ({@link
   * Lucene104ScalarQuantizedVectorsWriter#mergeOneFlatVectorFieldForMergeScorer}) for asymmetric
   * encodings, otherwise from {@link
   * Lucene104ScalarQuantizedVectorsReader#getRandomVectorScorerSupplierForMerge}.
   */
  public void testMergeScorer() throws IOException {
    int dim = 8;

    for (ScalarEncoding scalarEncoding : ScalarEncoding.values()) {
      for (VectorEncoding vectorEncoding : VectorEncoding.values()) {
        if (vectorEncoding == VectorEncoding.BYTE) { // not applicable for BYTE
          continue;
        }

        MergeScorerCounts counts = new MergeScorerCounts();
        IndexWriterConfig config =
            newIndexWriterConfig()
                .setCodec(
                    TestUtil.alwaysKnnVectorsFormat(
                        new MergeScorerCountingFormat(scalarEncoding, counts)))
                .setMergeScheduler(new SerialMergeScheduler())
                .setMergePolicy(NoMergePolicy.INSTANCE); // no merges while indexing

        try (Directory dir = newDirectory();
            IndexWriter w = new IndexWriter(dir, config)) {
          for (int i = 0; i < 2; i++) {
            Document document = new Document();

            IndexableField field =
                switch (vectorEncoding) {
                  case BYTE ->
                      throw new IllegalStateException("not expected to run for byte vectors");
                  case FLOAT32 ->
                      new KnnFloatVectorField("v", randomNormalizedVector(dim), DOT_PRODUCT);
                };
            document.add(field);

            w.addDocument(document);
            w.commit();
          }

          String fields = vectorEncoding + " x " + scalarEncoding;
          String preparedMessage =
              fields + ": merge scorer data prepared by mergeOneFlatVectorFieldForMergeScorer";
          String fromReaderMessage = fields + ": calls to getRandomVectorScorerSupplierForMerge";

          // the merge scorer is only requested during merges, never on flush
          assertEquals(preparedMessage + " before merge", 0, counts.prepared.get());
          assertEquals(fromReaderMessage + " before merge", 0, counts.fromReader.get());

          w.getConfig().setMergePolicy(new TieredMergePolicy());
          w.forceMerge(1);

          // asymmetric encodings get their merge scorer from query data the flat writer prepared
          // while merging; symmetric encodings ask the merged reader for it
          boolean expectPrepared = scalarEncoding.isAsymmetric();
          assertEquals(preparedMessage, expectPrepared ? 1 : 0, counts.prepared.get());
          assertEquals(fromReaderMessage, expectPrepared ? 0 : 1, counts.fromReader.get());
        }
      }
    }
  }

  /** Counts how merges obtained the scorer supplier used to build the HNSW graph. */
  private static final class MergeScorerCounts {
    /** Non-null {@link MergeScorerData} returned by the flat writer while merging a field. */
    final AtomicInteger prepared = new AtomicInteger();

    /**
     * Calls to {@link Lucene104ScalarQuantizedVectorsReader#getRandomVectorScorerSupplierForMerge}.
     */
    final AtomicInteger fromReader = new AtomicInteger();
  }

  /**
   * Same as {@link Lucene104HnswScalarQuantizedVectorsFormat}, except that merges use a flat writer
   * and reader that record in {@link MergeScorerCounts} where the merge scorer came from. Keeps the
   * parent's name so that segments are still read back via SPI with the regular format.
   */
  private static final class MergeScorerCountingFormat
      extends Lucene104HnswScalarQuantizedVectorsFormat {
    private final FlatVectorsFormat flatVectorsFormat;

    MergeScorerCountingFormat(ScalarEncoding encoding, MergeScorerCounts counts) {
      // tinySegmentsThreshold=0 so a graph (and hence the merge scorer) is always built
      super(
          encoding,
          Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN,
          Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH,
          1,
          null,
          0);
      FlatVectorsFormat delegate = new Lucene104ScalarQuantizedVectorsFormat(encoding);
      FlatVectorsFormat rawVectorsFormat =
          new Lucene99FlatVectorsFormat(FlatVectorScorerUtil.getLucene99FlatVectorsScorer());
      Lucene104ScalarQuantizedVectorScorer scorer =
          new Lucene104ScalarQuantizedVectorScorer(
              FlatVectorScorerUtil.getLucene99FlatVectorsScorer());

      this.flatVectorsFormat =
          new FlatVectorsFormat(delegate.getName()) {
            @Override
            public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
              return new Lucene104ScalarQuantizedVectorsWriter(
                  state, encoding, rawVectorsFormat.fieldsWriter(state), scorer) {
                @Override
                public MergeScorerData mergeOneFlatVectorFieldForMergeScorer(
                    FieldInfo fieldInfo, MergeState mergeState, IntPredicate needsMergeScorer)
                    throws IOException {
                  MergeScorerData prepared =
                      super.mergeOneFlatVectorFieldForMergeScorer(
                          fieldInfo, mergeState, needsMergeScorer);
                  if (prepared != null) {
                    counts.prepared.incrementAndGet();
                  }
                  return prepared;
                }
              };
            }

            @Override
            public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
              return new Lucene104ScalarQuantizedVectorsReader(
                  state, rawVectorsFormat.fieldsReader(state), scorer) {
                @Override
                public CloseableRandomVectorScorerSupplier getRandomVectorScorerSupplierForMerge(
                    FieldInfo fieldInfo, SegmentWriteState segmentWriteState) throws IOException {
                  counts.fromReader.incrementAndGet();
                  return super.getRandomVectorScorerSupplierForMerge(fieldInfo, segmentWriteState);
                }
              };
            }

            @Override
            public int getMaxDimensions(String fieldName) {
              return delegate.getMaxDimensions(fieldName);
            }
          };
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
      return new Lucene99HnswVectorsWriter(
          state,
          Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN,
          Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH,
          flatVectorsFormat,
          flatVectorsFormat.fieldsWriter(state),
          1,
          null,
          0);
    }
  }

  @Override
  protected boolean supportsFloatVectorFallback() {
    return false;
  }
}
