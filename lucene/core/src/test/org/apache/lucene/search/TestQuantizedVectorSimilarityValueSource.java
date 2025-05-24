package org.apache.lucene.search;

import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.TestVectorUtil;
import org.junit.Before;
import org.junit.Test;

public class TestQuantizedVectorSimilarityValueSource extends LuceneTestCase {

  private Codec savedCodec;

  private static final String KNN_FIELD = "knnField";
  private static final int NUM_VECTORS = 1000;
  private static final int VECTOR_DIMENSION = 128;

  KnnVectorsFormat format;
  Float confidenceInterval;
  int bits;

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    bits = random().nextBoolean() ? 4 : 7;
    confidenceInterval = random().nextBoolean() ? random().nextFloat(0.90f, 1.0f) : null;
    if (random().nextBoolean()) {
      confidenceInterval = 0f;
    }
    format = getKnnFormat(bits);
    savedCodec = Codec.getDefault();
    Codec.setDefault(getCodec());
  }

  @Override
  public void tearDown() throws Exception {
    Codec.setDefault(savedCodec); // restore
    super.tearDown();
  }

  protected Codec getCodec() {
    return TestUtil.alwaysKnnVectorsFormat(format);
  }

  private final KnnVectorsFormat getKnnFormat(int bits) {
    return new Lucene99HnswScalarQuantizedVectorsFormat(
        Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN,
        Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH,
        1,
        bits,
        bits == 4 ? random().nextBoolean() : false,
        confidenceInterval,
        null);
  }

  @Test
  public void testFullPrecisionVectorSimilarityDVS() throws Exception {
    List<float[]> vectors = new ArrayList<>();
    int numVectors = atLeast(NUM_VECTORS);
    int numSegments = random().nextInt(2, 10);
    final VectorSimilarityFunction vectorSimilarityFunction =
        VectorSimilarityFunction.values()[
            random().nextInt(VectorSimilarityFunction.values().length)];

    try (Directory dir = newDirectory()) {
      int id = 0;

      // index some 4 bit quantized vectors
      try (IndexWriter w =
          new IndexWriter(
              dir,
              newIndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(getKnnFormat(4))))) {
        for (int j = 0; j < numSegments; j++) {
          for (int i = 0; i < numVectors; i++) {
            Document doc = new Document();
            if (random().nextInt(100) < 30) {
              // skip vector for some docs to create sparse vector field
              doc.add(new IntField("has_vector", 0, Field.Store.YES));
            } else {
              float[] vector = TestVectorUtil.randomVector(VECTOR_DIMENSION);
              vectors.add(vector);
              doc.add(new IntField("id", id++, Field.Store.YES));
              doc.add(new KnnFloatVectorField(KNN_FIELD, vector, vectorSimilarityFunction));
              doc.add(new IntField("has_vector", 1, Field.Store.YES));
            }
            w.addDocument(doc);
            w.flush();
          }
        }
        // add a segment with no vectors
        for (int i = 0; i < 100; i++) {
          Document doc = new Document();
          doc.add(new IntField("has_vector", 0, Field.Store.YES));
          w.addDocument(doc);
        }
        w.flush();
      }

      // index some 7 bit quantized vectors
      try (IndexWriter w =
          new IndexWriter(
              dir,
              newIndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(getKnnFormat(7))))) {
        for (int j = 0; j < numSegments; j++) {
          for (int i = 0; i < numVectors; i++) {
            Document doc = new Document();
            if (random().nextInt(100) < 30) {
              // skip vector for some docs to create sparse vector field
              doc.add(new IntField("has_vector", 0, Field.Store.YES));
            } else {
              float[] vector = TestVectorUtil.randomVector(VECTOR_DIMENSION);
              vectors.add(vector);
              doc.add(new IntField("id", id++, Field.Store.YES));
              doc.add(new KnnFloatVectorField(KNN_FIELD, vector, vectorSimilarityFunction));
              doc.add(new IntField("has_vector", 1, Field.Store.YES));
            }
            w.addDocument(doc);
            w.flush();
          }
        }
        // add a segment with no vectors
        for (int i = 0; i < 100; i++) {
          Document doc = new Document();
          doc.add(new IntField("has_vector", 0, Field.Store.YES));
          w.addDocument(doc);
        }
        w.flush();
      }

      float[] queryVector = TestVectorUtil.randomVector(VECTOR_DIMENSION);
      FloatVectorSimilarityValuesSource fpSimValueSource =
          new FloatVectorSimilarityValuesSource(queryVector, KNN_FIELD, true);
      FloatVectorSimilarityValuesSource quantizedSimValueSource =
          new FloatVectorSimilarityValuesSource(queryVector, KNN_FIELD);

      try (IndexReader reader = DirectoryReader.open(dir)) {
        FieldExistsQuery query = new FieldExistsQuery(KNN_FIELD);
        for (LeafReaderContext ctx : reader.leaves()) {
          DoubleValues fpSimValues = fpSimValueSource.getValues(ctx, null);
          DoubleValues quantizedSimValues = quantizedSimValueSource.getValues(ctx, null);
          // validate when segment has no vectors
          if (fpSimValues == DoubleValues.EMPTY || quantizedSimValues == DoubleValues.EMPTY) {
            assertEquals(fpSimValues, quantizedSimValues);
            assertNull(ctx.reader().getFloatVectorValues(KNN_FIELD));
            continue;
          }
          StoredFields storedFields = ctx.reader().storedFields();
          VectorScorer quantizedScorer =
              ctx.reader().getFloatVectorValues(KNN_FIELD).scorer(queryVector);
          DocIdSetIterator disi = quantizedScorer.iterator();
          while (disi.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            int doc = disi.docID();
            fpSimValues.advanceExact(doc);
            quantizedSimValues.advanceExact(doc);
            int idValue = Integer.parseInt(storedFields.document(doc).get("id"));
            float[] docVector = vectors.get(idValue);
            assert docVector != null : "Vector for id " + idValue + " not found";
            // validate full precision vector scores
            double expectedFpScore = vectorSimilarityFunction.compare(queryVector, docVector);
            double actualFpScore = fpSimValues.doubleValue();
            assertEquals(expectedFpScore, actualFpScore, 1e-5);
            // validate quantized vector scores
            double expectedQScore = quantizedScorer.score();
            double actualQScore = quantizedSimValues.doubleValue();
            assertEquals(expectedQScore, actualQScore, 1e-5);
          }
        }
      }
    }
  }
}
