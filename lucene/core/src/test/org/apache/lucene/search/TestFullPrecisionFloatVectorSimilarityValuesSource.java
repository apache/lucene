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

package org.apache.lucene.search;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat;
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

public class TestFullPrecisionFloatVectorSimilarityValuesSource extends LuceneTestCase {

  private Codec savedCodec;

  private static final String KNN_FIELD = "knnField";
  private static final int NUM_VECTORS = 100;
  private static final int VECTOR_DIMENSION = 8;

  KnnVectorsFormat format;
  int bits;

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    bits = random().nextBoolean() ? 4 : 8;
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

  private KnnVectorsFormat getKnnFormat(int bits) {
    return new Lucene104HnswScalarQuantizedVectorsFormat(
        Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding.fromNumBits(bits),
        Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN,
        Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH,
        1,
        null);
  }

  // TODO: incredibly slow
  @Nightly
  @Test
  public void testFullPrecisionVectorSimilarityDVS() throws Exception {
    List<float[]> vectors = new ArrayList<>();
    int numVectors = atLeast(NUM_VECTORS);
    int numSegments = random().nextInt(2, 10);
    final VectorSimilarityFunction indexingSimilarityFunction =
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
              doc.add(new KnnFloatVectorField(KNN_FIELD, vector, indexingSimilarityFunction));
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

      // index some 8 bit quantized vectors
      try (IndexWriter w =
          new IndexWriter(
              dir,
              newIndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(getKnnFormat(8))))) {
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
              doc.add(new KnnFloatVectorField(KNN_FIELD, vector, indexingSimilarityFunction));
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
      VectorSimilarityFunction rerankSimilarityFunction;
      try (IndexReader reader = DirectoryReader.open(dir)) {
        for (LeafReaderContext ctx : reader.leaves()) {
          DoubleValues fpSimValues;
          if (random().nextBoolean()) {
            rerankSimilarityFunction =
                VectorSimilarityFunction.values()[
                    random().nextInt(VectorSimilarityFunction.values().length)];
            fpSimValues =
                new FullPrecisionFloatVectorSimilarityValuesSource(
                        queryVector, KNN_FIELD, rerankSimilarityFunction)
                    .getSimilarityScores(ctx);
          } else {
            fpSimValues =
                new FullPrecisionFloatVectorSimilarityValuesSource(queryVector, KNN_FIELD)
                    .getSimilarityScores(ctx);
            rerankSimilarityFunction = indexingSimilarityFunction;
          }
          DoubleValues quantizedSimValues =
              DoubleValuesSource.similarityToQueryVector(ctx, queryVector, KNN_FIELD);
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
            int idValue =
                Integer.parseInt(Objects.requireNonNull(storedFields.document(doc).get("id")));
            float[] docVector = vectors.get(idValue);
            assert docVector != null : "Vector for id " + idValue + " not found";
            // validate full precision vector scores
            double expectedFpScore = rerankSimilarityFunction.compare(queryVector, docVector);
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
