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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LateInteractionField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.TestVectorUtil;
import org.apache.lucene.util.VectorUtil;

public class TestLateInteractionFloatValuesSource extends LuceneTestCase {

  private static final int DIMENSION = 16;
  private static final String LATE_I_FIELD = "lateIF";

  public void testValidations() {
    expectThrows(
        IllegalArgumentException.class,
        () -> new LateInteractionFloatValuesSource("fieldName", null, null));
    expectThrows(
        IllegalArgumentException.class,
        () -> new LateInteractionFloatValuesSource("fieldName", new float[0][], null));
    float[][] emptyTokens = new float[1][];
    emptyTokens[0] = new float[0];
    expectThrows(
        IllegalArgumentException.class,
        () -> new LateInteractionFloatValuesSource("fieldName", emptyTokens, null));

    float[][] valueBad = new float[random().nextInt(3, 12)][];
    for (int i = 0; i < valueBad.length; i++) {
      if (random().nextBoolean()) {
        valueBad[i] = TestVectorUtil.randomVector(DIMENSION);
      } else {
        valueBad[i] = TestVectorUtil.randomVector(DIMENSION + 1);
      }
    }
  }

  public void testValues() throws IOException {
    List<float[][]> corpus = new ArrayList<>();
    final int numDocs = atLeast(100);
    final int numSegments = random().nextInt(2, 5);
    final VectorSimilarityFunction vectorSimilarityFunction =
        VectorSimilarityFunction.values()[
            random().nextInt(VectorSimilarityFunction.values().length)];
    LateInteractionFloatValuesSource.ScoreFunction scoreFunction =
        LateInteractionFloatValuesSource.ScoreFunction.values()[
            random().nextInt(LateInteractionFloatValuesSource.ScoreFunction.values().length)];

    try (Directory dir = newDirectory()) {
      int id = 0;
      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        for (int j = 0; j < numSegments; j++) {
          for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            if (random().nextInt(100) < 30) {
              // skip value for some docs to create sparse field
              doc.add(new IntField("has_li_vector", 0, Field.Store.YES));
            } else {
              float[][] value = createMultiVector();
              corpus.add(value);
              doc.add(new IntField("id", id++, Field.Store.YES));
              doc.add(new LateInteractionField(LATE_I_FIELD, value));
              doc.add(new IntField("has_li_vector", 1, Field.Store.YES));
            }
            w.addDocument(doc);
            w.flush();
          }
        }
        // add a segment with no vectors
        for (int i = 0; i < 10; i++) {
          Document doc = new Document();
          doc.add(new IntField("has_li_vector", 0, Field.Store.YES));
          w.addDocument(doc);
        }
        w.flush();
      }

      float[][] queryVector = createMultiVector();
      LateInteractionFloatValuesSource source =
          new LateInteractionFloatValuesSource(
              LATE_I_FIELD, queryVector, vectorSimilarityFunction, scoreFunction);
      try (IndexReader reader = DirectoryReader.open(dir)) {
        for (LeafReaderContext ctx : reader.leaves()) {
          DoubleValues values = source.getValues(ctx, null);
          final FieldInfo fi = ctx.reader().getFieldInfos().fieldInfo(LATE_I_FIELD);
          if (fi == null) {
            assertEquals(values, DoubleValues.EMPTY);
            continue;
          }
          BinaryDocValues disi = ctx.reader().getBinaryDocValues(LATE_I_FIELD);
          StoredFields storedFields = ctx.reader().storedFields();
          while (disi.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            int doc = disi.docID();
            int idValue = Integer.parseInt(storedFields.document(doc).get("id"));
            float[][] docVector = corpus.get(idValue);
            float expected =
                scoreFunction.compare(queryVector, docVector, vectorSimilarityFunction);
            values.advanceExact(doc);
            assertEquals(expected, values.doubleValue(), 0.0001);
          }
        }
      }
    }
  }

  public void testSumMaxSim() {
    float[][] queryVector = createMultiVector();
    // docVector has all the token vectors in the query and some random token vectors
    float[][] docVector = new float[queryVector.length + 4][];
    for (int i = 0; i < queryVector.length; i++) {
      docVector[i] = new float[queryVector[i].length];
      System.arraycopy(queryVector[i], 0, docVector[i], 0, queryVector[i].length);
    }
    for (int i = queryVector.length; i < docVector.length; i++) {
      docVector[i] = TestVectorUtil.randomVector(queryVector[0].length);
    }
    float score =
        LateInteractionFloatValuesSource.ScoreFunction.SUM_MAX_SIM.compare(
            queryVector, docVector, VectorSimilarityFunction.COSINE);
    assertEquals(queryVector.length, score, 1e-5);
  }

  public void testSumMaxSimEuclideanFollowsMaxSimOrder() {
    // Best squared distances per query token: (0, 2) for docA, (0.9, 0.9) for docB. MaxSim prefers
    // docB (1.8 < 2.0), while summing per-token 1 / (1 + d^2) would prefer docA (1.333 > 1.053).
    float[][] queryVector = {{1, 0, 0}, {0, 1, 0}};
    float[][] docA = {{1, 0, 0}};
    float[][] docB = {{0.55f, 0.55f, (float) Math.sqrt(1 - 2 * 0.55 * 0.55)}};
    float scoreA = sumMaxSim(queryVector, docA, VectorSimilarityFunction.EUCLIDEAN);
    float scoreB = sumMaxSim(queryVector, docB, VectorSimilarityFunction.EUCLIDEAN);
    // n / (1 + sum / n)
    assertEquals(2 / (1 + 2f / 2), scoreA, 1e-5);
    assertEquals(2 / (1 + 1.8f / 2), scoreB, 1e-5);
    assertTrue(scoreB > scoreA);
  }

  public void testSumMaxSimMaxInnerProductFollowsMaxSimOrder() {
    // Best dot products per query token: (0.9, -0.6) for docA, (0.2, 0.2) for docB. MaxSim prefers
    // docB (0.4 > 0.3), while summing per-token scaled scores would prefer docA (2.525 > 2.4).
    float[][] queryVector = {{1, 0}, {0, 1}};
    float[][] docA = {{0.9f, -0.6f}};
    float[][] docB = {{0.2f, 0.2f}};
    float scoreA = sumMaxSim(queryVector, docA, VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT);
    float scoreB = sumMaxSim(queryVector, docB, VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT);
    // n * scaleMaxInnerProductScore(sum / n)
    assertEquals(2 * (1 + 0.3f / 2), scoreA, 1e-5);
    assertEquals(2 * (1 + 0.4f / 2), scoreB, 1e-5);
    assertTrue(scoreB > scoreA);

    // a negative MaxSim still yields a positive score
    float[][] docC = {{-0.5f, -0.3f}};
    assertEquals(
        2 / (1 + 0.8f / 2),
        sumMaxSim(queryVector, docC, VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT),
        1e-5);
  }

  public void testSumMaxSimSingleQueryTokenMatchesVectorSimilarity() {
    for (VectorSimilarityFunction function : VectorSimilarityFunction.values()) {
      float[] q = TestVectorUtil.randomVector(DIMENSION);
      float[][] docVector = createMultiVector();
      float expected = Float.NEGATIVE_INFINITY;
      for (float[] d : docVector) {
        expected = Math.max(expected, function.compare(q, d));
      }
      assertEquals(
          function.toString(), expected, sumMaxSim(new float[][] {q}, docVector, function), 1e-5);
    }
  }

  public void testSumMaxSimRanksByRawMaxSim() {
    for (VectorSimilarityFunction function : VectorSimilarityFunction.values()) {
      // signed components, so that per-token maxima can be negative; unit length for DOT_PRODUCT
      boolean normalize = function == VectorSimilarityFunction.DOT_PRODUCT;
      float[][] queryVector = createSignedMultiVector(normalize);
      for (int iter = 0; iter < 100; iter++) {
        float[][] docA = createSignedMultiVector(normalize);
        float[][] docB = createSignedMultiVector(normalize);
        double rawA = rawMaxSim(queryVector, docA, function);
        double rawB = rawMaxSim(queryVector, docB, function);
        if (Math.abs(rawA - rawB) < 1e-3) {
          continue;
        }
        float scoreA = sumMaxSim(queryVector, docA, function);
        float scoreB = sumMaxSim(queryVector, docB, function);
        assertEquals(
            function + " raw=" + rawA + "," + rawB + " score=" + scoreA + "," + scoreB,
            rawA > rawB,
            scoreA > scoreB);
      }
    }
  }

  private static float sumMaxSim(
      float[][] queryVector, float[][] docVector, VectorSimilarityFunction function) {
    return LateInteractionFloatValuesSource.ScoreFunction.SUM_MAX_SIM.compare(
        queryVector, docVector, function);
  }

  /** Sum over query tokens of the best raw similarity; squared distance is negated. */
  private static double rawMaxSim(
      float[][] queryVector, float[][] docVector, VectorSimilarityFunction function) {
    double sum = 0;
    for (float[] q : queryVector) {
      double best = Double.NEGATIVE_INFINITY;
      for (float[] d : docVector) {
        double raw =
            switch (function) {
              case EUCLIDEAN -> -VectorUtil.squareDistance(q, d);
              case DOT_PRODUCT, MAXIMUM_INNER_PRODUCT -> VectorUtil.dotProduct(q, d);
              case COSINE -> VectorUtil.cosine(q, d);
            };
        best = Math.max(best, raw);
      }
      sum += best;
    }
    return sum;
  }

  private float[][] createSignedMultiVector(boolean normalize) {
    float[][] value = new float[random().nextInt(2, 5)][];
    for (int i = 0; i < value.length; i++) {
      value[i] = new float[DIMENSION];
      for (int j = 0; j < DIMENSION; j++) {
        value[i][j] = random().nextFloat() * 2 - 1;
      }
      if (normalize) {
        VectorUtil.l2normalize(value[i]);
      }
    }
    return value;
  }

  private float[][] createMultiVector() {
    float[][] value = new float[random().nextInt(3, 5)][];
    for (int i = 0; i < value.length; i++) {
      value[i] = TestVectorUtil.randomVector(DIMENSION);
    }
    return value;
  }
}
