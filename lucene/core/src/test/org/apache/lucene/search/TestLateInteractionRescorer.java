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

import static org.apache.lucene.search.LateInteractionFloatValuesSource.ScoreFunction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.LateInteractionField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestLateInteractionRescorer extends LuceneTestCase {

  private final String LATE_I_FIELD = "li_vector";
  private final String KNN_FIELD = "knn_vector";
  private final int DIMENSION = 16;

  public void testBasic() throws Exception {
    List<float[][]> corpus = new ArrayList<>();
    final VectorSimilarityFunction vectorSimilarityFunction =
        VectorSimilarityFunction.values()[
            random().nextInt(VectorSimilarityFunction.values().length)];
    ScoreFunction scoreFunction = ScoreFunction.SUM_MAX_SIM;

    try (Directory dir = newDirectory()) {
      indexMultiVectors(dir, corpus);
      float[][] lateIQueryVector = createMultiVector(DIMENSION);
      float[] knnQueryVector = randomFloatVector(DIMENSION, random());
      KnnFloatVectorQuery knnQuery = new KnnFloatVectorQuery(KNN_FIELD, knnQueryVector, 50);

      try (IndexReader reader = DirectoryReader.open(dir)) {
        final int topN = 10;
        IndexSearcher s = new IndexSearcher(reader);
        TopDocs knnHits = s.search(knnQuery, 5 * topN);
        LateInteractionRescorer rescorer =
            LateInteractionRescorer.create(
                LATE_I_FIELD, lateIQueryVector, vectorSimilarityFunction);
        TopDocs rerankedHits = rescorer.rescore(s, knnHits, topN);
        Set<Integer> knnHitDocs =
            Arrays.stream(knnHits.scoreDocs).map(k -> k.doc).collect(Collectors.toSet());
        assertEquals(topN, rerankedHits.scoreDocs.length);
        StoredFields storedFields = reader.storedFields();
        for (int i = 0; i < rerankedHits.scoreDocs.length; i++) {
          assertTrue(knnHitDocs.contains(rerankedHits.scoreDocs[i].doc));
          int idValue =
              Integer.parseInt(storedFields.document(rerankedHits.scoreDocs[i].doc).get("id"));
          float[][] docVector = corpus.get(idValue);
          float expected =
              scoreFunction.compare(lateIQueryVector, docVector, vectorSimilarityFunction);
          assertEquals(expected, rerankedHits.scoreDocs[i].score, 1e-4f);
          if (i > 0) {
            assertTrue(rerankedHits.scoreDocs[i].score <= rerankedHits.scoreDocs[i - 1].score);
          }
        }
      }
    }
  }

  public void testMissingLateIValues() throws Exception {
    List<float[][]> corpus = new ArrayList<>();
    final VectorSimilarityFunction vectorSimilarityFunction =
        VectorSimilarityFunction.values()[
            random().nextInt(VectorSimilarityFunction.values().length)];

    try (Directory dir = newDirectory()) {
      indexMultiVectors(dir, corpus);
      float[][] lateIQueryVector = createMultiVector(DIMENSION);
      float[] knnQueryVector = randomFloatVector(DIMENSION, random());
      KnnFloatVectorQuery knnQuery = new KnnFloatVectorQuery(KNN_FIELD, knnQueryVector, 50);

      try (IndexReader reader = DirectoryReader.open(dir)) {
        final int topN = 10;
        IndexSearcher s = new IndexSearcher(reader);
        TopDocs knnHits = s.search(knnQuery, 5 * topN);
        LateInteractionRescorer rescorer =
            LateInteractionRescorer.create(
                "bad-test-field", lateIQueryVector, vectorSimilarityFunction);
        TopDocs rerankedHits = rescorer.rescore(s, knnHits, topN);
        Set<Integer> knnHitDocs =
            Arrays.stream(knnHits.scoreDocs).map(k -> k.doc).collect(Collectors.toSet());
        assertEquals(topN, rerankedHits.scoreDocs.length);
        for (int i = 0; i < rerankedHits.scoreDocs.length; i++) {
          assertTrue(knnHitDocs.contains(rerankedHits.scoreDocs[i].doc));
          assertEquals(0f, rerankedHits.scoreDocs[i].score, 1e-5);
        }

        LateInteractionRescorer rescorerWithFallback =
            LateInteractionRescorer.withFallbackToFirstPassScore(
                "bad-test-field", lateIQueryVector, vectorSimilarityFunction);
        knnHits = s.search(knnQuery, 5 * topN);
        rerankedHits = rescorerWithFallback.rescore(s, knnHits, topN);
        knnHitDocs = Arrays.stream(knnHits.scoreDocs).map(k -> k.doc).collect(Collectors.toSet());
        assertEquals(topN, rerankedHits.scoreDocs.length);
        for (int i = 0; i < rerankedHits.scoreDocs.length; i++) {
          assertTrue(knnHitDocs.contains(rerankedHits.scoreDocs[i].doc));
          assertEquals(knnHits.scoreDocs[i].score, rerankedHits.scoreDocs[i].score, 1e-5);
        }
      }
    }
  }

  private void indexMultiVectors(Directory dir, List<float[][]> corpus) throws IOException {
    final int numDocs = atLeast(1000);
    final int numSegments = random().nextInt(2, 5);
    int id = 0;
    try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      for (int j = 0; j < numSegments; j++) {
        for (int i = 0; i < numDocs; i++) {
          Document doc = new Document();
          if (random().nextInt(100) < 30) {
            // skip value for some docs to create sparse field
            doc.add(new IntField("has_li_vector", 0, Field.Store.YES));
          } else {
            float[][] value = createMultiVector(DIMENSION);
            corpus.add(value);
            doc.add(new IntField("id", id++, Field.Store.YES));
            doc.add(new LateInteractionField(LATE_I_FIELD, value));
            doc.add(new KnnFloatVectorField(KNN_FIELD, randomFloatVector(DIMENSION, random())));
            doc.add(new IntField("has_li_vector", 1, Field.Store.YES));
          }
          w.addDocument(doc);
          w.flush();
        }
      }
      // add a segment with no vectors
      for (int i = 0; i < 100; i++) {
        Document doc = new Document();
        doc.add(new IntField("has_li_vector", 0, Field.Store.YES));
        w.addDocument(doc);
      }
      w.flush();
    }
  }

  private float[][] createMultiVector(int dimension) {
    float[][] value = new float[random().nextInt(2, 5)][];
    for (int i = 0; i < value.length; i++) {
      value[i] = randomFloatVector(dimension, random());
    }
    return value;
  }

  private float[] randomFloatVector(int dimension, Random random) {
    float[] vector = new float[dimension];
    for (int i = 0; i < dimension; i++) {
      vector[i] = random.nextFloat();
    }
    return vector;
  }
}
