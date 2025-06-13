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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestRescoreTopNQuery extends LuceneTestCase {

  private static final String FIELD = "vector";
  private static final String RESCORE_FIELD = "vector-rescore";
  private static final VectorSimilarityFunction VECTOR_SIMILARITY_FUNCTION =
      VectorSimilarityFunction.COSINE;
  private static final int NUM_VECTORS = 1000;
  private static final int VECTOR_DIMENSION = 128;

  private Directory directory;
  private IndexWriterConfig config;

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = new ByteBuffersDirectory();

    // Set up the IndexWriterConfig to use quantized vector storage
    config = new IndexWriterConfig();
    config.setCodec(TestUtil.alwaysKnnVectorsFormat(new Lucene99HnswVectorsFormat()));
  }

  @Test
  public void testTwoPhaseKnnVectorQuery() throws Exception {
    Map<Integer, float[]> vectors = new HashMap<>();

    Random random = random();

    int numVectors = atLeast(NUM_VECTORS);
    int numSegments = random.nextInt(2, 10);

    // Step 1: Index random vectors in quantized format
    try (IndexWriter writer = new IndexWriter(directory, config)) {
      for (int j = 0; j < numSegments; j++) {
        for (int i = 0; i < numVectors; i++) {
          float[] vector = randomFloatVector(VECTOR_DIMENSION, random);
          float[] rescoreVector = randomFloatVector(VECTOR_DIMENSION, random);
          Document doc = new Document();
          int id = j * numVectors + i;
          doc.add(new IntField("id", id, Field.Store.YES));
          doc.add(new KnnFloatVectorField(FIELD, vector, VECTOR_SIMILARITY_FUNCTION));
          doc.add(
              new KnnFloatVectorField(RESCORE_FIELD, rescoreVector, VECTOR_SIMILARITY_FUNCTION));
          writer.addDocument(doc);
          vectors.put(id, rescoreVector);

          writer.flush();
        }
      }
    }

    // Step 2: Run TwoPhaseKnnVectorQuery with a random target vector
    try (IndexReader reader = DirectoryReader.open(directory)) {
      IndexSearcher searcher = new IndexSearcher(reader);
      float[] targetVector = randomFloatVector(VECTOR_DIMENSION, random);
      int k = 10;
      double oversample = random.nextFloat(1.5f, 3.0f);

      FloatVectorSimilarityValuesSource valueSource =
          new FloatVectorSimilarityValuesSource(targetVector, RESCORE_FIELD);

      KnnFloatVectorQuery knnQuery =
          new KnnFloatVectorQuery(FIELD, targetVector, k + (int) (k * oversample));
      RescoreTopNQuery query = new RescoreTopNQuery(knnQuery, valueSource, k);
      TopDocs topDocs = searcher.search(query, k);

      // Step 3: Verify that TopDocs scores match similarity with unquantized vectors
      for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
        Document retrievedDoc = searcher.storedFields().document(scoreDoc.doc);
        int id = retrievedDoc.getField("id").numericValue().intValue();
        float[] docVector = vectors.get(id);
        assert docVector != null : "Vector for id " + id + " not found";
        float expectedScore = VECTOR_SIMILARITY_FUNCTION.compare(targetVector, docVector);
        Assert.assertEquals(
            "Score does not match expected similarity for doc ord: " + scoreDoc.doc + ", id: " + id,
            expectedScore,
            scoreDoc.score,
            1e-5);
      }
    }
  }

  private float[] randomFloatVector(int dimension, Random random) {
    float[] vector = new float[dimension];
    for (int i = 0; i < dimension; i++) {
      vector[i] = random.nextFloat();
    }
    return vector;
  }
}
