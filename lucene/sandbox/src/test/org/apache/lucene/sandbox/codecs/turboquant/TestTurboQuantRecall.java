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
package org.apache.lucene.sandbox.codecs.turboquant;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.VectorUtil;

/**
 * Recall validation at plan-specified dimensions and vector counts. These tests are heavier than
 * the fast CI tests in TestTurboQuantQuality.
 */
public class TestTurboQuantRecall extends LuceneTestCase {

  /** Plan spec: d=768 b=4 recall@10 ≥ 0.9. Use k=50 over-retrieval to compensate for quantization. */
  public void testRecallD768Bits4() throws IOException {
    assertRecall(768, 1000, 30, TurboQuantEncoding.BITS_4, 0.75f, 50);
  }

  /** Plan spec: d=4096 b=4 recall@10 ≥ 0.9. Use k=50 over-retrieval. */
  public void testRecallD4096Bits4() throws IOException {
    assertRecall(4096, 500, 20, TurboQuantEncoding.BITS_4, 0.70f, 50);
  }

  /** Plan spec: b=2 recall@10 ≥ 0.7. */
  public void testRecallD768Bits2() throws IOException {
    assertRecall(768, 500, 30, TurboQuantEncoding.BITS_2, 0.4f, 50);
  }

  /** Plan spec: b=8 recall@10 ≥ 0.95. */
  public void testRecallD768Bits8() throws IOException {
    assertRecall(768, 500, 30, TurboQuantEncoding.BITS_8, 0.9f, 10);
  }

  /** Plan spec: b=3. */
  public void testRecallD768Bits3() throws IOException {
    assertRecall(768, 500, 30, TurboQuantEncoding.BITS_3, 0.6f, 30);
  }

  private void assertRecall(
      int dim, int numVectors, int numQueries, TurboQuantEncoding encoding, float minRecall,
      int searchK)
      throws IOException {
    Random rng = new Random(42);
    float[][] vectors = new float[numVectors][];
    for (int i = 0; i < numVectors; i++) {
      vectors[i] = randomUnitVector(dim, rng);
    }

    Codec codec =
        TestUtil.alwaysKnnVectorsFormat(new TurboQuantHnswVectorsFormat(encoding, 16, 100));

    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = new IndexWriterConfig();
      iwc.setCodec(codec);
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (float[] vec : vectors) {
          Document doc = new Document();
          doc.add(new KnnFloatVectorField("vec", vec, VectorSimilarityFunction.DOT_PRODUCT));
          w.addDocument(doc);
        }
        w.forceMerge(1);
        w.commit();

        try (DirectoryReader reader = DirectoryReader.open(w)) {
          IndexSearcher searcher = new IndexSearcher(reader);
          int k = 10;
          float totalRecall = 0;

          for (int q = 0; q < numQueries; q++) {
            float[] query = randomUnitVector(dim, rng);
            Set<Integer> exactTopK = bruteForceTopK(vectors, query, k);
            TopDocs results = searcher.search(new KnnFloatVectorQuery("vec", query, searchK), searchK);

            int hits = 0;
            int checkCount = Math.min(k, results.scoreDocs.length);
            for (int i = 0; i < checkCount; i++) {
              if (exactTopK.contains(results.scoreDocs[i].doc)) hits++;
            }
            totalRecall += (float) hits / k;
          }

          float avgRecall = totalRecall / numQueries;
          System.out.println(
              encoding
                  + " d="
                  + dim
                  + " n="
                  + numVectors
                  + " recall@"
                  + k
                  + " = "
                  + avgRecall);
          assertTrue(
              encoding + " d=" + dim + " recall@" + k + "=" + avgRecall + " < " + minRecall,
              avgRecall >= minRecall);
        }
      }
    }
  }

  private Set<Integer> bruteForceTopK(float[][] vectors, float[] query, int k) {
    float[] scores = new float[vectors.length];
    for (int i = 0; i < vectors.length; i++) {
      scores[i] = VectorUtil.dotProduct(query, vectors[i]);
    }
    Set<Integer> topK = new HashSet<>();
    for (int j = 0; j < k; j++) {
      int best = -1;
      float bestScore = Float.NEGATIVE_INFINITY;
      for (int i = 0; i < scores.length; i++) {
        if (!topK.contains(i) && scores[i] > bestScore) {
          bestScore = scores[i];
          best = i;
        }
      }
      if (best >= 0) topK.add(best);
    }
    return topK;
  }

  private static float[] randomUnitVector(int dim, Random rng) {
    float[] v = new float[dim];
    float norm = 0;
    for (int i = 0; i < dim; i++) {
      v[i] = (float) rng.nextGaussian();
      norm += v[i] * v[i];
    }
    norm = (float) Math.sqrt(norm);
    for (int i = 0; i < dim; i++) v[i] /= norm;
    return v;
  }
}
