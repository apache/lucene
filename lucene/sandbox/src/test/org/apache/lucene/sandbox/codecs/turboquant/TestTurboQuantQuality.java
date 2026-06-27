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
import java.util.Set;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.VectorUtil;

/** Phase 4: Comprehensive quality validation tests for TurboQuant. */
public class TestTurboQuantQuality extends LuceneTestCase {

  private Codec getCodec(TurboQuantEncoding encoding) {
    return TestUtil.alwaysKnnVectorsFormat(
        new TurboQuantHnswVectorsFormat(encoding, 16, 100));
  }

  /** 4.1: Recall validation at d=128 b=4 (smaller dim for fast CI). */

  public void testRecallBits4() throws IOException {
    doRecallTest(128, 500, TurboQuantEncoding.BITS_4, 0.8f);
  }

  /** 4.1: Recall at d=768 b=4 per plan spec. */

  public void testRecallD768Bits4() throws IOException {
    doRecallTest(768, 200, TurboQuantEncoding.BITS_4, 0.8f);
  }

  /** 4.1: Recall at b=8 should be very high. */

  public void testRecallBits8() throws IOException {
    doRecallTest(64, 200, TurboQuantEncoding.BITS_8, 0.9f);
  }

  /** 4.1: Recall at b=2 should be reasonable. */

  public void testRecallBits2() throws IOException {
    doRecallTest(64, 200, TurboQuantEncoding.BITS_2, 0.5f);
  }

  /** 4.1: Randomized dimension. */

  public void testRecallRandomDim() throws IOException {
    int d = random().nextInt(32, 257);
    doRecallTest(d, 200, TurboQuantEncoding.BITS_4, 0.6f);
  }

  /** 4.3: Empty segment — index, search succeeds. */
  public void testEmptySegment() throws IOException {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = new IndexWriterConfig();
      iwc.setCodec(getCodec(TurboQuantEncoding.BITS_4));
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        w.commit();
        try (DirectoryReader reader = DirectoryReader.open(w)) {
          IndexSearcher searcher = new IndexSearcher(reader);
          float[] query = new float[] {1, 0, 0, 0};
          TopDocs results =
              searcher.search(new KnnFloatVectorQuery("vec", query, 5), 5);
          assertEquals(0, results.totalHits.value());
        }
      }
    }
  }

  /** 4.3: Single vector segment. */
  public void testSingleVector() throws IOException {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = new IndexWriterConfig();
      iwc.setCodec(getCodec(TurboQuantEncoding.BITS_4));
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("vec", new float[] {1, 0, 0, 0},
            VectorSimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
        w.commit();
        try (DirectoryReader reader = DirectoryReader.open(w)) {
          IndexSearcher searcher = new IndexSearcher(reader);
          TopDocs results =
              searcher.search(
                  new KnnFloatVectorQuery("vec", new float[] {1, 0, 0, 0}, 1), 1);
          assertEquals(1, results.totalHits.value());
        }
      }
    }
  }

  /** 4.4: Merge with deleted docs. */
  public void testMergeWithDeletedDocs() throws IOException {
    int dim = 32;
    int numVectors = 50;
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = new IndexWriterConfig();
      iwc.setCodec(getCodec(TurboQuantEncoding.BITS_4));
      java.util.Random rng = new java.util.Random(42);

      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < numVectors; i++) {
          Document doc = new Document();
          doc.add(new KnnFloatVectorField("vec", randomUnitVector(dim, rng),
              VectorSimilarityFunction.DOT_PRODUCT));
          doc.add(new org.apache.lucene.document.StringField(
              "id", String.valueOf(i), org.apache.lucene.document.Field.Store.YES));
          w.addDocument(doc);
        }
        w.commit();

        // Delete half the docs
        for (int i = 0; i < numVectors; i += 2) {
          w.deleteDocuments(new Term("id", String.valueOf(i)));
        }
        w.forceMerge(1);
        w.commit();

        try (DirectoryReader reader = DirectoryReader.open(w)) {
          IndexSearcher searcher = new IndexSearcher(reader);
          float[] query = randomUnitVector(dim, rng);
          TopDocs results =
              searcher.search(new KnnFloatVectorQuery("vec", query, 10), 10);
          // Should only find live docs
          assertTrue(results.totalHits.value() > 0);
          assertTrue(results.totalHits.value() <= numVectors / 2);
        }
      }
    }
  }

  /** 4.4: Force merge from multiple segments. */
  public void testForceMergeMultipleSegments() throws IOException {
    int dim = 32;
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = new IndexWriterConfig();
      iwc.setCodec(getCodec(TurboQuantEncoding.BITS_4));
      java.util.Random rng = new java.util.Random(42);

      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        // Create 3 segments
        for (int seg = 0; seg < 3; seg++) {
          for (int i = 0; i < 20; i++) {
            Document doc = new Document();
            doc.add(new KnnFloatVectorField("vec", randomUnitVector(dim, rng),
                VectorSimilarityFunction.DOT_PRODUCT));
            w.addDocument(doc);
          }
          w.commit();
        }

        w.forceMerge(1);
        w.commit();

        try (DirectoryReader reader = DirectoryReader.open(w)) {
          assertEquals(1, reader.leaves().size());
          IndexSearcher searcher = new IndexSearcher(reader);
          float[] query = randomUnitVector(dim, rng);
          TopDocs results =
              searcher.search(new KnnFloatVectorQuery("vec", query, 10), 10);
          assertTrue(results.totalHits.value() > 0);
        }
      }
    }
  }

  /** 4.4: 10 segments → force merge to 1. */
  public void testForceMerge10Segments() throws IOException {
    int dim = 32;
    int totalVectors = 0;
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = new IndexWriterConfig();
      iwc.setCodec(getCodec(TurboQuantEncoding.BITS_4));
      java.util.Random rng = new java.util.Random(99);

      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int seg = 0; seg < 10; seg++) {
          for (int i = 0; i < 10; i++) {
            Document doc = new Document();
            doc.add(new KnnFloatVectorField("vec", randomUnitVector(dim, rng),
                VectorSimilarityFunction.DOT_PRODUCT));
            w.addDocument(doc);
            totalVectors++;
          }
          w.commit();
        }

        w.forceMerge(1);
        w.commit();

        try (DirectoryReader reader = DirectoryReader.open(w)) {
          assertEquals(1, reader.leaves().size());
          IndexSearcher searcher = new IndexSearcher(reader);
          float[] query = randomUnitVector(dim, rng);
          TopDocs results =
              searcher.search(new KnnFloatVectorQuery("vec", query, totalVectors), totalVectors);
          assertEquals(totalVectors, results.totalHits.value());
        }
      }
    }
  }

  /** 4.2: All similarity functions produce valid scores. */
  public void testAllSimilarityFunctions() throws IOException {
    int dim = 32;
    int numVectors = 20;
    java.util.Random rng = new java.util.Random(42);

    for (VectorSimilarityFunction sim : VectorSimilarityFunction.values()) {
      for (TurboQuantEncoding enc : TurboQuantEncoding.values()) {
        try (Directory dir = newDirectory()) {
          IndexWriterConfig iwc = new IndexWriterConfig();
          iwc.setCodec(getCodec(enc));
          try (IndexWriter w = new IndexWriter(dir, iwc)) {
            for (int i = 0; i < numVectors; i++) {
              Document doc = new Document();
              float[] vec = randomUnitVector(dim, rng);
              doc.add(new KnnFloatVectorField("vec", vec, sim));
              w.addDocument(doc);
            }
            w.commit();
            try (DirectoryReader reader = DirectoryReader.open(w)) {
              IndexSearcher searcher = new IndexSearcher(reader);
              float[] query = randomUnitVector(dim, rng);
              TopDocs results =
                  searcher.search(new KnnFloatVectorQuery("vec", query, 5), 5);
              assertTrue(sim + "/" + enc + ": expected results", results.totalHits.value() > 0);
              for (var sd : results.scoreDocs) {
                assertFalse(sim + "/" + enc + ": NaN score", Float.isNaN(sd.score));
                assertTrue(sim + "/" + enc + ": negative score", sd.score >= 0);
              }
            }
          }
        }
      }
    }
  }

  private void doRecallTest(
      int dim, int numVectors, TurboQuantEncoding encoding, float minRecall) throws IOException {
    java.util.Random rng = new java.util.Random(42);
    float[][] vectors = new float[numVectors][];
    for (int i = 0; i < numVectors; i++) {
      vectors[i] = randomUnitVector(dim, rng);
    }

    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = new IndexWriterConfig();
      iwc.setCodec(getCodec(encoding));
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (float[] vec : vectors) {
          Document doc = new Document();
          doc.add(new KnnFloatVectorField("vec", vec, VectorSimilarityFunction.DOT_PRODUCT));
          w.addDocument(doc);
        }
        w.commit();

        try (DirectoryReader reader = DirectoryReader.open(w)) {
          IndexSearcher searcher = new IndexSearcher(reader);
          int k = 10;
          int numQueries = 50;
          float totalRecall = 0;

          for (int q = 0; q < numQueries; q++) {
            float[] query = randomUnitVector(dim, rng);

            // Brute-force exact top-k
            Set<Integer> exactTopK = bruteForceTopK(vectors, query, k);

            // TurboQuant search
            TopDocs results =
                searcher.search(new KnnFloatVectorQuery("vec", query, k), k);
            Set<Integer> approxTopK = new HashSet<>();
            for (var sd : results.scoreDocs) {
              approxTopK.add(sd.doc);
            }

            // Compute recall
            int hits = 0;
            for (int doc : approxTopK) {
              if (exactTopK.contains(doc)) hits++;
            }
            totalRecall += (float) hits / k;
          }

          float avgRecall = totalRecall / numQueries;
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
    // Find top-k by score
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

  private static float[] randomUnitVector(int dim, java.util.Random rng) {
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
