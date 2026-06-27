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
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
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

/** Targeted tests for TurboQuant at high dimensions. */
public class TestTurboQuantHighDim extends LuceneTestCase {

  private Codec getCodec(TurboQuantEncoding encoding) {
    return org.apache.lucene.tests.util.TestUtil.alwaysKnnVectorsFormat(
        new TurboQuantHnswVectorsFormat(encoding, 16, 100));
  }

  public void testIndexAndSearchD768() throws IOException {
    doTestIndexAndSearch(768, 50, TurboQuantEncoding.BITS_4);
  }

  public void testIndexAndSearchD4096() throws IOException {
    doTestIndexAndSearch(4096, 20, TurboQuantEncoding.BITS_4);
  }

  private void doTestIndexAndSearch(int dim, int numVectors, TurboQuantEncoding encoding)
      throws IOException {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = new IndexWriterConfig();
      iwc.setCodec(getCodec(encoding));
      java.util.Random rng = new java.util.Random(42);

      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < numVectors; i++) {
          Document doc = new Document();
          float[] vec = randomUnitVector(dim, rng);
          doc.add(new KnnFloatVectorField("vec", vec, VectorSimilarityFunction.DOT_PRODUCT));
          w.addDocument(doc);
        }
        w.commit();

        try (DirectoryReader reader = DirectoryReader.open(w)) {
          IndexSearcher searcher = new IndexSearcher(reader);
          float[] query = randomUnitVector(dim, rng);
          TopDocs results =
              searcher.search(new KnnFloatVectorQuery("vec", query, 5), 5);
          assertTrue(
              "Expected results at d=" + dim + ", got " + results.totalHits.value(),
              results.totalHits.value() > 0);
          // Verify scores are valid
          for (var sd : results.scoreDocs) {
            assertTrue("Score should be non-negative", sd.score >= 0);
            assertFalse("Score should not be NaN", Float.isNaN(sd.score));
          }
        }
      }
    }
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
