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
package org.apache.lucene.codecs.spann;

import java.util.Random;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestSpannAssignment extends LuceneTestCase {

  public void testAssignmentRecall() throws Exception {
    // This test verifies that the HNSW assignment logic (Phase 2)
    // correctly assigns vectors to their closest centroids.
    // If assignment is correct, searching for a vector with nprobe=1
    // should find the vector itself in the top results.

    int numDocs = atLeast(1000);
    int dim = 16;
    int numPartitions = 20; // Force enough partitions to make assignment non-trivial
    int nProbe = 1; // Strict check: must be in the BEST partition

    try (Directory dir = newDirectory()) {
      Codec codec =
          new Lucene104Codec() {
            @Override
            public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
              // nProbe=1, numPartitions=20, sample=256, replication=1
              return new Lucene99SpannVectorsFormat(nProbe, numPartitions, 256, 1);
            }
          };

      IndexWriterConfig iwc = newIndexWriterConfig().setCodec(codec).setUseCompoundFile(false);

      float[][] vectors = new float[numDocs][dim];
      Random random = random();

      try (IndexWriter writer = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < numDocs; i++) {
          float[] v = new float[dim];
          for (int d = 0; d < dim; d++) {
            v[d] = random.nextFloat();
          }
          // Normalize to unit length for COSINE/DOT_PRODUCT similarity consistency
          float norm = 0;
          for (float f : v) norm += f * f;
          norm = (float) Math.sqrt(norm);
          for (int d = 0; d < dim; d++) v[d] /= norm;

          vectors[i] = v;
          Document doc = new Document();
          doc.add(new KnnFloatVectorField("vec", v, VectorSimilarityFunction.COSINE));
          writer.addDocument(doc);
        }
        writer.forceMerge(1); // Ensure we test the flushed segment
        writer.commit();
      }

      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        int recallCount = 0;

        // Randomly sample docs to test
        int numChecks = Math.min(100, numDocs);
        for (int i = 0; i < numChecks; i++) {
          int docId = random.nextInt(numDocs);
          float[] query = vectors[docId];

          TopDocs results = searcher.search(new KnnFloatVectorQuery("vec", query, 10), 10);

          boolean found = false;
          for (ScoreDoc sd : results.scoreDocs) {
            // Strict check: The found docID should match the queried docID (since we query with the
            // vector itself)
            // In this static index, docID maps 1:1, but let's be robust and check if the matched
            // docID is correct.
            if (sd.doc == docId) {
              found = true;
              break;
            }
          }
          if (found) {
            recallCount++;
          }
        }

        // We expect very high recall for self-search even with nprobe=1
        // because the vector *should* be assigned to the centroid it is closest to.
        double recall = (double) recallCount / numChecks;
        assertTrue("Recall " + recall + " should be > 0.9", recall > 0.9);
      }
    }
  }
}
