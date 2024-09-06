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
package org.apache.lucene.codecs.lucene912;

import static java.lang.String.format;
import static org.apache.lucene.codecs.lucene912.Lucene912BinaryQuantizedVectorsFormat.NAME;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

import java.io.IOException;
import java.util.Locale;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.quantization.BQVectorUtils;
import org.apache.lucene.util.quantization.BinaryQuantizer;

public class TestLucene912BinaryQuantizedVectorsFormat extends BaseKnnVectorsFormatTestCase {

  @Override
  protected Codec getCodec() {
    return new Lucene912Codec() {
      @Override
      public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return new Lucene912BinaryQuantizedVectorsFormat();
      }
    };
  }

  public void testSearch() throws Exception {
    String fieldName = "field";
    int numVectors = random().nextInt(99, 500);
    int dims = random().nextInt(4, 65);
    float[] vector = randomVector(dims);
    VectorSimilarityFunction similarityFunction = randomSimilarity();
    KnnFloatVectorField knnField = new KnnFloatVectorField(fieldName, vector, similarityFunction);
    IndexWriterConfig iwc = newIndexWriterConfig();
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < numVectors; i++) {
          Document doc = new Document();
          knnField.setVectorValue(randomVector(dims));
          doc.add(knnField);
          w.addDocument(doc);
        }
        w.commit();

        try (IndexReader reader = DirectoryReader.open(w)) {
          IndexSearcher searcher = new IndexSearcher(reader);
          final int k = random().nextInt(5, 50);
          float[] queryVector = randomVector(dims);
          Query q = new KnnFloatVectorQuery(fieldName, queryVector, k);
          TopDocs collectedDocs = searcher.search(q, k);
          assertEquals(k, collectedDocs.totalHits.value);
          assertEquals(TotalHits.Relation.EQUAL_TO, collectedDocs.totalHits.relation);
        }
      }
    }
  }

  public void testToString() {
    FilterCodec customCodec =
        new FilterCodec("foo", Codec.getDefault()) {
          @Override
          public KnnVectorsFormat knnVectorsFormat() {
            return new Lucene912BinaryQuantizedVectorsFormat(90_000_000);
          }
        };
    String expectedPattern =
        "Lucene912BinaryQuantizedVectorsFormat(name=Lucene912BinaryQuantizedVectorsFormat, numVectorsPerCluster=90000000, flatVectorScorer=Lucene912BinaryFlatVectorsScorer(nonQuantizedDelegate=DefaultFlatVectorScorer()))";
    var defaultScorer = format(Locale.ROOT, expectedPattern, "DefaultFlatVectorScorer");
    var memSegScorer =
        format(Locale.ROOT, expectedPattern, "Lucene99MemorySegmentFlatVectorsScorer");
    assertThat(customCodec.knnVectorsFormat().toString(), is(oneOf(defaultScorer, memSegScorer)));
  }

  public void testLimits() {
    // TODO: uncomment this test we decide on the number of vectors in a cluster
    //    expectThrows(
    //        IllegalArgumentException.class, () -> new Lucene912BinaryQuantizedVectorsFormat(12));
  }

  @Override
  public void testRandomWithUpdatesAndGraph() {
    // graph not supported
  }

  @Override
  public void testSearchWithVisitedLimit() {
    // visited limit is not respected, as it is brute force search
  }

  public void testQuantizedVectorsWriteAndRead() throws IOException {
    int numberOfVectorsPerCluster = 50;
    String fieldName = "field";
    int numVectors = random().nextInt(99, 500);
    int dims = random().nextInt(4, 65);
    IndexWriterConfig conf =
        newIndexWriterConfig()
            .setCodec(
                new Lucene912Codec() {
                  @Override
                  public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                    return new Lucene912BinaryQuantizedVectorsFormat(
                        NAME, numberOfVectorsPerCluster);
                  }
                })
            .setMaxBufferedDocs(numVectors + 1)
            .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH)
            .setMergePolicy(NoMergePolicy.INSTANCE);

    float[] vector = randomVector(dims);
    VectorSimilarityFunction similarityFunction = randomSimilarity();
    KnnFloatVectorField knnField = new KnnFloatVectorField(fieldName, vector, similarityFunction);
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, conf)) {
        for (int i = 0; i < numVectors; i++) {
          Document doc = new Document();
          knnField.setVectorValue(randomVector(dims));
          doc.add(knnField);
          w.addDocument(doc);
        }
        w.commit();

        try (IndexReader reader = DirectoryReader.open(w)) {
          LeafReader r = getOnlyLeafReader(reader);
          FloatVectorValues vectorValues = r.getFloatVectorValues(fieldName);
          assertEquals(vectorValues.size(), numVectors);
          OffHeapBinarizedVectorValues qvectorValues =
              ((Lucene912BinaryQuantizedVectorsReader.BinarizedVectorValues) vectorValues)
                  .getQuantizedVectorValues();
          float[][] centroids = qvectorValues.getCentroids();
          assertEquals(centroids[0].length, dims);
          int expectedNumCentroids = Math.max(1, numVectors / numberOfVectorsPerCluster);
          int maxNumClusters = Math.max(1, numVectors / 100);
          assertEquals(centroids.length, Math.min(expectedNumCentroids, maxNumClusters));

          int descritizedDimension = BQVectorUtils.discretize(dims, 64);
          BinaryQuantizer quantizer = new BinaryQuantizer(descritizedDimension, similarityFunction);
          byte[] expectedVector = new byte[BQVectorUtils.discretize(dims, 64) / 8];

          int correctCentroidAssigns = 0;
          while (vectorValues.nextDoc() != NO_MORE_DOCS) {
            int nearestCentroid = nearestCentroid(vectorValues.vectorValue(), centroids);
            if (nearestCentroid == qvectorValues.clusterId()) {
              correctCentroidAssigns++;
            }
            float[] centroid = centroids[qvectorValues.clusterId()];
            float[] corrections =
                quantizer.quantizeForIndex(vectorValues.vectorValue(), expectedVector, centroid);
            assertArrayEquals(expectedVector, qvectorValues.vectorValue());
            assertEquals(corrections.length, qvectorValues.getCorrectiveTerms().length);
            for (int i = 0; i < corrections.length; i++) {
              assertEquals(corrections[i], qvectorValues.getCorrectiveTerms()[i], 0.00001f);
            }
          }
          // Because we recalculate centroids as the last step,
          // it could happen that some vectors have slightly wrong centroids assigned
          if (correctCentroidAssigns < numVectors * 0.9) {
            fail("Centroids were not assigned correctly");
          }
        }
      }
    }
  }

  static int nearestCentroid(float[] vector, float[][] centroids) {
    int nearest = 0;
    float minDistance = Float.POSITIVE_INFINITY;
    for (int i = 0; i < centroids.length; i++) {
      float distance = VectorUtil.squareDistance(vector, centroids[i]);
      if (distance < minDistance) {
        minDistance = distance;
        nearest = i;
      }
    }
    return nearest;
  }
}
