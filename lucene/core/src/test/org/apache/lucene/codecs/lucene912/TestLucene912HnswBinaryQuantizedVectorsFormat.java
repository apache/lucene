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
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.util.SameThreadExecutorService;

public class TestLucene912HnswBinaryQuantizedVectorsFormat extends BaseKnnVectorsFormatTestCase {

  @Override
  protected Codec getCodec() {
    return new Lucene912Codec() {
      @Override
      public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return new Lucene912HnswBinaryQuantizedVectorsFormat();
      }
    };
  }

  public void testToString() {
    FilterCodec customCodec =
        new FilterCodec("foo", Codec.getDefault()) {
          @Override
          public KnnVectorsFormat knnVectorsFormat() {
            return new Lucene912HnswBinaryQuantizedVectorsFormat(10, 20, 1, 90_000_000, null);
          }
        };
    String expectedPattern =
        "Lucene912HnswBinaryQuantizedVectorsFormat(name=Lucene912HnswBinaryQuantizedVectorsFormat, maxConn=10, beamWidth=20,"
            + " flatVectorFormat=Lucene912BinaryQuantizedVectorsFormat(name=Lucene912BinaryQuantizedVectorsFormat, numVectorsPerCluster=90000000,"
            + " flatVectorScorer=Lucene912BinaryFlatVectorsScorer(nonQuantizedDelegate=DefaultFlatVectorScorer())))";

    var defaultScorer = format(Locale.ROOT, expectedPattern, "DefaultFlatVectorScorer");
    var memSegScorer =
        format(Locale.ROOT, expectedPattern, "Lucene99MemorySegmentFlatVectorsScorer");
    assertThat(customCodec.knnVectorsFormat().toString(), is(oneOf(defaultScorer, memSegScorer)));
  }

  public void testSingleVectorCase() throws Exception {
    float[] vector = randomVector(random().nextInt(12, 500));
    for (VectorSimilarityFunction similarityFunction : VectorSimilarityFunction.values()) {
      try (Directory dir = newDirectory();
          IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("f", vector, similarityFunction));
        w.addDocument(doc);
        w.commit();
        try (IndexReader reader = DirectoryReader.open(w)) {
          LeafReader r = getOnlyLeafReader(reader);
          FloatVectorValues vectorValues = r.getFloatVectorValues("f");
          assert (vectorValues.size() == 1);
          while (vectorValues.nextDoc() != NO_MORE_DOCS) {
            assertArrayEquals(vector, vectorValues.vectorValue(), 0.00001f);
          }
          TopDocs td =
              r.searchNearestVectors("f", randomVector(vector.length), 1, null, Integer.MAX_VALUE);
          assertEquals(1, td.totalHits.value);
          assertTrue(td.scoreDocs[0].score >= 0);
        }
      }
    }
  }

  public void testLimits() {
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene912HnswBinaryQuantizedVectorsFormat(-1, 20));
    expectThrows(
        IllegalArgumentException.class, () -> new Lucene912HnswBinaryQuantizedVectorsFormat(0, 20));
    expectThrows(
        IllegalArgumentException.class, () -> new Lucene912HnswBinaryQuantizedVectorsFormat(20, 0));
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene912HnswBinaryQuantizedVectorsFormat(20, -1));
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene912HnswBinaryQuantizedVectorsFormat(512 + 1, 20));
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene912HnswBinaryQuantizedVectorsFormat(20, 3201));
    // TODO: uncomment this test we decide on the number of vectors in a cluster
    //    expectThrows(
    //        IllegalArgumentException.class,
    //        () -> new Lucene912HnswBinaryQuantizedVectorsFormat(20, 100, 0, 12, null));
    expectThrows(
        IllegalArgumentException.class,
        () ->
            new Lucene912HnswBinaryQuantizedVectorsFormat(
                20, 100, 1, 90_000_000, new SameThreadExecutorService()));
  }

  // Ensures that all expected vector similarity functions are translatable in the format.
  public void testVectorSimilarityFuncs() {
    // This does not necessarily have to be all similarity functions, but
    // differences should be considered carefully.
    var expectedValues = Arrays.stream(VectorSimilarityFunction.values()).toList();
    assertEquals(Lucene99HnswVectorsReader.SIMILARITY_FUNCTIONS, expectedValues);
  }

  public void testMergingWithMultipleCentroids() throws IOException {
    int numberOfVectorsPerCluster = 100;
    String fieldName = "field";
    int numVectors = random().nextInt(1200, 2000);
    int dims = random().nextInt(4, 65);
    IndexWriterConfig conf =
        newIndexWriterConfig()
            .setCodec(
                new Lucene912Codec() {
                  @Override
                  public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                    return new Lucene912HnswBinaryQuantizedVectorsFormat(
                        DEFAULT_MAX_CONN, DEFAULT_BEAM_WIDTH, 1, numberOfVectorsPerCluster, null);
                  }
                })
            .setMaxBufferedDocs(numVectors + 1)
            .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);

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
          if (i % 500 == 0) {
            w.commit();
          }
        }
        w.commit();
        w.forceMerge(random().nextInt(1, 3));

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
}
