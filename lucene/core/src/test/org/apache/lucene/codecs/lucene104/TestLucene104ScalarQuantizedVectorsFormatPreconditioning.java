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
package org.apache.lucene.codecs.lucene104;

import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues.ScalarEncoding;

/**
 * Tests for the always-on rotation preconditioning in {@link
 * Lucene104ScalarQuantizedVectorsFormat}.
 */
public class TestLucene104ScalarQuantizedVectorsFormatPreconditioning extends LuceneTestCase {

  /** Search with preconditioning returns correct top-K results. */
  public void testPreconditionedSearchReturnsResults() throws Exception {
    int dims = 64;
    int numDocs = 200;

    Codec codec =
        codecWithFormat(
            new Lucene104ScalarQuantizedVectorsFormat(ScalarEncoding.UNSIGNED_BYTE, true));
    IndexWriterConfig iwc = newIndexWriterConfig().setCodec(codec);

    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, iwc)) {
      float[][] vectors = new float[numDocs][];
      for (int i = 0; i < numDocs; i++) {
        float[] v = randomGaussianVector(dims);
        vectors[i] = v;
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("field", v, VectorSimilarityFunction.EUCLIDEAN));
        w.addDocument(doc);
      }
      w.commit();

      try (IndexReader reader = DirectoryReader.open(w)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        float[] query = vectors[0].clone();
        TopDocs top = searcher.search(new KnnFloatVectorQuery("field", query, 5), 5);
        assertTrue(
            "expected at least 1 hit, got " + top.totalHits.value(), top.totalHits.value() >= 1);
        Set<Integer> ids = new HashSet<>();
        for (var sd : top.scoreDocs) {
          ids.add(sd.doc);
        }
        assertTrue("doc 0 should be among top-5 hits: got " + ids, ids.contains(0));
      }
    }
  }

  /** Verifies that getFloatVectorValues returns the original (inverse-rotated) vectors. */
  public void testGetFloatVectorValuesInverseRotates() throws Exception {
    int dims = 32;
    int numDocs = 8;

    Codec codec =
        codecWithFormat(
            new Lucene104ScalarQuantizedVectorsFormat(ScalarEncoding.UNSIGNED_BYTE, true));
    IndexWriterConfig iwc = newIndexWriterConfig().setCodec(codec);

    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, iwc)) {
      float[][] indexed = new float[numDocs][];
      for (int i = 0; i < numDocs; i++) {
        float[] v = randomGaussianVector(dims);
        indexed[i] = v.clone();
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("field", v, VectorSimilarityFunction.EUCLIDEAN));
        w.addDocument(doc);
      }
      w.forceMerge(1);
      w.commit();

      try (IndexReader reader = DirectoryReader.open(w)) {
        var leaf = reader.leaves().get(0).reader();
        var values = leaf.getFloatVectorValues("field");
        assertNotNull(values);
        var it = values.iterator();
        int count = 0;
        for (int doc = it.nextDoc();
            doc != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
            doc = it.nextDoc()) {
          float[] got = values.vectorValue(it.index());
          assertArrayEquals(
              "vector for doc " + doc + " should round-trip through rotation",
              indexed[doc],
              got,
              1e-4f);
          count++;
        }
        assertEquals(numDocs, count);
      }
    }
  }

  /** Verifies that the rotation seed is deterministic from field name. */
  public void testRotationSeedDeterministic() {
    long s1 = Lucene104ScalarQuantizedVectorsFormat.rotationSeed("myfield");
    long s2 = Lucene104ScalarQuantizedVectorsFormat.rotationSeed("myfield");
    assertEquals(s1, s2);
    // Different fields get different seeds
    long s3 = Lucene104ScalarQuantizedVectorsFormat.rotationSeed("otherfield");
    assertNotEquals(s1, s3);
  }

  private static Codec codecWithFormat(KnnVectorsFormat format) {
    return new FilterCodec(Codec.getDefault().getName(), Codec.getDefault()) {
      @Override
      public KnnVectorsFormat knnVectorsFormat() {
        return new PerFieldKnnVectorsFormat() {
          @Override
          public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
            return format;
          }
        };
      }
    };
  }

  private float[] randomGaussianVector(int dims) {
    float[] v = new float[dims];
    for (int i = 0; i < dims; i++) {
      v[i] = (float) random().nextGaussian();
    }
    return v;
  }
}
