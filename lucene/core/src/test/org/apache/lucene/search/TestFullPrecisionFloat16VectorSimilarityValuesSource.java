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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.KnnFloat16VectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.TestVectorUtil;
import org.apache.lucene.util.VectorUtil;

/** Tests {@link FullPrecisionFloat16VectorSimilarityValuesSource}. */
public class TestFullPrecisionFloat16VectorSimilarityValuesSource extends LuceneTestCase {

  private static final String KNN_FIELD = "knnField";
  private static final int VECTOR_DIMENSION = 8;

  /** A unit-length vector as float16 bit patterns, valid for every similarity function. */
  private static short[] randomFloat16Vector(int dim) {
    float[] v = TestVectorUtil.randomVector(dim);
    VectorUtil.l2normalize(v);
    short[] f16 = new short[dim];
    for (int i = 0; i < dim; i++) {
      f16[i] = Float.floatToFloat16(v[i]);
    }
    return f16;
  }

  public void testFloat16SimilarityScores() throws Exception {
    int numVectors = atLeast(50);
    VectorSimilarityFunction indexingSimilarityFunction =
        VectorSimilarityFunction.values()[
            random().nextInt(VectorSimilarityFunction.values().length)];

    List<short[]> vectors = new ArrayList<>();
    try (Directory dir = newDirectory()) {
      int id = 0;
      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        for (int i = 0; i < numVectors; i++) {
          Document doc = new Document();
          if (random().nextInt(100) < 30) {
            // skip the vector for some docs so the field is sparse
            doc.add(new IntField("has_vector", 0, Field.Store.YES));
          } else {
            short[] vector = randomFloat16Vector(VECTOR_DIMENSION);
            vectors.add(vector);
            doc.add(new IntField("id", id++, Field.Store.YES));
            doc.add(new KnnFloat16VectorField(KNN_FIELD, vector, indexingSimilarityFunction));
            doc.add(new IntField("has_vector", 1, Field.Store.YES));
          }
          w.addDocument(doc);
        }
        // a segment with no vectors at all
        for (int i = 0; i < 10; i++) {
          Document doc = new Document();
          doc.add(new IntField("has_vector", 0, Field.Store.YES));
          w.addDocument(doc);
        }
        w.flush();
      }

      short[] queryVector = randomFloat16Vector(VECTOR_DIMENSION);
      try (IndexReader reader = DirectoryReader.open(dir)) {
        for (LeafReaderContext ctx : reader.leaves()) {
          // exercise both ctors: explicit similarity function, and the field's configured one
          // (which routes through Float16VectorValues#rescorer)
          boolean explicitSimilarity = random().nextBoolean();
          VectorSimilarityFunction scoringSimilarityFunction;
          DoubleValues fpSimValues;
          if (explicitSimilarity) {
            scoringSimilarityFunction =
                VectorSimilarityFunction.values()[
                    random().nextInt(VectorSimilarityFunction.values().length)];
            fpSimValues =
                new FullPrecisionFloat16VectorSimilarityValuesSource(
                        queryVector, KNN_FIELD, scoringSimilarityFunction)
                    .getSimilarityScores(ctx);
          } else {
            scoringSimilarityFunction = indexingSimilarityFunction;
            fpSimValues =
                new FullPrecisionFloat16VectorSimilarityValuesSource(queryVector, KNN_FIELD)
                    .getSimilarityScores(ctx);
          }

          if (ctx.reader().getFloat16VectorValues(KNN_FIELD) == null) {
            assertSame(DoubleValues.EMPTY, fpSimValues);
            continue;
          }

          StoredFields storedFields = ctx.reader().storedFields();
          VectorScorer scorer = ctx.reader().getFloat16VectorValues(KNN_FIELD).scorer(queryVector);
          DocIdSetIterator disi = scorer.iterator();
          int scored = 0;
          while (disi.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            int doc = disi.docID();
            assertTrue(fpSimValues.advanceExact(doc));
            int idValue =
                Integer.parseInt(Objects.requireNonNull(storedFields.document(doc).get("id")));
            short[] docVector = vectors.get(idValue);
            // raw float16 vectors are stored exactly, so the score must match precisely
            double expected = scoringSimilarityFunction.compare(queryVector, docVector);
            assertEquals(expected, fpSimValues.doubleValue(), 1e-5);
            scored++;
          }
          assertTrue("expected at least one scored doc in this leaf", scored > 0);
        }
      }
    }
  }

  public void testRescoreTopNQueryWithFloat16Vectors() throws Exception {
    int numVectors = atLeast(50);
    VectorSimilarityFunction similarityFunction = VectorSimilarityFunction.DOT_PRODUCT;
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        for (int i = 0; i < numVectors; i++) {
          Document doc = new Document();
          doc.add(
              new KnnFloat16VectorField(
                  KNN_FIELD, randomFloat16Vector(VECTOR_DIMENSION), similarityFunction));
          w.addDocument(doc);
        }
      }
      short[] queryVector = randomFloat16Vector(VECTOR_DIMENSION);
      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);
        int n = 10;
        Query inner = new KnnFloat16VectorQuery(KNN_FIELD, queryVector, n);
        Query rescored =
            RescoreTopNQuery.createFullPrecisionRescorerQuery(inner, queryVector, KNN_FIELD, n);
        TopDocs topDocs = searcher.search(rescored, n);
        assertEquals(n, topDocs.scoreDocs.length);
        // rescored hits must come back in descending score order
        for (int i = 1; i < topDocs.scoreDocs.length; i++) {
          assertTrue(topDocs.scoreDocs[i - 1].score >= topDocs.scoreDocs[i].score);
        }
      }
    }
  }
}
