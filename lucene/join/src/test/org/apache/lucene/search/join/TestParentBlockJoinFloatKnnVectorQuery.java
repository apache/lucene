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

package org.apache.lucene.search.join;

import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;
import static org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;
import static org.apache.lucene.search.TotalHits.Relation.EQUAL_TO;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BitSet;

public class TestParentBlockJoinFloatKnnVectorQuery extends ParentBlockJoinKnnVectorQueryTestCase {

  @Override
  Query getParentJoinKnnQuery(
      String fieldName,
      float[] queryVector,
      Query childFilter,
      int k,
      BitSetProducer parentBitSet) {
    return new DiversifyingChildrenFloatKnnVectorQuery(
        fieldName, queryVector, childFilter, k, parentBitSet);
  }

  public void testVectorEncodingMismatch() throws IOException {
    try (Directory d = newDirectory()) {
      try (IndexWriter w =
          new IndexWriter(
              d, new IndexWriterConfig().setMergePolicy(newMergePolicy(random(), false)))) {
        List<Document> toAdd = new ArrayList<>();
        Document doc = new Document();
        doc.add(getKnnVectorField("field", new float[] {1, 1}, COSINE));
        toAdd.add(doc);
        toAdd.add(makeParent(new int[] {1}));
        w.addDocuments(toAdd);
      }
      try (IndexReader reader = DirectoryReader.open(d)) {
        IndexSearcher searcher = newSearcher(reader);
        BitSetProducer parentFilter = parentFilter(reader);
        Query kvq =
            new DiversifyingChildrenByteKnnVectorQuery(
                "field", new byte[] {1, 2}, null, 2, parentFilter);
        assertThrows(IllegalStateException.class, () -> searcher.search(kvq, 3));
      }
    }
  }

  public void testScoreCosine() throws IOException {
    try (Directory d = newDirectory()) {
      try (IndexWriter w =
          new IndexWriter(
              d,
              new IndexWriterConfig()
                  .setCodec(TestUtil.getDefaultCodec())
                  .setMergePolicy(newMergePolicy(random(), false)))) {
        for (int j = 1; j <= 5; j++) {
          List<Document> toAdd = new ArrayList<>();
          Document doc = new Document();
          doc.add(getKnnVectorField("field", new float[] {j, j * j}, COSINE));
          doc.add(newStringField("id", Integer.toString(j), Field.Store.YES));
          toAdd.add(doc);
          toAdd.add(makeParent(new int[] {j}));
          w.addDocuments(toAdd);
        }
      }
      try (IndexReader reader = DirectoryReader.open(d)) {
        assertEquals(1, reader.leaves().size());
        IndexSearcher searcher = new IndexSearcher(reader);
        BitSetProducer parentFilter = parentFilter(searcher.getIndexReader());
        DiversifyingChildrenFloatKnnVectorQuery query =
            new DiversifyingChildrenFloatKnnVectorQuery(
                "field", new float[] {2, 3}, null, 3, parentFilter);
        /* score0 = ((2,3) * (1, 1) = 5) / (||2, 3|| * ||1, 1|| = sqrt(26)), then
         * normalized by (1 + x) /2.
         */
        float score0 =
            (float) ((1 + (2 * 1 + 3 * 1) / Math.sqrt((2 * 2 + 3 * 3) * (1 * 1 + 1 * 1))) / 2);

        /* score1 = ((2,3) * (2, 4) = 16) / (||2, 3|| * ||2, 4|| = sqrt(260)), then
         * normalized by (1 + x) /2
         */
        float score1 =
            (float) ((1 + (2 * 2 + 3 * 4) / Math.sqrt((2 * 2 + 3 * 3) * (2 * 2 + 4 * 4))) / 2);

        assertScorerResults(
            searcher, query, new float[] {score0, score1}, new String[] {"1", "2"}, 2);
      }
    }
  }

  public void testToString() {
    // test without filter
    Query query = getParentJoinKnnQuery("field", new float[] {0, 1}, null, 10, null);
    assertEquals(
        "DiversifyingChildrenFloatKnnVectorQuery:field[0.0,...][10]", query.toString("ignored"));

    // test with filter
    Query filter = new TermQuery(new Term("id", "text"));
    query = getParentJoinKnnQuery("field", new float[] {0.0f, 1.0f}, filter, 10, null);
    assertEquals(
        "DiversifyingChildrenFloatKnnVectorQuery:field[0.0,...][10][id:text]",
        query.toString("ignored"));
  }

  @Override
  Field getKnnVectorField(String name, float[] vector) {
    return new KnnFloatVectorField(name, vector);
  }

  @Override
  Field getKnnVectorField(
      String name, float[] vector, VectorSimilarityFunction vectorSimilarityFunction) {
    return new KnnFloatVectorField(name, vector, vectorSimilarityFunction);
  }

  @Override
  float[] randomVector(int dim) {
    float[] v = new float[dim];
    Random random = random();
    for (int i = 0; i < dim; i++) {
      v[i] = random.nextFloat();
    }
    return v;
  }

  /**
   * Directly proves that {@code blockRescore} corrects a suboptimal HNSW result: we manually
   * construct a {@link TopDocs} where every parent is represented by its worst child (the one
   * pointing away from the query), then verify the static rescore method replaces it with the best
   * child (the one pointing toward the query).
   *
   * <p>This test does not depend on HNSW traversal order and will always demonstrate the
   * correction.
   */
  public void testBlockRescoreCorrectsSuboptimalResult() throws IOException {
    // 3 parents, each with 2 children:
    //   child "bad"  → vec orthogonal to query  (DOT_PRODUCT Lucene score ≈ 0.5)
    //   child "good" → vec aligned with query   (DOT_PRODUCT Lucene score ≈ 1.0)
    final float[] queryVector = new float[] {1f, 0f};

    try (Directory d = newDirectory()) {
      try (IndexWriter w =
          new IndexWriter(
              d,
              new IndexWriterConfig()
                  .setCodec(TestUtil.getDefaultCodec())
                  .setMergePolicy(newMergePolicy(random(), false)))) {
        for (int p = 0; p < 3; p++) {
          List<Document> block = new ArrayList<>();
          // bad child: orthogonal to query
          Document bad = new Document();
          bad.add(new KnnFloatVectorField("field", new float[] {0f, 1f}, DOT_PRODUCT));
          block.add(bad);
          // good child: aligned with query
          Document good = new Document();
          good.add(new KnnFloatVectorField("field", new float[] {1f, 0f}, DOT_PRODUCT));
          block.add(good);
          block.add(makeParent(new int[] {0, 1}));
          w.addDocuments(block);
        }
        w.forceMerge(1);
      }

      try (IndexReader reader = DirectoryReader.open(d)) {
        LeafReaderContext leaf = reader.leaves().get(0);
        BitSetProducer parentFilter = parentFilter(reader);
        BitSet parentBitSet = parentFilter.getBitSet(leaf);

        // Score the bad child of each parent so we know its score.
        VectorScorer badScorer = leaf.reader().getFloatVectorValues("field").scorer(queryVector);
        badScorer.iterator().advance(0); // bad child of parent 0 is docId 0
        float badScore = badScorer.score();

        // Manually construct a TopDocs that reports only the BAD child per parent —
        // simulating an HNSW run that reached the wrong sibling first.
        //   parent 0 → docId 2,  children are docId 0 (bad) and docId 1 (good)
        //   parent 1 → docId 5,  children are docId 3 (bad) and docId 4 (good)
        //   parent 2 → docId 8,  children are docId 6 (bad) and docId 7 (good)
        ScoreDoc[] badResults =
            new ScoreDoc[] {
              new ScoreDoc(0, badScore), // bad child of parent 0
              new ScoreDoc(3, badScore), // bad child of parent 1
              new ScoreDoc(6, badScore), // bad child of parent 2
            };
        TopDocs badTopDocs = new TopDocs(new TotalHits(10, EQUAL_TO), badResults);

        // Rescore using the static package-private method.
        VectorScorer scorer = leaf.reader().getFloatVectorValues("field").scorer(queryVector);
        TopDocs corrected =
            DiversifyingChildrenFloatKnnVectorQuery.blockRescore(
                badTopDocs, null, parentBitSet, scorer);

        assertEquals(3, corrected.scoreDocs.length);
        // Every result should now be the GOOD child (one per parent).
        assertEquals(1, corrected.scoreDocs[0].doc); // good child of parent 0
        assertEquals(4, corrected.scoreDocs[1].doc); // good child of parent 1
        assertEquals(7, corrected.scoreDocs[2].doc); // good child of parent 2
        // Scores must be strictly better than the bad-child scores.
        for (ScoreDoc sd : corrected.scoreDocs) {
          assertTrue(
              "expected score > badScore after rescoring, got " + sd.score,
              sd.score > badScore + 1e-5f);
        }
        // visitedCount must have grown: each parent had 1 extra sibling scored.
        assertEquals(10 + 3, corrected.totalHits.value());
      }
    }
  }

  /**
   * End-to-end verification: with {@code rescoreBlocks=true}, the query always returns the best
   * child for each found parent (no sibling should outscore the returned child).
   *
   * <p>The invariant test uses a fresh {@link VectorScorer} per parent (docId-based, not
   * ordinal-based) to avoid the ordinal/docId confusion that plagues {@code
   * FloatVectorValues.vectorValue(int ord)}.
   */
  public void testBlockRescoreReturnsBestChildPerParent() throws IOException {
    final int numParents = 20;
    final int childrenPerParent = 8;
    final int dim = 4;
    final float[] queryVector = new float[] {1f, 0f, 0f, 0f};

    try (Directory d = newDirectory()) {
      try (IndexWriter w =
          new IndexWriter(
              d,
              new IndexWriterConfig()
                  .setCodec(TestUtil.getDefaultCodec())
                  .setMergePolicy(newMergePolicy(random(), false)))) {
        for (int p = 0; p < numParents; p++) {
          List<Document> block = new ArrayList<>();
          int[] childIds = new int[childrenPerParent];
          for (int c = 0; c < childrenPerParent; c++) {
            // Last child per parent (c == childrenPerParent - 1) is aligned with the query.
            // All other children use dims 1-3, so DOT_PRODUCT with [1,0,0,0] == 0 → score 0.5.
            float[] vec = new float[dim];
            if (c == childrenPerParent - 1) {
              vec[0] = 1f;
              vec[1] = (p + 1) * 1e-4f; // tiny perturbation to keep vectors distinct
            } else {
              vec[1 + (c % (dim - 1))] = 1f;
            }
            normalize(vec);
            Document doc = new Document();
            doc.add(new KnnFloatVectorField("field", vec, DOT_PRODUCT));
            block.add(doc);
            childIds[c] = c;
          }
          block.add(makeParent(childIds));
          w.addDocuments(block);
        }
        w.forceMerge(1);
      }

      try (IndexReader reader = DirectoryReader.open(d)) {
        assertEquals(1, reader.leaves().size());
        IndexSearcher searcher = new IndexSearcher(reader);
        BitSetProducer parentFilter = parentFilter(reader);

        // rescoreBlocks=true to enable the feature under test.
        DiversifyingChildrenFloatKnnVectorQuery query =
            new DiversifyingChildrenFloatKnnVectorQuery(
                "field",
                queryVector,
                null,
                numParents,
                parentFilter,
                KnnSearchStrategy.Hnsw.DEFAULT,
                true);
        TopDocs results = searcher.search(query, numParents);

        LeafReaderContext leaf = reader.leaves().get(0);
        BitSet parentBitSet = parentFilter.getBitSet(leaf);

        for (ScoreDoc sd : results.scoreDocs) {
          int parent = parentBitSet.nextSetBit(sd.doc);
          int prevParent = parent > 0 ? parentBitSet.prevSetBit(parent - 1) : -1;
          // Re-score siblings with a fresh docId-based scorer and verify nothing beats the result.
          VectorScorer sibScorer = leaf.reader().getFloatVectorValues("field").scorer(queryVector);
          DocIdSetIterator sibIter = sibScorer.iterator();
          for (int child = prevParent + 1; child < parent; child++) {
            if (sibIter.advance(child) == child) {
              float sibScore = sibScorer.score();
              assertTrue(
                  "block rescore missed a better child: parent="
                      + parent
                      + " returnedChild="
                      + sd.doc
                      + " returnedScore="
                      + sd.score
                      + " siblingChild="
                      + child
                      + " siblingScore="
                      + sibScore,
                  sd.score + 1e-5f >= sibScore);
            }
          }
        }
      }
    }
  }

  private static void normalize(float[] v) {
    float norm = 0f;
    for (float x : v) norm += x * x;
    norm = (float) Math.sqrt(norm);
    if (norm > 0f) {
      for (int i = 0; i < v.length; i++) v[i] /= norm;
    }
  }
}
