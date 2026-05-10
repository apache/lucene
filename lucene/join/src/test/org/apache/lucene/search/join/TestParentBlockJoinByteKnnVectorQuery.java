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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnByteVectorField;
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
import org.apache.lucene.util.BytesRef;

public class TestParentBlockJoinByteKnnVectorQuery extends ParentBlockJoinKnnVectorQueryTestCase {

  @Override
  Query getParentJoinKnnQuery(
      String fieldName,
      float[] queryVector,
      Query childFilter,
      int k,
      BitSetProducer parentBitSet) {
    return new DiversifyingChildrenByteKnnVectorQuery(
        fieldName, fromFloat(queryVector), childFilter, k, parentBitSet);
  }

  @Override
  Field getKnnVectorField(String name, float[] vector) {
    return new KnnByteVectorField(name, fromFloat(vector));
  }

  @Override
  Field getKnnVectorField(
      String name, float[] vector, VectorSimilarityFunction vectorSimilarityFunction) {
    return new KnnByteVectorField(name, fromFloat(vector), vectorSimilarityFunction);
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
            new DiversifyingChildrenFloatKnnVectorQuery(
                "field", new float[] {1, 2}, null, 2, parentFilter);
        assertThrows(IllegalStateException.class, () -> searcher.search(kvq, 3));
      }
    }
  }

  public void testToString() {
    // test without filter
    Query query = getParentJoinKnnQuery("field", new float[] {0, 1}, null, 10, null);
    assertEquals(
        "DiversifyingChildrenByteKnnVectorQuery:field[0,...][10]", query.toString("ignored"));

    // test with filter
    Query filter = new TermQuery(new Term("id", "text"));
    query = getParentJoinKnnQuery("field", new float[] {0, 1}, filter, 10, null);
    assertEquals(
        "DiversifyingChildrenByteKnnVectorQuery:field[0,...][10][id:text]",
        query.toString("ignored"));
  }

  private static byte[] fromFloat(float[] queryVector) {
    byte[] query = new byte[queryVector.length];
    for (int i = 0; i < queryVector.length; i++) {
      assert queryVector[i] == (byte) queryVector[i];
      query[i] = (byte) queryVector[i];
    }
    return query;
  }

  @Override
  float[] randomVector(int dim) {
    BytesRef v = TestUtil.randomBinaryTerm(random(), dim);
    // clip at -127 to avoid overflow
    for (int i = v.offset; i < v.offset + v.length; i++) {
      if (v.bytes[i] == -128) {
        v.bytes[i] = -127;
      }
    }
    assert v.offset == 0;
    byte[] b = v.bytes;
    float[] v1 = new float[b.length];
    int vi = 0;
    for (int i = 0; i < v.length; i++) {
      v1[vi++] = b[i];
    }
    return v1;
  }

  /**
   * Byte-vector parallel of {@code
   * TestParentBlockJoinFloatKnnVectorQuery#testBlockRescoreCorrectsSuboptimalResult}: proves the
   * shared static {@link DiversifyingChildrenFloatKnnVectorQuery#blockRescore} works correctly when
   * called with a {@link org.apache.lucene.index.ByteVectorValues} scorer.
   */
  public void testBlockRescoreCorrectsSuboptimalResult() throws IOException {
    final byte[] queryVector = new byte[] {1, 0};

    try (Directory d = newDirectory()) {
      try (IndexWriter w =
          new IndexWriter(
              d,
              new IndexWriterConfig()
                  .setCodec(TestUtil.getDefaultCodec())
                  .setMergePolicy(newMergePolicy(random(), false)))) {
        for (int p = 0; p < 3; p++) {
          List<Document> block = new ArrayList<>();
          Document bad = new Document();
          bad.add(new KnnByteVectorField("field", new byte[] {0, 1}, DOT_PRODUCT));
          block.add(bad);
          Document good = new Document();
          good.add(new KnnByteVectorField("field", new byte[] {1, 0}, DOT_PRODUCT));
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

        VectorScorer badScorer = leaf.reader().getByteVectorValues("field").scorer(queryVector);
        badScorer.iterator().advance(0);
        float badScore = badScorer.score();

        //   parent 0 → docId 2,  children are docId 0 (bad) and docId 1 (good)
        //   parent 1 → docId 5,  children are docId 3 (bad) and docId 4 (good)
        //   parent 2 → docId 8,  children are docId 6 (bad) and docId 7 (good)
        ScoreDoc[] badResults =
            new ScoreDoc[] {
              new ScoreDoc(0, badScore), new ScoreDoc(3, badScore), new ScoreDoc(6, badScore),
            };
        TopDocs badTopDocs = new TopDocs(new TotalHits(10, EQUAL_TO), badResults);

        VectorScorer scorer = leaf.reader().getByteVectorValues("field").scorer(queryVector);
        TopDocs corrected =
            DiversifyingChildrenFloatKnnVectorQuery.blockRescore(
                badTopDocs, null, parentBitSet, scorer);

        assertEquals(3, corrected.scoreDocs.length);
        assertEquals(1, corrected.scoreDocs[0].doc);
        assertEquals(4, corrected.scoreDocs[1].doc);
        assertEquals(7, corrected.scoreDocs[2].doc);
        for (ScoreDoc sd : corrected.scoreDocs) {
          assertTrue(
              "expected score > badScore after rescoring, got " + sd.score,
              sd.score > badScore + 1e-5f);
        }
        assertEquals(10 + 3, corrected.totalHits.value());
      }
    }
  }

  /**
   * End-to-end verification for the byte variant: with {@code rescoreBlocks=true}, no sibling child
   * outscores the returned child for any found parent.
   */
  public void testBlockRescoreReturnsBestChildPerParent() throws IOException {
    final int numParents = 10;
    final int childrenPerParent = 4;
    final byte[] queryVector = new byte[] {1, 0, 0, 0};

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
            // Last child is aligned with query; others are orthogonal.
            byte[] vec = new byte[queryVector.length];
            vec[c == childrenPerParent - 1 ? 0 : 1 + (c % (queryVector.length - 1))] = 1;
            Document doc = new Document();
            doc.add(new KnnByteVectorField("field", vec, DOT_PRODUCT));
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

        DiversifyingChildrenByteKnnVectorQuery query =
            new DiversifyingChildrenByteKnnVectorQuery(
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
          VectorScorer sibScorer = leaf.reader().getByteVectorValues("field").scorer(queryVector);
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
}
