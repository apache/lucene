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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.hnsw.OrdinalTranslatedKnnCollector;

/**
 * Tests for sibling expansion in {@link DiversifyingNearestChildrenKnnCollector} ({@link
 * org.apache.lucene.util.hnsw.DocSiblingExpansion} contract) and end-to-end correctness via {@link
 * DiversifyingChildrenFloatKnnVectorQuery}.
 */
public class TestDiversifyingChildrenKnnSiblingExpansion
    extends DiversifyingChildrenKnnCollectorTestCase {

  // ---------------------------------------------------------------------------
  // Unit tests: DiversifyingNearestChildrenKnnCollector — findSiblingDocIds
  // ---------------------------------------------------------------------------

  public void testFindSiblingDocIds_returnsAllSiblings() throws IOException {
    // 2 parents, 2 children each → blocks: [0,1|2], [3,4|5]
    int numParents = 2, childrenPerParent = 2;
    BitSet parents = parentBitSet(numParents, childrenPerParent);
    int[] docToOrd = buildDocToOrd(numParents, childrenPerParent);
    DiversifyingNearestChildrenKnnCollector c = makeCollector(10, parents, docToOrd);

    // child 0: sibling is 1
    int[] s0 = c.findSiblingDocIds(0);
    assertNotNull(s0);
    assertArrayEquals(new int[] {1}, s0);

    // child 1: sibling is 0
    int[] s1 = c.findSiblingDocIds(1);
    assertNotNull(s1);
    assertArrayEquals(new int[] {0}, s1);

    // child 3 (second block): sibling is 4
    int[] s3 = c.findSiblingDocIds(3);
    assertNotNull(s3);
    assertArrayEquals(new int[] {4}, s3);

    // child 4 (second block): sibling is 3
    int[] s4 = c.findSiblingDocIds(4);
    assertNotNull(s4);
    assertArrayEquals(new int[] {3}, s4);
  }

  public void testFindSiblingDocIds_parentAlreadyInHeap_returnsNull() throws IOException {
    // 1 parents, 2 children → block: [0,1|2]
    int numParents = 1, childrenPerParent = 2;
    BitSet parents = parentBitSet(numParents, childrenPerParent);
    int[] docToOrd = buildDocToOrd(numParents, childrenPerParent);
    DiversifyingNearestChildrenKnnCollector c = makeCollector(10, parents, docToOrd);

    // Collect child 0 → parent 2 enters the heap
    c.collect(0, 0.8f);

    // Now asking for siblings of child 1 (same parent 2) must return null
    assertNull(c.findSiblingDocIds(1));
  }

  public void testFindSiblingDocIds_singleChildParent_returnsNull() throws IOException {
    // 1 parents, 1 child each → blocks: [0|1]
    int numParents = 1, childrenPerParent = 1;
    BitSet parents = parentBitSet(numParents, childrenPerParent);
    int[] docToOrd = buildDocToOrd(numParents, childrenPerParent);
    DiversifyingNearestChildrenKnnCollector c = makeCollector(10, parents, docToOrd);

    assertNull("sole child has no siblings", c.findSiblingDocIds(0));
  }

  /**
   * The trigger child must not appear in its own sibling list. Only the *other* children of the
   * same parent are returned.
   */
  public void testFindSiblingDocIds_excludesTriggerChild() throws IOException {
    // 1 parent, 3 children: [C0, C1, C2 | P0=3]
    int numParents = 1, childrenPerParent = 3;
    BitSet parents = parentBitSet(numParents, childrenPerParent);
    int[] docToOrd = buildDocToOrd(numParents, childrenPerParent);
    DiversifyingNearestChildrenKnnCollector c = makeCollector(10, parents, docToOrd);

    // Trigger is C1 (docId=1); expected siblings: C0 and C2 only
    int[] siblings = c.findSiblingDocIds(1);
    assertNotNull(siblings);
    assertEquals(2, siblings.length);
    for (int s : siblings) {
      assertNotEquals("trigger child must not appear in sibling list", 1, s);
    }
  }

  // ---------------------------------------------------------------------------
  // Unit tests: DiversifyingNearestChildrenKnnCollector — docIdToOrdinal
  // ---------------------------------------------------------------------------

  public void testDocIdToOrdinal_correctMapping() throws IOException {
    // 2 parents, 2 children: docToOrd = [0, 1, -1, 2, 3, -1]
    int numParents = 2, childrenPerParent = 2;
    BitSet parents = parentBitSet(numParents, childrenPerParent);
    int[] docToOrd = buildDocToOrd(numParents, childrenPerParent);
    DiversifyingNearestChildrenKnnCollector c = makeCollector(10, parents, docToOrd);

    assertEquals(0, c.docIdToOrdinal(0));
    assertEquals(1, c.docIdToOrdinal(1));
    assertEquals(-1, c.docIdToOrdinal(2)); // parent → no vector
    assertEquals(2, c.docIdToOrdinal(3));
    assertEquals(3, c.docIdToOrdinal(4));
    assertEquals(-1, c.docIdToOrdinal(5)); // parent → no vector
    assertEquals(-1, c.docIdToOrdinal(9999)); // beyond array bounds
  }

  public void testDocIdToOrdinal_nullMapping_alwaysMinusOne() throws IOException {
    // Collector created without a docToOrd array (sibling expansion disabled)
    BitSet parents = parentBitSet(2, 2);
    DiversifyingNearestChildrenKnnCollector c =
        new DiversifyingNearestChildrenKnnCollector(5, Integer.MAX_VALUE, null, parents, null);

    assertEquals(-1, c.docIdToOrdinal(0));
    assertEquals(-1, c.docIdToOrdinal(1));
  }

  // ---------------------------------------------------------------------------
  // Unit tests: heap replacement behaviour
  // ---------------------------------------------------------------------------

  /**
   * C3 is the entry point (score 0.85) but its siblings C4 (0.95) and C5 (0.90) are expanded
   * immediately. The parent must be represented by C4, not C3.
   */
  public void testBestSiblingReplacesFirstFoundChild() throws IOException {
    // 1 parent, 3 children: [C0=0, C1=1, C2=2 | P0=3]
    int numParents = 1, childrenPerParent = 3;
    BitSet parents = parentBitSet(numParents, childrenPerParent);
    int[] docToOrd = buildDocToOrd(numParents, childrenPerParent);
    DiversifyingNearestChildrenKnnCollector c = makeCollector(1, parents, docToOrd);

    // C0 found first as entry point (like C3=0.85 in the example)
    c.collect(0, 0.85f);
    // Sibling expansion scores C1 (like C4=0.95) and C2 (like C5=0.90)
    c.collect(1, 0.95f);
    c.collect(2, 0.90f);

    TopDocs td = c.topDocs();
    assertEquals(1, td.scoreDocs.length);
    assertEquals(
        "best sibling must win over first-found child", 0.95f, td.scoreDocs[0].score, 1e-5f);
    assertEquals("best child doc id must be C1", 1, td.scoreDocs[0].doc);
  }

  /**
   * When two parents are found and the heap is full (k=2), a sibling of the second parent that
   * scores better than the trigger child replaces it.
   */
  public void testBestSiblingReplacesWorseChildWhenHeapFull() throws IOException {
    // 2 parents, 2 children each: [C0, C1 | P0=2], [C2, C3 | P1=5]
    int numParents = 2, childrenPerParent = 2;
    BitSet parents = parentBitSet(numParents, childrenPerParent);
    int[] docToOrd = buildDocToOrd(numParents, childrenPerParent);
    DiversifyingNearestChildrenKnnCollector c = makeCollector(2, parents, docToOrd);

    // P0: C0 found first (0.85), C1 is better sibling (0.95) → P0 represented by C1
    c.collect(0, 0.85f);
    c.collect(1, 0.95f);

    // P1: C2 found first (0.60), C3 is better sibling (0.65)
    c.collect(3, 0.60f); // trigger child → P1 enters heap, now FULL (size=2=k)
    c.collect(4, 0.65f); // better sibling → heap full but P1 already present, updates entry

    TopDocs td = c.topDocs();
    assertEquals(2, td.scoreDocs.length);
    assertEquals("P0 best child score", 0.95f, td.scoreDocs[0].score, 1e-5f);
    assertEquals("P1 best child score", 0.65f, td.scoreDocs[1].score, 1e-5f);
    assertEquals("P0 best child doc", 1, td.scoreDocs[0].doc);
    assertEquals("P1 best child doc", 4, td.scoreDocs[1].doc);
  }

  // ---------------------------------------------------------------------------
  // Unit tests: OrdinalTranslatedKnnCollector — getSiblingOrdinals
  // ---------------------------------------------------------------------------

  /**
   * Siblings whose ordinal is already in visitedOrds must be filtered out to prevent double-scoring
   * when the sibling was independently discovered via normal graph traversal.
   */
  public void testPendingSiblingOrdinals_filtersAlreadyVisited() throws IOException {
    // 1 parent, 3 children: [C0=doc0, C1=doc1, C2=doc2 | P0=doc3]
    // docToOrd=[0,1,2,-1]; ordToDoc=[0,1,2]
    BitSet parents = parentBitSet(1, 3);
    int[] docToOrd = buildDocToOrd(1, 3);
    int[] ordToDoc = {0, 1, 2};
    OrdinalTranslatedKnnCollector collector =
        new OrdinalTranslatedKnnCollector(
            makeCollector(10, parents, docToOrd), ord -> ordToDoc[ord]);

    // Mark C1 (ordinal 1) as already visited by normal graph traversal
    FixedBitSet visited = new FixedBitSet(4);
    visited.set(1);

    // Trigger on C0 (ordinal 0 → docId 0); siblings are C1 and C2
    int[] result = collector.getSiblingOrdinals(0, visited);
    assertNotNull(result);
    assertArrayEquals("C1 must be filtered (visited); only C2 remains", new int[] {2}, result);
  }

  /**
   * Siblings with no vector in this field (docIdToOrdinal returns -1) must be skipped because they
   * have no node in the HNSW graph and cannot be scored.
   */
  public void testPendingSiblingOrdinals_filtersSparseSiblings() throws IOException {
    // 1 parent, 3 children but C1 has no vector:
    // [C0=doc0, C1(sparse)=doc1, C2=doc2 | P0=doc3]
    // docToOrd=[0,-1,1,-1]; ordToDoc=[0,2]
    BitSet parents = parentBitSet(1, 3);
    int[] docToOrd = {0, -1, 1, -1};
    int[] ordToDoc = {0, 2};
    OrdinalTranslatedKnnCollector collector =
        new OrdinalTranslatedKnnCollector(
            makeCollector(10, parents, docToOrd), ord -> ordToDoc[ord]);

    FixedBitSet visited = new FixedBitSet(4);

    // Trigger on C0 (ordinal 0 → docId 0); siblings are C1 (sparse) and C2
    int[] result = collector.getSiblingOrdinals(0, visited);
    assertNotNull(result);
    assertArrayEquals("C1 (no vector) must be filtered; only C2 remains", new int[] {1}, result);
  }

  // ---------------------------------------------------------------------------
  // Index fixtures (used by integration tests only)
  // ---------------------------------------------------------------------------

  private static final String FIELD = "vec";
  private static final int DIM = 4;

  /**
   * Builds a block-join float-vector index. Parent p has numChildren children; child c of parent p
   * gets vector: v[i] = (p * numChildren + c + i) * 0.1, then normalised for COSINE/DOT_PRODUCT.
   */
  private Directory buildIndex(int numParents, int numChildren, VectorSimilarityFunction sim)
      throws IOException {
    Directory dir = newDirectory();
    try (IndexWriter w =
        new IndexWriter(
            dir, new IndexWriterConfig().setMergePolicy(newMergePolicy(random(), false)))) {
      for (int p = 0; p < numParents; p++) {
        List<Document> block = new ArrayList<>();
        for (int c = 0; c < numChildren; c++) {
          float[] vec = childVector(p, c, numChildren);
          if (sim == VectorSimilarityFunction.COSINE
              || sim == VectorSimilarityFunction.DOT_PRODUCT) {
            normalise(vec);
          }
          Document child = new Document();
          child.add(new KnnFloatVectorField(FIELD, vec, sim));
          child.add(new StoredField("parent", p));
          block.add(child);
        }
        Document parent = new Document();
        parent.add(new StringField("docType", "_parent", Field.Store.NO));
        parent.add(new StoredField("parent", p));
        block.add(parent);
        w.addDocuments(block);
      }
    }
    return dir;
  }

  private static float[] childVector(int parent, int child, int numChildren) {
    float[] vec = new float[DIM];
    for (int i = 0; i < DIM; i++) {
      vec[i] = (parent * numChildren + child + i + 1) * 0.1f;
    }
    return vec;
  }

  private static void normalise(float[] vec) {
    float norm = 0;
    for (float v : vec) norm += v * v;
    norm = (float) Math.sqrt(norm);
    if (norm > 0) {
      for (int i = 0; i < DIM; i++) vec[i] /= norm;
    }
  }

  /** Computes brute-force top-k scores: max similarity per parent, sorted descending. */
  private static float[] bruteForceTopK(
      float[] query, int numParents, int numChildren, VectorSimilarityFunction sim, int k) {
    float[] parentBest = new float[numParents];
    Arrays.fill(parentBest, Float.NEGATIVE_INFINITY);
    for (int p = 0; p < numParents; p++) {
      for (int c = 0; c < numChildren; c++) {
        float[] vec = childVector(p, c, numChildren);
        if (sim == VectorSimilarityFunction.COSINE || sim == VectorSimilarityFunction.DOT_PRODUCT) {
          normalise(vec);
        }
        float score = sim.compare(query, vec);
        if (score > parentBest[p]) parentBest[p] = score;
      }
    }
    float[] sorted = parentBest.clone();
    Arrays.sort(sorted);
    int n = Math.min(k, numParents);
    float[] top = new float[n];
    for (int i = 0; i < n; i++) top[i] = sorted[numParents - 1 - i];
    return top;
  }

  // ---------------------------------------------------------------------------
  // Integration tests: end-to-end correctness via DiversifyingChildrenFloatKnnVectorQuery
  // ---------------------------------------------------------------------------

  /**
   * Each result doc must belong to a distinct parent. Sibling expansion must not cause the same
   * parent to appear multiple times.
   */
  public void testSiblingExpansionNoDuplicateParents() throws Exception {
    int numParents = 15, numChildren = 4, k = 8;
    VectorSimilarityFunction sim = VectorSimilarityFunction.EUCLIDEAN;
    float[] query = {1f, 0f, 0f, 0f};

    try (Directory dir = buildIndex(numParents, numChildren, sim);
        IndexReader reader = DirectoryReader.open(dir)) {
      IndexSearcher searcher = newSearcher(reader);
      BitSetProducer parentFilter =
          new QueryBitSetProducer(new TermQuery(new Term("docType", "_parent")));

      Query knnQuery =
          new DiversifyingChildrenFloatKnnVectorQuery(FIELD, query, null, k, parentFilter);
      TopDocs results = searcher.search(knnQuery, k);

      Set<Integer> seenParents = new HashSet<>();
      for (ScoreDoc sd : results.scoreDocs) {
        int parentId =
            reader.storedFields().document(sd.doc).getField("parent").numericValue().intValue();
        assertTrue("parent " + parentId + " appeared more than once", seenParents.add(parentId));
      }
    }
  }

  /**
   * When every parent has exactly one child there are no siblings to expand. Results must still be
   * correct.
   */
  public void testSiblingExpansionSingleChildParents() throws Exception {
    int numParents = 12, numChildren = 1, k = 5;
    VectorSimilarityFunction sim = VectorSimilarityFunction.EUCLIDEAN;
    float[] query = {0.2f, 0.3f, 0.4f, 0.5f};

    try (Directory dir = buildIndex(numParents, numChildren, sim);
        IndexReader reader = DirectoryReader.open(dir)) {
      IndexSearcher searcher = newSearcher(reader);
      BitSetProducer parentFilter =
          new QueryBitSetProducer(new TermQuery(new Term("docType", "_parent")));

      Query knnQuery =
          new DiversifyingChildrenFloatKnnVectorQuery(FIELD, query, null, k, parentFilter);
      TopDocs results = searcher.search(knnQuery, k);

      float[] expected = bruteForceTopK(query, numParents, numChildren, sim, k);
      assertEquals(Math.min(k, numParents), results.scoreDocs.length);
      for (int i = 0; i < results.scoreDocs.length; i++) {
        assertEquals("score at rank " + i, expected[i], results.scoreDocs[i].score, 1e-4f);
      }
    }
  }

  /**
   * The query is close to one parent's children; sibling expansion must find the best child of each
   * discovered parent rather than whichever child the graph traversal happens to reach first.
   */
  public void testSiblingExpansion_bestChildPerParentFound() throws Exception {
    int numParents = 3, numChildren = 3, k = 2;
    VectorSimilarityFunction sim = VectorSimilarityFunction.EUCLIDEAN;
    float[] query = {0.9f, 0.9f, 0.9f, 0.9f};

    try (Directory dir = buildIndex(numParents, numChildren, sim);
        IndexReader reader = DirectoryReader.open(dir)) {
      IndexSearcher searcher = newSearcher(reader);
      BitSetProducer parentFilter =
          new QueryBitSetProducer(new TermQuery(new Term("docType", "_parent")));
      CheckJoinIndex.check(reader, parentFilter);

      Query knnQuery =
          new DiversifyingChildrenFloatKnnVectorQuery(FIELD, query, null, k, parentFilter);
      TopDocs results = searcher.search(knnQuery, k);

      assertEquals(k, results.scoreDocs.length);
      for (ScoreDoc sd : results.scoreDocs) {
        int parentIdx =
            reader.storedFields().document(sd.doc).getField("parent").numericValue().intValue();
        // verify no other child of the same parent scores higher than the returned one
        for (int c = 0; c < numChildren; c++) {
          float[] vec = childVector(parentIdx, c, numChildren);
          float cScore = sim.compare(query, vec);
          assertTrue(
              "parent "
                  + parentIdx
                  + " has a better child (score "
                  + cScore
                  + ") than returned doc "
                  + sd.doc
                  + " (score "
                  + sd.score
                  + ")",
              cScore <= sd.score + 1e-4f);
        }
      }
    }
  }

  /**
   * With a single parent and many children, sibling expansion must score all children so the best
   * one is returned. Without expansion the graph might stop early and miss the best child.
   */
  public void testSiblingExpansion_singleParentManyChildren() throws Exception {
    int numParents = 1, numChildren = 8, k = 1;
    VectorSimilarityFunction sim = VectorSimilarityFunction.EUCLIDEAN;
    float[] query = {0.9f, 0.8f, 0.7f, 0.6f};

    try (Directory dir = buildIndex(numParents, numChildren, sim);
        IndexReader reader = DirectoryReader.open(dir)) {
      IndexSearcher searcher = newSearcher(reader);
      BitSetProducer parentFilter =
          new QueryBitSetProducer(new TermQuery(new Term("docType", "_parent")));
      CheckJoinIndex.check(reader, parentFilter);

      Query knnQuery =
          new DiversifyingChildrenFloatKnnVectorQuery(FIELD, query, null, k, parentFilter);
      TopDocs results = searcher.search(knnQuery, k);

      float[] expected = bruteForceTopK(query, numParents, numChildren, sim, k);
      assertEquals(1, results.scoreDocs.length);
      assertEquals("best child of single parent", expected[0], results.scoreDocs[0].score, 1e-4f);
    }
  }
}
