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
package org.apache.lucene.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestLiveDocs extends LuceneTestCase {

  public void testSparseLiveDocsBasic() {
    // GIVEN: SparseLiveDocs with 1000 documents
    final int maxDoc = 1000;
    SparseLiveDocs liveDocs = new SparseLiveDocs(maxDoc);

    // THEN: All docs are initially live
    assertEquals(maxDoc, liveDocs.length());
    assertEquals(0, liveDocs.deletedCount());

    for (int i = 0; i < maxDoc; i++) {
      assertTrue("Doc " + i + " should be live", liveDocs.get(i));
    }

    // WHEN: Deleting specific documents
    liveDocs.delete(10);
    liveDocs.delete(50);
    liveDocs.delete(100);

    // THEN: Deleted docs return false, live docs return true
    assertEquals(3, liveDocs.deletedCount());
    assertFalse("Doc 10 should be deleted", liveDocs.get(10));
    assertFalse("Doc 50 should be deleted", liveDocs.get(50));
    assertFalse("Doc 100 should be deleted", liveDocs.get(100));
    assertTrue("Doc 11 should be live", liveDocs.get(11));
    assertTrue("Doc 51 should be live", liveDocs.get(51));
  }

  public void testDenseLiveDocsBasic() {
    // GIVEN: DenseLiveDocs with 1000 documents
    final int maxDoc = 1000;
    DenseLiveDocs liveDocs = new DenseLiveDocs(maxDoc);

    // THEN: All docs are initially live
    assertEquals(maxDoc, liveDocs.length());
    assertEquals(0, liveDocs.deletedCount());

    for (int i = 0; i < maxDoc; i++) {
      assertTrue("Doc " + i + " should be live", liveDocs.get(i));
    }

    // WHEN: Deleting specific documents
    liveDocs.delete(10);
    liveDocs.delete(50);
    liveDocs.delete(100);

    // THEN: Deleted docs return false, live docs return true
    assertEquals(3, liveDocs.deletedCount());
    assertFalse("Doc 10 should be deleted", liveDocs.get(10));
    assertFalse("Doc 50 should be deleted", liveDocs.get(50));
    assertFalse("Doc 100 should be deleted", liveDocs.get(100));
    assertTrue("Doc 11 should be live", liveDocs.get(11));
    assertTrue("Doc 51 should be live", liveDocs.get(51));
  }

  public void testSparseLiveDocsIterator() throws IOException {
    // GIVEN: SparseLiveDocs with specific deleted documents
    final int maxDoc = 1000;
    SparseLiveDocs liveDocs = new SparseLiveDocs(maxDoc);
    int[] deletedDocs = {5, 50, 100, 150, 500, 999};
    for (int doc : deletedDocs) {
      liveDocs.delete(doc);
    }

    // WHEN: Getting deleted docs iterator
    DocIdSetIterator it = liveDocs.deletedDocsIterator();

    // THEN: Iterator cost matches deleted count
    assertEquals("Iterator cost should match deleted count", deletedDocs.length, it.cost());

    // THEN: Iterator returns all deleted docs in order
    List<Integer> iteratedDocs = new ArrayList<>();
    for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
      iteratedDocs.add(doc);
    }

    assertEquals("Should iterate exact number of deleted docs", deletedDocs.length, iteratedDocs.size());
    for (int i = 0; i < deletedDocs.length; i++) {
      assertEquals("Deleted doc mismatch at position " + i, deletedDocs[i], iteratedDocs.get(i).intValue());
    }
  }

  public void testDenseLiveDocsIterator() throws IOException {
    // GIVEN: DenseLiveDocs with specific deleted documents
    final int maxDoc = 1000;
    DenseLiveDocs liveDocs = new DenseLiveDocs(maxDoc);
    int[] deletedDocs = {5, 50, 100, 150, 500, 999};
    for (int doc : deletedDocs) {
      liveDocs.delete(doc);
    }

    // WHEN: Getting deleted docs iterator
    DocIdSetIterator it = liveDocs.deletedDocsIterator();

    // THEN: Iterator cost matches deleted count
    assertEquals("Iterator cost should match deleted count", deletedDocs.length, it.cost());

    // THEN: Iterator returns all deleted docs in order
    List<Integer> iteratedDocs = new ArrayList<>();
    for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
      iteratedDocs.add(doc);
    }

    assertEquals("Should iterate exact number of deleted docs", deletedDocs.length, iteratedDocs.size());
    for (int i = 0; i < deletedDocs.length; i++) {
      assertEquals("Deleted doc mismatch at position " + i, deletedDocs[i], iteratedDocs.get(i).intValue());
    }
  }

  public void testSparseDenseEquivalence() throws IOException {
    // GIVEN: Both SparseLiveDocs and DenseLiveDocs with same deletions
    final int maxDoc = 1000;
    SparseLiveDocs sparse = new SparseLiveDocs(maxDoc);
    DenseLiveDocs dense = new DenseLiveDocs(maxDoc);
    int[] deletedDocs = {1, 10, 50, 100, 200, 500, 750, 999};
    for (int doc : deletedDocs) {
      sparse.delete(doc);
      dense.delete(doc);
    }

    // THEN: Both have same deletion state
    assertEquals("Deleted counts should match", sparse.deletedCount(), dense.deletedCount());
    assertEquals("Lengths should match", sparse.length(), dense.length());

    // THEN: get() returns same results for all docs
    for (int i = 0; i < maxDoc; i++) {
      assertEquals("get(" + i + ") should match", sparse.get(i), dense.get(i));
    }

    // THEN: Iterators return same documents in same order
    DocIdSetIterator sparseIt = sparse.deletedDocsIterator();
    DocIdSetIterator denseIt = dense.deletedDocsIterator();

    int sparseDoc, denseDoc;
    do {
      sparseDoc = sparseIt.nextDoc();
      denseDoc = denseIt.nextDoc();
      assertEquals("Iterators should return same documents", sparseDoc, denseDoc);
    } while (sparseDoc != DocIdSetIterator.NO_MORE_DOCS);
  }

  public void testEmptyIterator() throws IOException {
    // GIVEN: LiveDocs with no deletions
    final int maxDoc = 1000;
    SparseLiveDocs sparse = new SparseLiveDocs(maxDoc);
    DenseLiveDocs dense = new DenseLiveDocs(maxDoc);

    // WHEN: Getting deleted docs iterator
    DocIdSetIterator sparseIt = sparse.deletedDocsIterator();
    DocIdSetIterator denseIt = dense.deletedDocsIterator();

    // THEN: Iterator immediately returns NO_MORE_DOCS
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, sparseIt.nextDoc());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, denseIt.nextDoc());
  }

  public void testIteratorAdvance() throws IOException {
    // GIVEN: DenseLiveDocs with documents 10, 20, 30, 40, 50 deleted
    final int maxDoc = 1000;
    DenseLiveDocs liveDocs = new DenseLiveDocs(maxDoc);
    for (int i = 10; i <= 50; i += 10) {
      liveDocs.delete(i);
    }

    // WHEN: Using iterator advance() method
    DocIdSetIterator it = liveDocs.deletedDocsIterator();

    // THEN: advance(15) returns next deleted doc 20
    assertEquals(20, it.advance(15));

    // THEN: nextDoc() returns 30
    assertEquals(30, it.nextDoc());

    // THEN: advance(45) returns 50
    assertEquals(50, it.advance(45));

    // THEN: advance(1000) returns NO_MORE_DOCS
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.advance(1000));
  }

  public void testRandomDeletions() throws IOException {
    // GIVEN: Random doc count and deletion ratio
    final int maxDoc = TestUtil.nextInt(random(), 1000, 10000);
    final double deleteRatio = random().nextDouble() * 0.5;

    // GIVEN: Both implementations with random deletions
    SparseLiveDocs sparse = new SparseLiveDocs(maxDoc);
    DenseLiveDocs dense = new DenseLiveDocs(maxDoc);
    List<Integer> expectedDeleted = new ArrayList<>();
    for (int i = 0; i < maxDoc; i++) {
      if (random().nextDouble() < deleteRatio) {
        sparse.delete(i);
        dense.delete(i);
        expectedDeleted.add(i);
      }
    }

    // THEN: Deletion counts match expected
    assertEquals("Deleted counts should match expected", expectedDeleted.size(), sparse.deletedCount());
    assertEquals("Sparse and dense counts should match", sparse.deletedCount(), dense.deletedCount());

    // THEN: get() returns correct live/deleted state
    for (int i = 0; i < maxDoc; i++) {
      boolean expectedLive = !expectedDeleted.contains(i);
      assertEquals("Sparse get(" + i + ") mismatch", expectedLive, sparse.get(i));
      assertEquals("Dense get(" + i + ") mismatch", expectedLive, dense.get(i));
    }

    // THEN: Iterators return all deleted docs correctly
    List<Integer> sparseDeleted = collectDocs(sparse.deletedDocsIterator());
    List<Integer> denseDeleted = collectDocs(dense.deletedDocsIterator());

    assertEquals("Sparse iterator should return all deleted docs", expectedDeleted, sparseDeleted);
    assertEquals("Dense iterator should return all deleted docs", expectedDeleted, denseDeleted);
  }

  public void testMemoryUsage() {
    // GIVEN: 1M docs with very sparse deletions (0.1%)
    final int maxDoc = 1000000;
    SparseLiveDocs sparse = new SparseLiveDocs(maxDoc);
    DenseLiveDocs dense = new DenseLiveDocs(maxDoc);
    for (int i = 0; i < maxDoc / 1000; i++) {
      sparse.delete(i * 1000);
      dense.delete(i * 1000);
    }

    // WHEN: Checking memory usage
    long sparseBytes = sparse.ramBytesUsed();
    long denseBytes = dense.ramBytesUsed();

    // THEN: Sparse uses less memory than dense
    assertTrue(
        "Sparse should use less memory than dense for 0.1% deletions (sparse="
            + sparseBytes
            + ", dense="
            + denseBytes
            + ", ratio="
            + String.format("%.2f%%", 100.0 * sparseBytes / denseBytes)
            + ")",
        sparseBytes < denseBytes);

    // THEN: Sparse achieves at least 50% memory reduction
    assertTrue(
        "Sparse should use significantly less memory (< 50%) for very sparse deletions (ratio="
            + String.format("%.2f%%", 100.0 * sparseBytes / denseBytes)
            + ")",
        sparseBytes < denseBytes / 2);
  }

  public void testWrappingExistingBitSets() {
    // GIVEN: Existing SparseFixedBitSet with deleted docs
    final int maxDoc = 1000;
    SparseFixedBitSet sparseDeleted = new SparseFixedBitSet(maxDoc);
    sparseDeleted.set(10);
    sparseDeleted.set(50);
    sparseDeleted.set(100);

    // WHEN: Wrapping in SparseLiveDocs
    SparseLiveDocs sparse = new SparseLiveDocs(sparseDeleted, maxDoc);

    // THEN: Deletion state is correctly represented
    assertEquals(3, sparse.deletedCount());
    assertFalse(sparse.get(10));
    assertFalse(sparse.get(50));
    assertTrue(sparse.get(11));

    // GIVEN: Existing FixedBitSet with live docs
    FixedBitSet liveSet = new FixedBitSet(maxDoc);
    liveSet.set(0, maxDoc);
    liveSet.clear(10);
    liveSet.clear(50);
    liveSet.clear(100);

    // WHEN: Wrapping in DenseLiveDocs
    DenseLiveDocs dense = new DenseLiveDocs(liveSet, maxDoc);

    // THEN: Deletion state is correctly represented
    assertEquals(3, dense.deletedCount());
    assertFalse(dense.get(10));
    assertFalse(dense.get(50));
    assertTrue(dense.get(11));
  }

  public void testSparseLiveDocsLiveIterator() throws IOException {
    // GIVEN: SparseLiveDocs with specific deleted documents
    final int maxDoc = 100;
    SparseLiveDocs liveDocs = new SparseLiveDocs(maxDoc);
    int[] deletedDocs = {5, 10, 50, 99};
    for (int doc : deletedDocs) {
      liveDocs.delete(doc);
    }

    // WHEN: Getting live docs iterator
    DocIdSetIterator it = liveDocs.liveDocsIterator();

    // THEN: Iterator cost matches live count
    assertEquals("Iterator cost should match live count", maxDoc - deletedDocs.length, it.cost());

    // THEN: Iterator returns all live docs in order (skipping deleted)
    List<Integer> iteratedDocs = collectDocs(it);
    assertEquals("Should iterate exact number of live docs", maxDoc - deletedDocs.length, iteratedDocs.size());

    // Verify no deleted docs in the iteration
    for (int deletedDoc : deletedDocs) {
      assertFalse("Deleted doc " + deletedDoc + " should not be in live iterator", iteratedDocs.contains(deletedDoc));
    }

    // Verify all docs are in ascending order
    for (int i = 1; i < iteratedDocs.size(); i++) {
      assertTrue("Docs should be in ascending order", iteratedDocs.get(i) > iteratedDocs.get(i - 1));
    }
  }

  public void testDenseLiveDocsLiveIterator() throws IOException {
    // GIVEN: DenseLiveDocs with specific deleted documents
    final int maxDoc = 100;
    DenseLiveDocs liveDocs = new DenseLiveDocs(maxDoc);
    int[] deletedDocs = {5, 10, 50, 99};
    for (int doc : deletedDocs) {
      liveDocs.delete(doc);
    }

    // WHEN: Getting live docs iterator
    DocIdSetIterator it = liveDocs.liveDocsIterator();

    // THEN: Iterator cost matches live count
    assertEquals("Iterator cost should match live count", maxDoc - deletedDocs.length, it.cost());

    // THEN: Iterator returns all live docs in order (skipping deleted)
    List<Integer> iteratedDocs = collectDocs(it);
    assertEquals("Should iterate exact number of live docs", maxDoc - deletedDocs.length, iteratedDocs.size());

    // Verify no deleted docs in the iteration
    for (int deletedDoc : deletedDocs) {
      assertFalse("Deleted doc " + deletedDoc + " should not be in live iterator", iteratedDocs.contains(deletedDoc));
    }

    // Verify all docs are in ascending order
    for (int i = 1; i < iteratedDocs.size(); i++) {
      assertTrue("Docs should be in ascending order", iteratedDocs.get(i) > iteratedDocs.get(i - 1));
    }
  }

  public void testLiveIteratorEquivalence() throws IOException {
    // GIVEN: Both SparseLiveDocs and DenseLiveDocs with same deletions
    final int maxDoc = 1000;
    SparseLiveDocs sparse = new SparseLiveDocs(maxDoc);
    DenseLiveDocs dense = new DenseLiveDocs(maxDoc);
    int[] deletedDocs = {1, 10, 50, 100, 200, 500, 750, 999};
    for (int doc : deletedDocs) {
      sparse.delete(doc);
      dense.delete(doc);
    }

    // WHEN: Getting live docs iterators
    DocIdSetIterator sparseIt = sparse.liveDocsIterator();
    DocIdSetIterator denseIt = dense.liveDocsIterator();

    // THEN: Both iterators return same documents in same order
    int sparseDoc, denseDoc;
    do {
      sparseDoc = sparseIt.nextDoc();
      denseDoc = denseIt.nextDoc();
      assertEquals("Live iterators should return same documents", sparseDoc, denseDoc);
    } while (sparseDoc != DocIdSetIterator.NO_MORE_DOCS);
  }

  public void testLiveIteratorFullIteration() throws IOException {
    // GIVEN: LiveDocs with no deletions
    final int maxDoc = 1000;
    SparseLiveDocs sparse = new SparseLiveDocs(maxDoc);
    DenseLiveDocs dense = new DenseLiveDocs(maxDoc);

    // WHEN: Getting live docs iterator
    DocIdSetIterator sparseIt = sparse.liveDocsIterator();
    DocIdSetIterator denseIt = dense.liveDocsIterator();

    // THEN: Iterators return all documents
    List<Integer> sparseDocs = collectDocs(sparseIt);
    List<Integer> denseDocs = collectDocs(denseIt);

    assertEquals("Sparse iterator should return all docs", maxDoc, sparseDocs.size());
    assertEquals("Dense iterator should return all docs", maxDoc, denseDocs.size());

    // Verify they're all in order (0, 1, 2, ..., maxDoc-1)
    for (int i = 0; i < maxDoc; i++) {
      assertEquals("Sparse doc at position " + i, i, sparseDocs.get(i).intValue());
      assertEquals("Dense doc at position " + i, i, denseDocs.get(i).intValue());
    }
  }

  public void testLiveIteratorAdvance() throws IOException {
    // GIVEN: DenseLiveDocs with documents 10, 20, 30, 40, 50 deleted
    final int maxDoc = 1000;
    DenseLiveDocs liveDocs = new DenseLiveDocs(maxDoc);
    for (int i = 10; i <= 50; i += 10) {
      liveDocs.delete(i);
    }

    // WHEN: Using live iterator advance() method
    DocIdSetIterator it = liveDocs.liveDocsIterator();

    // THEN: advance(10) returns next live doc 11 (skipping deleted 10)
    assertEquals(11, it.advance(10));

    // THEN: nextDoc() returns 12
    assertEquals(12, it.nextDoc());

    // THEN: advance(20) returns next live doc 21 (skipping deleted 20)
    assertEquals(21, it.advance(20));

    // THEN: advance(500) returns 500 (live doc)
    assertEquals(500, it.advance(500));

    // THEN: advance(1000) returns NO_MORE_DOCS
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.advance(1000));
  }

  public void testRandomDeletionsLiveIterator() throws IOException {
    // GIVEN: Random doc count and deletion ratio
    final int maxDoc = TestUtil.nextInt(random(), 1000, 10000);
    final double deleteRatio = random().nextDouble() * 0.5;

    // GIVEN: Both implementations with random deletions
    SparseLiveDocs sparse = new SparseLiveDocs(maxDoc);
    DenseLiveDocs dense = new DenseLiveDocs(maxDoc);
    List<Integer> expectedLive = new ArrayList<>();
    for (int i = 0; i < maxDoc; i++) {
      if (random().nextDouble() < deleteRatio) {
        sparse.delete(i);
        dense.delete(i);
      } else {
        expectedLive.add(i);
      }
    }

    // THEN: Live iterators return all live docs correctly
    List<Integer> sparseLive = collectDocs(sparse.liveDocsIterator());
    List<Integer> denseLive = collectDocs(dense.liveDocsIterator());

    assertEquals("Sparse live iterator should return all live docs", expectedLive, sparseLive);
    assertEquals("Dense live iterator should return all live docs", expectedLive, denseLive);
  }

  private List<Integer> collectDocs(DocIdSetIterator it) throws IOException {
    List<Integer> docs = new ArrayList<>();
    for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
      docs.add(doc);
    }
    return docs;
  }
}
