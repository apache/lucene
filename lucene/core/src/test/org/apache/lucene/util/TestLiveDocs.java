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
import java.util.Locale;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestLiveDocs extends LuceneTestCase {

  public void testSparseLiveDocsBasic() {
    // GIVEN
    final int maxDoc = 1000;
    SparseFixedBitSet sparseSet = new SparseFixedBitSet(maxDoc);
    sparseSet.set(10);
    sparseSet.set(50);
    sparseSet.set(100);

    // WHEN
    SparseLiveDocs liveDocs = SparseLiveDocs.builder(sparseSet, maxDoc).build();

    // THEN
    assertEquals(3, liveDocs.deletedCount());
    assertFalse("Doc 10 should be deleted", liveDocs.get(10));
    assertFalse("Doc 50 should be deleted", liveDocs.get(50));
    assertFalse("Doc 100 should be deleted", liveDocs.get(100));
    assertTrue("Doc 11 should be live", liveDocs.get(11));
    assertTrue("Doc 51 should be live", liveDocs.get(51));
  }

  public void testDenseLiveDocsBasic() {
    // GIVEN
    final int maxDoc = 1000;
    FixedBitSet fixedSet = new FixedBitSet(maxDoc);
    fixedSet.set(0, maxDoc);
    fixedSet.clear(10);
    fixedSet.clear(50);
    fixedSet.clear(100);

    // WHEN
    DenseLiveDocs liveDocs = DenseLiveDocs.builder(fixedSet, maxDoc).build();

    // THEN
    assertEquals(3, liveDocs.deletedCount());
    assertFalse("Doc 10 should be deleted", liveDocs.get(10));
    assertFalse("Doc 50 should be deleted", liveDocs.get(50));
    assertFalse("Doc 100 should be deleted", liveDocs.get(100));
    assertTrue("Doc 11 should be live", liveDocs.get(11));
    assertTrue("Doc 51 should be live", liveDocs.get(51));
  }

  public void testSparseLiveDocsIterator() throws IOException {
    // GIVEN
    final int maxDoc = 1000;
    SparseFixedBitSet sparseSet = new SparseFixedBitSet(maxDoc);
    int[] deletedDocs = {5, 50, 100, 150, 500, 999};
    for (int doc : deletedDocs) {
      sparseSet.set(doc);
    }
    SparseLiveDocs liveDocs = SparseLiveDocs.builder(sparseSet, maxDoc).build();

    // WHEN
    DocIdSetIterator it = liveDocs.deletedDocsIterator();
    List<Integer> iteratedDocs = new ArrayList<>();
    for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
      iteratedDocs.add(doc);
    }

    // THEN
    assertEquals("Iterator cost should match deleted count", deletedDocs.length, it.cost());
    assertEquals(
        "Should iterate exact number of deleted docs", deletedDocs.length, iteratedDocs.size());
    for (int i = 0; i < deletedDocs.length; i++) {
      assertEquals(
          "Deleted doc mismatch at position " + i, deletedDocs[i], iteratedDocs.get(i).intValue());
    }
  }

  public void testDenseLiveDocsIterator() throws IOException {
    // GIVEN
    final int maxDoc = 1000;
    FixedBitSet fixedSet = new FixedBitSet(maxDoc);
    fixedSet.set(0, maxDoc);
    int[] deletedDocs = {5, 50, 100, 150, 500, 999};
    for (int doc : deletedDocs) {
      fixedSet.clear(doc);
    }
    DenseLiveDocs liveDocs = DenseLiveDocs.builder(fixedSet, maxDoc).build();

    // WHEN
    DocIdSetIterator it = liveDocs.deletedDocsIterator();
    List<Integer> iteratedDocs = new ArrayList<>();
    for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
      iteratedDocs.add(doc);
    }

    // THEN
    assertEquals("Iterator cost should match deleted count", deletedDocs.length, it.cost());
    assertEquals(
        "Should iterate exact number of deleted docs", deletedDocs.length, iteratedDocs.size());
    for (int i = 0; i < deletedDocs.length; i++) {
      assertEquals(
          "Deleted doc mismatch at position " + i, deletedDocs[i], iteratedDocs.get(i).intValue());
    }
  }

  public void testSparseDenseEquivalence() throws IOException {
    // GIVEN
    final int maxDoc = 1000;
    SparseFixedBitSet sparseSet = new SparseFixedBitSet(maxDoc);
    FixedBitSet fixedSet = new FixedBitSet(maxDoc);
    fixedSet.set(0, maxDoc);
    int[] deletedDocs = {1, 10, 50, 100, 200, 500, 750, 999};
    for (int doc : deletedDocs) {
      sparseSet.set(doc);
      fixedSet.clear(doc);
    }

    // WHEN
    SparseLiveDocs sparse = SparseLiveDocs.builder(sparseSet, maxDoc).build();
    DenseLiveDocs dense = DenseLiveDocs.builder(fixedSet, maxDoc).build();
    DocIdSetIterator sparseIt = sparse.deletedDocsIterator();
    DocIdSetIterator denseIt = dense.deletedDocsIterator();

    // THEN
    assertEquals("Deleted counts should match", sparse.deletedCount(), dense.deletedCount());
    assertEquals("Lengths should match", sparse.length(), dense.length());
    for (int i = 0; i < maxDoc; i++) {
      assertEquals("get(" + i + ") should match", sparse.get(i), dense.get(i));
    }
    int sparseDoc, denseDoc;
    do {
      sparseDoc = sparseIt.nextDoc();
      denseDoc = denseIt.nextDoc();
      assertEquals("Iterators should return same documents", sparseDoc, denseDoc);
    } while (sparseDoc != DocIdSetIterator.NO_MORE_DOCS);
  }

  public void testEmptyIterator() throws IOException {
    // GIVEN
    final int maxDoc = 1000;
    SparseFixedBitSet sparseSet = new SparseFixedBitSet(maxDoc);
    FixedBitSet fixedSet = new FixedBitSet(maxDoc);
    fixedSet.set(0, maxDoc);
    SparseLiveDocs sparse = SparseLiveDocs.builder(sparseSet, maxDoc).build();
    DenseLiveDocs dense = DenseLiveDocs.builder(fixedSet, maxDoc).build();

    // WHEN
    DocIdSetIterator sparseIt = sparse.deletedDocsIterator();
    DocIdSetIterator denseIt = dense.deletedDocsIterator();

    // THEN
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, sparseIt.nextDoc());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, denseIt.nextDoc());
  }

  public void testIteratorAdvance() throws IOException {
    // GIVEN
    final int maxDoc = 1000;
    FixedBitSet fixedSet = new FixedBitSet(maxDoc);
    fixedSet.set(0, maxDoc);
    for (int i = 10; i <= 50; i += 10) {
      fixedSet.clear(i);
    }
    DenseLiveDocs liveDocs = DenseLiveDocs.builder(fixedSet, maxDoc).build();

    // WHEN
    DocIdSetIterator it = liveDocs.deletedDocsIterator();

    // THEN
    assertEquals(20, it.advance(15));
    assertEquals(30, it.nextDoc());
    assertEquals(50, it.advance(45));
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.advance(1000));
  }

  public void testRandomDeletions() throws IOException {
    // GIVEN
    final int maxDoc = TestUtil.nextInt(random(), 1000, 10000);
    final double deleteRatio = random().nextDouble() * 0.5;
    SparseFixedBitSet sparseSet = new SparseFixedBitSet(maxDoc);
    FixedBitSet fixedSet = new FixedBitSet(maxDoc);
    fixedSet.set(0, maxDoc);
    List<Integer> expectedDeleted = new ArrayList<>();
    for (int i = 0; i < maxDoc; i++) {
      if (random().nextDouble() < deleteRatio) {
        sparseSet.set(i);
        fixedSet.clear(i);
        expectedDeleted.add(i);
      }
    }

    // WHEN
    SparseLiveDocs sparse = SparseLiveDocs.builder(sparseSet, maxDoc).build();
    DenseLiveDocs dense = DenseLiveDocs.builder(fixedSet, maxDoc).build();
    List<Integer> sparseDeleted = collectDocs(sparse.deletedDocsIterator());
    List<Integer> denseDeleted = collectDocs(dense.deletedDocsIterator());

    // THEN
    assertEquals(
        "Deleted counts should match expected", expectedDeleted.size(), sparse.deletedCount());
    assertEquals(
        "Sparse and dense counts should match", sparse.deletedCount(), dense.deletedCount());
    for (int i = 0; i < maxDoc; i++) {
      boolean expectedLive = !expectedDeleted.contains(i);
      assertEquals("Sparse get(" + i + ") mismatch", expectedLive, sparse.get(i));
      assertEquals("Dense get(" + i + ") mismatch", expectedLive, dense.get(i));
    }
    assertEquals("Sparse iterator should return all deleted docs", expectedDeleted, sparseDeleted);
    assertEquals("Dense iterator should return all deleted docs", expectedDeleted, denseDeleted);
  }

  public void testMemoryUsage() {
    // GIVEN
    final int maxDoc = 1000000;
    SparseFixedBitSet sparseSet = new SparseFixedBitSet(maxDoc);
    FixedBitSet fixedSet = new FixedBitSet(maxDoc);
    fixedSet.set(0, maxDoc);
    for (int i = 0; i < maxDoc / 1000; i++) {
      sparseSet.set(i * 1000);
      fixedSet.clear(i * 1000);
    }

    // WHEN
    SparseLiveDocs sparse = SparseLiveDocs.builder(sparseSet, maxDoc).build();
    DenseLiveDocs dense = DenseLiveDocs.builder(fixedSet, maxDoc).build();
    long sparseBytes = sparse.ramBytesUsed();
    long denseBytes = dense.ramBytesUsed();

    // THEN
    assertTrue(
        "Sparse should use less memory than dense for 0.1% deletions (sparse="
            + sparseBytes
            + ", dense="
            + denseBytes
            + ", ratio="
            + String.format(Locale.ROOT, "%.2f%%", 100.0 * sparseBytes / denseBytes)
            + ")",
        sparseBytes < denseBytes);
    assertTrue(
        "Sparse should use significantly less memory (< 50%) for very sparse deletions (ratio="
            + String.format(Locale.ROOT, "%.2f%%", 100.0 * sparseBytes / denseBytes)
            + ")",
        sparseBytes < denseBytes / 2);
  }

  public void testWrappingExistingBitSets() {
    // GIVEN
    final int maxDoc = 1000;
    SparseFixedBitSet sparseDeleted = new SparseFixedBitSet(maxDoc);
    sparseDeleted.set(10);
    sparseDeleted.set(50);
    sparseDeleted.set(100);
    FixedBitSet liveSet = new FixedBitSet(maxDoc);
    liveSet.set(0, maxDoc);
    liveSet.clear(10);
    liveSet.clear(50);
    liveSet.clear(100);

    // WHEN
    SparseLiveDocs sparse = SparseLiveDocs.builder(sparseDeleted, maxDoc).build();
    DenseLiveDocs dense = DenseLiveDocs.builder(liveSet, maxDoc).build();

    // THEN
    assertEquals(3, sparse.deletedCount());
    assertFalse(sparse.get(10));
    assertFalse(sparse.get(50));
    assertTrue(sparse.get(11));
    assertEquals(3, dense.deletedCount());
    assertFalse(dense.get(10));
    assertFalse(dense.get(50));
    assertTrue(dense.get(11));
  }

  public void testSparseLiveDocsLiveIterator() throws IOException {
    // GIVEN
    final int maxDoc = 100;
    SparseFixedBitSet sparseSet = new SparseFixedBitSet(maxDoc);
    int[] deletedDocs = {5, 10, 50, 99};
    for (int doc : deletedDocs) {
      sparseSet.set(doc);
    }
    SparseLiveDocs liveDocs = SparseLiveDocs.builder(sparseSet, maxDoc).build();

    // WHEN
    DocIdSetIterator it = liveDocs.liveDocsIterator();
    List<Integer> iteratedDocs = collectDocs(it);

    // THEN
    assertEquals("Iterator cost should match live count", maxDoc - deletedDocs.length, it.cost());
    assertEquals(
        "Should iterate exact number of live docs",
        maxDoc - deletedDocs.length,
        iteratedDocs.size());
    for (int deletedDoc : deletedDocs) {
      assertFalse(
          "Deleted doc " + deletedDoc + " should not be in live iterator",
          iteratedDocs.contains(deletedDoc));
    }
    for (int i = 1; i < iteratedDocs.size(); i++) {
      assertTrue(
          "Docs should be in ascending order", iteratedDocs.get(i) > iteratedDocs.get(i - 1));
    }
  }

  public void testDenseLiveDocsLiveIterator() throws IOException {
    // GIVEN
    final int maxDoc = 100;
    FixedBitSet fixedSet = new FixedBitSet(maxDoc);
    fixedSet.set(0, maxDoc);
    int[] deletedDocs = {5, 10, 50, 99};
    for (int doc : deletedDocs) {
      fixedSet.clear(doc);
    }
    DenseLiveDocs liveDocs = DenseLiveDocs.builder(fixedSet, maxDoc).build();

    // WHEN
    DocIdSetIterator it = liveDocs.liveDocsIterator();
    List<Integer> iteratedDocs = collectDocs(it);

    // THEN
    assertEquals("Iterator cost should match live count", maxDoc - deletedDocs.length, it.cost());
    assertEquals(
        "Should iterate exact number of live docs",
        maxDoc - deletedDocs.length,
        iteratedDocs.size());
    for (int deletedDoc : deletedDocs) {
      assertFalse(
          "Deleted doc " + deletedDoc + " should not be in live iterator",
          iteratedDocs.contains(deletedDoc));
    }
    for (int i = 1; i < iteratedDocs.size(); i++) {
      assertTrue(
          "Docs should be in ascending order", iteratedDocs.get(i) > iteratedDocs.get(i - 1));
    }
  }

  public void testLiveIteratorEquivalence() throws IOException {
    // GIVEN
    final int maxDoc = 1000;
    SparseFixedBitSet sparseSet = new SparseFixedBitSet(maxDoc);
    FixedBitSet fixedSet = new FixedBitSet(maxDoc);
    fixedSet.set(0, maxDoc);
    int[] deletedDocs = {1, 10, 50, 100, 200, 500, 750, 999};
    for (int doc : deletedDocs) {
      sparseSet.set(doc);
      fixedSet.clear(doc);
    }
    SparseLiveDocs sparse = SparseLiveDocs.builder(sparseSet, maxDoc).build();
    DenseLiveDocs dense = DenseLiveDocs.builder(fixedSet, maxDoc).build();

    // WHEN
    DocIdSetIterator sparseIt = sparse.liveDocsIterator();
    DocIdSetIterator denseIt = dense.liveDocsIterator();

    // THEN
    int sparseDoc, denseDoc;
    do {
      sparseDoc = sparseIt.nextDoc();
      denseDoc = denseIt.nextDoc();
      assertEquals("Live iterators should return same documents", sparseDoc, denseDoc);
    } while (sparseDoc != DocIdSetIterator.NO_MORE_DOCS);
  }

  public void testLiveIteratorFullIteration() throws IOException {
    // GIVEN
    final int maxDoc = 1000;
    SparseFixedBitSet sparseSet = new SparseFixedBitSet(maxDoc);
    FixedBitSet fixedSet = new FixedBitSet(maxDoc);
    fixedSet.set(0, maxDoc);
    SparseLiveDocs sparse = SparseLiveDocs.builder(sparseSet, maxDoc).build();
    DenseLiveDocs dense = DenseLiveDocs.builder(fixedSet, maxDoc).build();

    // WHEN
    DocIdSetIterator sparseIt = sparse.liveDocsIterator();
    DocIdSetIterator denseIt = dense.liveDocsIterator();
    List<Integer> sparseDocs = collectDocs(sparseIt);
    List<Integer> denseDocs = collectDocs(denseIt);

    // THEN
    assertEquals("Sparse iterator should return all docs", maxDoc, sparseDocs.size());
    assertEquals("Dense iterator should return all docs", maxDoc, denseDocs.size());
    for (int i = 0; i < maxDoc; i++) {
      assertEquals("Sparse doc at position " + i, i, sparseDocs.get(i).intValue());
      assertEquals("Dense doc at position " + i, i, denseDocs.get(i).intValue());
    }
  }

  public void testLiveIteratorAdvance() throws IOException {
    // GIVEN
    final int maxDoc = 1000;
    FixedBitSet fixedSet = new FixedBitSet(maxDoc);
    fixedSet.set(0, maxDoc);
    for (int i = 10; i <= 50; i += 10) {
      fixedSet.clear(i);
    }
    DenseLiveDocs liveDocs = DenseLiveDocs.builder(fixedSet, maxDoc).build();

    // WHEN
    DocIdSetIterator it = liveDocs.liveDocsIterator();

    // THEN
    assertEquals(11, it.advance(10));
    assertEquals(12, it.nextDoc());
    assertEquals(21, it.advance(20));
    assertEquals(500, it.advance(500));
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.advance(1000));
  }

  public void testRandomDeletionsLiveIterator() throws IOException {
    // GIVEN
    final int maxDoc = TestUtil.nextInt(random(), 1000, 10000);
    final double deleteRatio = random().nextDouble() * 0.5;
    SparseFixedBitSet sparseSet = new SparseFixedBitSet(maxDoc);
    FixedBitSet fixedSet = new FixedBitSet(maxDoc);
    fixedSet.set(0, maxDoc);
    List<Integer> expectedLive = new ArrayList<>();
    for (int i = 0; i < maxDoc; i++) {
      if (random().nextDouble() < deleteRatio) {
        sparseSet.set(i);
        fixedSet.clear(i);
      } else {
        expectedLive.add(i);
      }
    }

    // WHEN
    SparseLiveDocs sparse = SparseLiveDocs.builder(sparseSet, maxDoc).build();
    DenseLiveDocs dense = DenseLiveDocs.builder(fixedSet, maxDoc).build();
    List<Integer> sparseLive = collectDocs(sparse.liveDocsIterator());
    List<Integer> denseLive = collectDocs(dense.liveDocsIterator());

    // THEN
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

  public void testSingleDocumentSegment() throws IOException {
    // GIVEN
    final int maxDoc = 1;
    SparseFixedBitSet sparseSet = new SparseFixedBitSet(maxDoc);
    FixedBitSet fixedSet = new FixedBitSet(maxDoc);
    fixedSet.set(0, maxDoc);
    sparseSet.set(0);
    fixedSet.clear(0);

    // WHEN
    SparseLiveDocs sparse = SparseLiveDocs.builder(sparseSet, maxDoc).build();
    DenseLiveDocs dense = DenseLiveDocs.builder(fixedSet, maxDoc).build();
    List<Integer> sparseDeleted = collectDocs(sparse.deletedDocsIterator());
    List<Integer> denseDeleted = collectDocs(dense.deletedDocsIterator());
    List<Integer> sparseLive = collectDocs(sparse.liveDocsIterator());
    List<Integer> denseLive = collectDocs(dense.liveDocsIterator());

    // THEN
    assertFalse("Doc 0 should be deleted", sparse.get(0));
    assertFalse("Doc 0 should be deleted", dense.get(0));
    assertEquals(1, sparse.deletedCount());
    assertEquals(1, dense.deletedCount());
    assertEquals(List.of(0), sparseDeleted);
    assertEquals(List.of(0), denseDeleted);
    assertEquals(0, sparseLive.size());
    assertEquals(0, denseLive.size());
  }

  public void testAllDocumentsDeleted() throws IOException {
    // GIVEN
    final int maxDoc = 100;
    SparseFixedBitSet sparseSet = new SparseFixedBitSet(maxDoc);
    FixedBitSet fixedSet = new FixedBitSet(maxDoc);
    fixedSet.set(0, maxDoc);
    for (int i = 0; i < maxDoc; i++) {
      sparseSet.set(i);
      fixedSet.clear(i);
    }

    // WHEN
    SparseLiveDocs sparse = SparseLiveDocs.builder(sparseSet, maxDoc).build();
    DenseLiveDocs dense = DenseLiveDocs.builder(fixedSet, maxDoc).build();
    List<Integer> sparseDeleted = collectDocs(sparse.deletedDocsIterator());
    List<Integer> denseDeleted = collectDocs(dense.deletedDocsIterator());
    List<Integer> sparseLive = collectDocs(sparse.liveDocsIterator());
    List<Integer> denseLive = collectDocs(dense.liveDocsIterator());

    // THEN
    assertEquals(maxDoc, sparse.deletedCount());
    assertEquals(maxDoc, dense.deletedCount());
    for (int i = 0; i < maxDoc; i++) {
      assertFalse("Doc " + i + " should be deleted", sparse.get(i));
      assertFalse("Doc " + i + " should be deleted", dense.get(i));
    }
    assertEquals(maxDoc, sparseDeleted.size());
    assertEquals(maxDoc, denseDeleted.size());
    for (int i = 0; i < maxDoc; i++) {
      assertEquals(i, sparseDeleted.get(i).intValue());
      assertEquals(i, denseDeleted.get(i).intValue());
    }
    assertEquals(0, sparseLive.size());
    assertEquals(0, denseLive.size());
  }

  public void testLargeSegment() throws IOException {
    // GIVEN
    final int maxDoc = 10_000_000;
    SparseFixedBitSet sparseSet = new SparseFixedBitSet(maxDoc);
    FixedBitSet fixedSet = new FixedBitSet(maxDoc);
    fixedSet.set(0, maxDoc);
    for (int i = 0; i < maxDoc; i += 1000) {
      sparseSet.set(i);
      fixedSet.clear(i);
    }

    // WHEN
    SparseLiveDocs sparse = SparseLiveDocs.builder(sparseSet, maxDoc).build();
    DenseLiveDocs dense = DenseLiveDocs.builder(fixedSet, maxDoc).build();
    DocIdSetIterator sparseIt = sparse.deletedDocsIterator();
    long sparseBytes = sparse.ramBytesUsed();
    long denseBytes = dense.ramBytesUsed();

    // THEN
    assertEquals(10_000, sparse.deletedCount());
    assertEquals(10_000, dense.deletedCount());
    assertEquals(0, sparseIt.nextDoc());
    sparseIt.advance(maxDoc - 1000);
    assertEquals(maxDoc - 1000, sparseIt.docID());
    assertTrue(
        "Sparse should use less memory than dense for 0.1% deletions (sparse="
            + sparseBytes
            + ", dense="
            + denseBytes
            + ")",
        sparseBytes < denseBytes);
    assertTrue(
        "Sparse should use significantly less memory (<50%) for very sparse deletions (ratio="
            + String.format(Locale.ROOT, "%.2f%%", 100.0 * sparseBytes / denseBytes)
            + ")",
        sparseBytes < denseBytes / 2);
  }

  public void testFirstAndLastDocDeletion() throws IOException {
    // GIVEN
    final int maxDoc = 1000;
    SparseFixedBitSet sparseSet = new SparseFixedBitSet(maxDoc);
    FixedBitSet fixedSet = new FixedBitSet(maxDoc);
    fixedSet.set(0, maxDoc);
    sparseSet.set(0);
    sparseSet.set(maxDoc - 1);
    fixedSet.clear(0);
    fixedSet.clear(maxDoc - 1);

    // WHEN
    SparseLiveDocs sparse = SparseLiveDocs.builder(sparseSet, maxDoc).build();
    DenseLiveDocs dense = DenseLiveDocs.builder(fixedSet, maxDoc).build();
    List<Integer> sparseDeleted = collectDocs(sparse.deletedDocsIterator());
    List<Integer> denseDeleted = collectDocs(dense.deletedDocsIterator());
    List<Integer> sparseLive = collectDocs(sparse.liveDocsIterator());
    List<Integer> denseLive = collectDocs(dense.liveDocsIterator());

    // THEN
    assertFalse("First doc should be deleted", sparse.get(0));
    assertFalse("Last doc should be deleted", sparse.get(maxDoc - 1));
    assertFalse("First doc should be deleted", dense.get(0));
    assertFalse("Last doc should be deleted", dense.get(maxDoc - 1));
    assertEquals(2, sparseDeleted.size());
    assertEquals(2, denseDeleted.size());
    assertEquals(0, sparseDeleted.get(0).intValue());
    assertEquals(maxDoc - 1, sparseDeleted.get(1).intValue());
    assertEquals(maxDoc - 2, sparseLive.size());
    assertEquals(maxDoc - 2, denseLive.size());
    assertEquals(1, sparseLive.get(0).intValue());
    assertEquals(maxDoc - 2, sparseLive.get(sparseLive.size() - 1).intValue());
  }
}
