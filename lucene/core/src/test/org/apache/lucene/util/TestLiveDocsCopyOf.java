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

import org.apache.lucene.tests.util.LuceneTestCase;

/** Tests for {@link FixedBitSet#copyOf(Bits)} on {@link LiveDocs} implementations. */
public class TestLiveDocsCopyOf extends LuceneTestCase {

  public void testDenseLiveDocsCopyOf() {
    int maxDoc = atLeast(1000);
    FixedBitSet liveBits = new FixedBitSet(maxDoc);
    liveBits.set(0, maxDoc);
    // Delete some known positions
    liveBits.clear(0);
    liveBits.clear(42);
    liveBits.clear(maxDoc - 1);

    DenseLiveDocs dense = DenseLiveDocs.builder(liveBits, maxDoc).build();
    FixedBitSet copy = FixedBitSet.copyOf(dense);

    assertEquals(maxDoc, copy.length());
    for (int i = 0; i < maxDoc; i++) {
      assertEquals("mismatch at doc " + i, dense.get(i), copy.get(i));
    }

    // PendingDeletes mutates the copy and publishes it, so it must not share state with the source
    copy.clear(100);
    assertTrue(dense.get(100));
  }

  public void testSparseLiveDocsCopyOf() {
    int maxDoc = atLeast(1000);
    SparseFixedBitSet deletedDocs = new SparseFixedBitSet(maxDoc);
    deletedDocs.set(0);
    deletedDocs.set(42);
    deletedDocs.set(maxDoc - 1);

    SparseLiveDocs sparse = SparseLiveDocs.builder(deletedDocs, maxDoc).build();
    FixedBitSet copy = FixedBitSet.copyOf(sparse);

    assertEquals(maxDoc, copy.length());
    for (int i = 0; i < maxDoc; i++) {
      assertEquals("mismatch at doc " + i, sparse.get(i), copy.get(i));
    }

    copy.clear(100);
    assertTrue(sparse.get(100));
  }

  public void testBoundarySizes() {
    // Word (64) and SparseFixedBitSet block (4096) boundaries
    for (int maxDoc : new int[] {1, 63, 64, 65, 4095, 4096, 4097}) {
      int[] allDocs = new int[maxDoc];
      for (int i = 0; i < maxDoc; i++) {
        allDocs[i] = i;
      }
      assertCopyOf(maxDoc, allDocs);
      assertCopyOf(maxDoc, new int[] {random().nextInt(maxDoc)});
      assertCopyOf(maxDoc, new int[] {maxDoc - 1});
    }
  }

  private static void assertCopyOf(int maxDoc, int[] deleted) {
    FixedBitSet liveBits = new FixedBitSet(maxDoc);
    liveBits.set(0, maxDoc);
    SparseFixedBitSet deletedDocs = new SparseFixedBitSet(maxDoc);
    FixedBitSet reference = new FixedBitSet(maxDoc);
    reference.set(0, maxDoc);
    for (int doc : deleted) {
      liveBits.clear(doc);
      deletedDocs.set(doc);
      reference.clear(doc);
    }

    DenseLiveDocs dense = DenseLiveDocs.builder(liveBits, maxDoc).build();
    SparseLiveDocs sparse = SparseLiveDocs.builder(deletedDocs, maxDoc).build();

    assertEquals(reference, FixedBitSet.copyOf(dense));
    assertEquals(reference, FixedBitSet.copyOf(sparse));
  }

  public void testCopyOfPaddedDenseLiveDocsPreservesLength() {
    int maxDoc = 100;
    FixedBitSet backing = new FixedBitSet(256);
    backing.set(0, maxDoc);
    backing.clear(7);
    DenseLiveDocs dense = DenseLiveDocs.builder(backing, maxDoc).build();
    assertEquals(maxDoc, dense.length());

    FixedBitSet copy = FixedBitSet.copyOf(dense);

    assertEquals(dense.length(), copy.length());
    assertEquals(dense.length() - dense.deletedCount(), copy.cardinality());
  }

  public void testRandomized() {
    int iters = atLeast(50);
    for (int iter = 0; iter < iters; iter++) {
      int maxDoc = random().nextInt(10_000) + 1;
      double deletionRate = random().nextDouble() * 0.5;
      int numDeleted = (int) (maxDoc * deletionRate);

      // Build both representations, plus a reference independent of LiveDocs#get
      FixedBitSet liveBits = new FixedBitSet(maxDoc);
      liveBits.set(0, maxDoc);
      SparseFixedBitSet deletedDocs = new SparseFixedBitSet(maxDoc);
      FixedBitSet reference = new FixedBitSet(maxDoc);
      reference.set(0, maxDoc);

      for (int i = 0; i < numDeleted; i++) {
        int docId = random().nextInt(maxDoc);
        liveBits.clear(docId);
        deletedDocs.set(docId);
        reference.clear(docId);
      }

      DenseLiveDocs dense = DenseLiveDocs.builder(liveBits, maxDoc).build();
      SparseLiveDocs sparse = SparseLiveDocs.builder(deletedDocs, maxDoc).build();

      FixedBitSet denseCopy = FixedBitSet.copyOf(dense);
      FixedBitSet sparseCopy = FixedBitSet.copyOf(sparse);

      assertEquals(reference, denseCopy);
      assertEquals(reference, sparseCopy);
    }
  }
}
