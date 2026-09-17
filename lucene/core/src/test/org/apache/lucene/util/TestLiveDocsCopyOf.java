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

/** Tests for {@link LiveDocs#toFixedBitSet()} via {@link FixedBitSet#copyOf(Bits)}. */
public class TestLiveDocsCopyOf extends LuceneTestCase {

  public void testDenseLiveDocsCopyOf() {
    int maxDoc = 1000;
    FixedBitSet liveBits = new FixedBitSet(maxDoc);
    liveBits.set(0, maxDoc);
    // Delete some known positions
    liveBits.clear(0);
    liveBits.clear(42);
    liveBits.clear(999);

    DenseLiveDocs dense = DenseLiveDocs.builder(liveBits, maxDoc).build();
    FixedBitSet copy = FixedBitSet.copyOf(dense);

    assertEquals(maxDoc, copy.length());
    for (int i = 0; i < maxDoc; i++) {
      assertEquals("mismatch at doc " + i, dense.get(i), copy.get(i));
    }
  }

  public void testSparseLiveDocsCopyOf() {
    int maxDoc = 1000;
    SparseFixedBitSet deletedDocs = new SparseFixedBitSet(maxDoc);
    deletedDocs.set(0);
    deletedDocs.set(42);
    deletedDocs.set(999);

    SparseLiveDocs sparse = SparseLiveDocs.builder(deletedDocs, maxDoc).build();
    FixedBitSet copy = FixedBitSet.copyOf(sparse);

    assertEquals(maxDoc, copy.length());
    for (int i = 0; i < maxDoc; i++) {
      assertEquals("mismatch at doc " + i, sparse.get(i), copy.get(i));
    }
  }

  public void testRandomized() {
    for (int iter = 0; iter < 50; iter++) {
      int maxDoc = random().nextInt(10_000) + 1;
      double deletionRate = random().nextDouble() * 0.5;
      int numDeleted = (int) (maxDoc * deletionRate);

      // Build both representations
      FixedBitSet liveBits = new FixedBitSet(maxDoc);
      liveBits.set(0, maxDoc);
      SparseFixedBitSet deletedDocs = new SparseFixedBitSet(maxDoc);

      for (int i = 0; i < numDeleted; i++) {
        int docId = random().nextInt(maxDoc);
        liveBits.clear(docId);
        deletedDocs.set(docId);
      }

      DenseLiveDocs dense = DenseLiveDocs.builder(liveBits, maxDoc).build();
      SparseLiveDocs sparse = SparseLiveDocs.builder(deletedDocs, maxDoc).build();

      // Build per-bit reference
      FixedBitSet reference = new FixedBitSet(maxDoc);
      for (int i = 0; i < maxDoc; i++) {
        if (dense.get(i)) {
          reference.set(i);
        }
      }

      FixedBitSet denseCopy = FixedBitSet.copyOf(dense);
      FixedBitSet sparseCopy = FixedBitSet.copyOf(sparse);

      assertEquals(reference, denseCopy);
      assertEquals(reference, sparseCopy);
    }
  }
}
