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

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

public class TestAcceptDocs extends LuceneTestCase {

  public void testValidation() {
    // iterator supplier must be non-null
    expectThrows(NullPointerException.class, () -> AcceptDocs.fromIteratorSupplier(null, null, 1));

    // iterator supplier may not produce null iterators
    expectThrows(
        NullPointerException.class,
        () -> AcceptDocs.fromIteratorSupplier(() -> null, null, 1).iterator());

    // Bits length != maxDoc
    expectThrows(
        IllegalArgumentException.class, () -> AcceptDocs.fromLiveDocs(new Bits.MatchNoBits(3), 4));
  }

  public void testIteratorIgnoresDeletedDocs() throws IOException {
    int maxDoc = 5;
    int deletedDoc = 3;
    FixedBitSet liveDocs = new FixedBitSet(maxDoc);
    liveDocs.set(0, liveDocs.length());
    liveDocs.clear(deletedDoc);

    Bits liveDocsBits = liveDocs.asReadOnlyBits();

    AcceptDocs bitsAcceptDocs = AcceptDocs.fromLiveDocs(liveDocsBits, maxDoc);
    AcceptDocs iteratorAcceptDocs =
        AcceptDocs.fromIteratorSupplier(() -> DocIdSetIterator.all(maxDoc), liveDocsBits, maxDoc);

    for (AcceptDocs acceptDocs : Arrays.asList(bitsAcceptDocs, iteratorAcceptDocs)) {
      Bits acceptBits = acceptDocs.bits();
      assertEquals(maxDoc, acceptBits.length());
      for (int i = 0; i < maxDoc; ++i) {
        assertEquals(i != deletedDoc, acceptBits.get(i));
      }

      DocIdSetIterator iterator = acceptDocs.iterator();
      for (int i = 0; i < maxDoc; ++i) {
        if (i != deletedDoc) {
          assertEquals(i, iterator.nextDoc());
        }
      }
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.nextDoc());
    }
  }

  public void testIteratorIsNew() throws IOException {
    int maxDoc = 5;
    AcceptDocs bitsAcceptDocs = AcceptDocs.fromLiveDocs(null, maxDoc);
    AcceptDocs iteratorAcceptDocs =
        AcceptDocs.fromIteratorSupplier(() -> DocIdSetIterator.all(maxDoc), null, maxDoc);

    for (AcceptDocs acceptDocs : Arrays.asList(bitsAcceptDocs, iteratorAcceptDocs)) {
      DocIdSetIterator iterator = acceptDocs.iterator();
      assertEquals(-1, iterator.docID());
      iterator.nextDoc();
      iterator = acceptDocs.iterator();
      assertEquals(-1, iterator.docID());

      // Triggers lazy loading of matches into a bit set when created from an iterator
      acceptDocs.bits();

      iterator = acceptDocs.iterator();
      assertEquals(-1, iterator.docID());
      iterator.nextDoc();
      iterator = acceptDocs.iterator();
      assertEquals(-1, iterator.docID());
    }
  }
}
