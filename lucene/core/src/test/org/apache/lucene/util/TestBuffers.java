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
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestBuffers extends LuceneTestCase {

  public void testSingleValue() throws IOException {
    int maxDoc = random().nextInt(10000) + 10000;
    int currentDoc = 0;
    Buffers buffers = new Buffers(maxDoc, false);
    while (true) {
      int batchsize = 1 + random().nextInt(10);
      if (buffers.ensureBufferCapacity(batchsize) == false) {
        break;
      }
      for (int i = 0; i < batchsize; i++) {
        buffers.addDoc(currentDoc++);
      }
    }
    FixedBitSet fixedBitSet = new FixedBitSet(maxDoc);
    long counter = buffers.toBitSet(fixedBitSet);
    DocIdSetIterator docIdSetIterator = buffers.toDocIdSet().iterator();
    assertEquals(counter, fixedBitSet.cardinality());
    assertEquals(currentDoc, counter);
    for (int i = 0; i < currentDoc; i++) {
      int doc = docIdSetIterator.nextDoc();
      assertEquals(i, doc);
      assertTrue(fixedBitSet.get(doc));
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docIdSetIterator.nextDoc());
  }

  public void testMultiValue() throws IOException {
    int maxDoc = random().nextInt(10000) + 10000;
    int currentDoc = 0;
    int totalDocs = 0;
    Buffers buffers = new Buffers(maxDoc, true);
    while (true) {
      int batchsize = 1 + random().nextInt(10);
      if (buffers.ensureBufferCapacity(batchsize) == false) {
        break;
      }
      for (int i = 0; i < batchsize; i++) {
        buffers.addDoc(currentDoc);
        totalDocs++;
      }
      currentDoc++;
    }
    FixedBitSet fixedBitSet = new FixedBitSet(maxDoc);
    long counter = buffers.toBitSet(fixedBitSet);
    assertEquals(totalDocs, counter);
    DocIdSetIterator docIdSetIterator = buffers.toDocIdSet().iterator();
    for (int i = 0; i < currentDoc; i++) {
      int doc = docIdSetIterator.nextDoc();
      assertEquals(i, doc);
      assertTrue(fixedBitSet.get(doc));
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docIdSetIterator.nextDoc());
  }

  public void testRandomValues() throws IOException {
    int maxDoc = random().nextInt(10000) + 10000;
    int totalDocs = 0;
    Buffers buffers = new Buffers(maxDoc, true);
    while (true) {
      int batchsize = 1 + random().nextInt(10);
      if (buffers.ensureBufferCapacity(batchsize) == false) {
        break;
      }
      for (int i = 0; i < batchsize; i++) {
        int doc = random().nextInt(maxDoc);
        buffers.addDoc(doc);
        maxDoc = Math.max(maxDoc, doc);
        totalDocs++;
      }
    }
    FixedBitSet fixedBitSet = new FixedBitSet(maxDoc + 1);
    long counter = buffers.toBitSet(fixedBitSet);
    DocIdSetIterator docIdSetIterator = buffers.toDocIdSet().iterator();
    assertEquals(totalDocs, counter);
    int iteratedDocs = 0;
    int doc;
    while ((doc = docIdSetIterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      assertTrue(fixedBitSet.get(doc));
      iteratedDocs++;
    }
    assertEquals(iteratedDocs, fixedBitSet.cardinality());
  }
}
