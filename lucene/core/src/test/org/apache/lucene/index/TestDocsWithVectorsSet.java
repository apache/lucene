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
package org.apache.lucene.index;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

import java.io.IOException;

public class TestDocsWithVectorsSet extends LuceneTestCase {

  public void testDense() throws IOException {
    DocsWithVectorsSet set = new DocsWithVectorsSet();
    DocIdSetIterator it = set.iterator();
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.nextDoc());

    set.add(0);
    it = set.iterator();
    assertEquals(0, it.nextDoc());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.nextDoc());

    long ramBytesUsed = set.ramBytesUsed();
    for (int i = 1; i < 1000; ++i) {
      set.add(i);
    }
    assertEquals(ramBytesUsed, set.ramBytesUsed());
    it = set.iterator();
    for (int i = 0; i < 1000; ++i) {
      assertEquals(i, it.nextDoc());
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.nextDoc());
  }

  public void testSparse() throws IOException {
    DocsWithVectorsSet set = new DocsWithVectorsSet();
    int doc = random().nextInt(10000);
    set.add(doc);
    DocIdSetIterator it = set.iterator();
    assertEquals(doc, it.nextDoc());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.nextDoc());
    int doc2 = doc + TestUtil.nextInt(random(), 1, 100);
    set.add(doc2);
    it = set.iterator();
    assertEquals(doc, it.nextDoc());
    assertEquals(doc2, it.nextDoc());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.nextDoc());
  }

  public void testSparse_multiValued() throws IOException {
    DocsWithVectorsSet set = new DocsWithVectorsSet();
    int doc1 = random().nextInt(10000);
    int doc1Vectors = 3;
    for(int i=0; i<doc1Vectors; i++){
    set.add(doc1);
    }

    DocIdSetIterator it = set.iterator();
    assertEquals(doc1, it.nextDoc());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.nextDoc());
    assertEquals(doc1Vectors, set.getVectorsCount());
    
    int doc2 = doc1 + TestUtil.nextInt(random(), 1, 100);
    int doc2Vectors = 5;
    for(int i=0; i<doc2Vectors; i++){
      set.add(doc2);
    }
    it = set.iterator();
    assertEquals(doc1, it.nextDoc());
    assertEquals(doc2, it.nextDoc());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.nextDoc());
    assertEquals(doc1Vectors + doc2Vectors, set.getVectorsCount());
    
    int[] valuesPerDocument = set.getVectorsPerDocument();
    assertEquals(doc1Vectors, valuesPerDocument[0]);
    assertEquals(doc2Vectors, valuesPerDocument[1]);
  }

  public void testDenseThenSparse() throws IOException {
    int denseCount = random().nextInt(10000);
    int nextDoc = denseCount + random().nextInt(10000);
    DocsWithVectorsSet set = new DocsWithVectorsSet();
    for (int i = 0; i < denseCount; ++i) {
      set.add(i);
    }
    set.add(nextDoc);
    DocIdSetIterator it = set.iterator();
    for (int i = 0; i < denseCount; ++i) {
      assertEquals(i, it.nextDoc());
    }
    assertEquals(nextDoc, it.nextDoc());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.nextDoc());
  }

  public void testDenseThenSparse_multiValued() throws IOException {
    DocsWithVectorsSet set = new DocsWithVectorsSet();
    int denseCount = random().nextInt(10000);

    for (int i = 0; i < denseCount; i++) {
      set.add(i);
    }
    int doc1 = denseCount + random().nextInt(10000);
    int doc1Vectors = 3;
    for(int i=0; i<doc1Vectors; i++){
      set.add(doc1);
    }

    DocIdSetIterator it = set.iterator();
    for (int docId = 0; docId < denseCount; docId++) {
      assertEquals(docId, it.nextDoc());
    }
    assertEquals(doc1, it.nextDoc());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.nextDoc());
    assertEquals(doc1Vectors + denseCount, set.getVectorsCount());

    int doc2 = doc1 + TestUtil.nextInt(random(), 1, 100);
    int doc2Vectors = 5;
    for(int i=0; i<doc2Vectors; i++){
      set.add(doc2);
    }
    it = set.iterator();
    for (int docId = 0; docId < denseCount; docId++) {
      assertEquals(docId, it.nextDoc());
    }
    assertEquals(doc1, it.nextDoc());
    assertEquals(doc2, it.nextDoc());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.nextDoc());
    assertEquals(denseCount + doc1Vectors + doc2Vectors, set.getVectorsCount());

    int[] valuesPerDocument = set.getVectorsPerDocument();
    for(int i=0; i<denseCount;i++){
      assertEquals(1, valuesPerDocument[i]);

    }
    assertEquals(doc1Vectors, valuesPerDocument[denseCount]);
    assertEquals(doc2Vectors, valuesPerDocument[denseCount+1]);
  }
}
