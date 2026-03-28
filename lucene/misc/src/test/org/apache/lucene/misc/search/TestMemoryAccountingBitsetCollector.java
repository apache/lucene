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

package org.apache.lucene.misc.search;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.misc.CollectorMemoryTracker;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

public class TestMemoryAccountingBitsetCollector extends LuceneTestCase {

  Directory dir;
  IndexReader reader;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    for (int i = 0; i < 1000; i++) {
      Document doc = new Document();
      doc.add(newStringField("field", Integer.toString(i), Field.Store.NO));
      doc.add(newStringField("field2", Boolean.toString(i % 2 == 0), Field.Store.NO));
      doc.add(new SortedDocValuesField("field2", new BytesRef(Boolean.toString(i % 2 == 0))));
      iw.addDocument(doc);
    }
    reader = iw.getReader();
    iw.close();
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    reader.close();
    dir.close();
  }

  public void testMemoryAccountingBitsetCollectorMemoryLimit() throws Exception {
    long perCollectorMemoryLimit = 150;
    CollectorMemoryTracker tracker =
        new CollectorMemoryTracker("testMemoryTracker", perCollectorMemoryLimit);
    MemoryAccountingBitsetCollectorManager bitsetCollectorManager =
        new MemoryAccountingBitsetCollectorManager(tracker);
    IndexSearcher searcher = new IndexSearcher(reader);
    expectThrows(
        IllegalStateException.class,
        () -> searcher.search(MatchAllDocsQuery.INSTANCE, bitsetCollectorManager));
  }

  public void testConcurrentMemoryLimit() throws Exception {
    // For collector with collecting only 1 doc, 80 bytes are required.
    long perCollectorMemoryLimit = 79;
    CollectorMemoryTracker tracker =
        new CollectorMemoryTracker("testMemoryTracker", perCollectorMemoryLimit);
    MemoryAccountingBitsetCollectorManager bitsetCollectorManager =
        new MemoryAccountingBitsetCollectorManager(tracker);
    IndexSearcher searcher = newSearcher(reader);
    expectThrows(
        IllegalStateException.class,
        () -> searcher.search(MatchAllDocsQuery.INSTANCE, bitsetCollectorManager));
  }

  public void testCollectedResult() throws Exception {
    CollectorMemoryTracker tracker =
        new CollectorMemoryTracker("testMemoryTracker", Long.MAX_VALUE);
    MemoryAccountingBitsetCollectorManager bitsetCollectorManager =
        new MemoryAccountingBitsetCollectorManager(tracker);

    IndexSearcher searcher = newSearcher(reader);
    FixedBitSet result = searcher.search(MatchAllDocsQuery.INSTANCE, bitsetCollectorManager);

    assertEquals(1000, result.cardinality());
    for (int i = 0; i < 1000; i++) {
      assertTrue(result.get(i));
    }
  }
}
