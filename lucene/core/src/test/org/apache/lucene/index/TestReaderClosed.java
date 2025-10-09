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

import java.util.concurrent.RejectedExecutionException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestReaderClosed extends LuceneTestCase {
  private DirectoryReader reader;
  private Directory dir;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    RandomIndexWriter writer =
        new RandomIndexWriter(
            random(),
            dir,
            newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.KEYWORD, false))
                .setMaxBufferedDocs(TestUtil.nextInt(random(), 50, 1000)));

    Document doc = new Document();
    Field field = newStringField("field", "", Field.Store.NO);
    doc.add(field);

    // we generate aweful prefixes: good for testing.
    // but for preflex codec, the test can be very slow, so use less iterations.
    int num = atLeast(10);
    for (int i = 0; i < num; i++) {
      field.setStringValue(TestUtil.randomUnicodeString(random(), 10));
      writer.addDocument(doc);
    }
    writer.forceMerge(1);
    reader = writer.getReader();
    writer.close();
  }

  public void test() throws Exception {
    assertTrue(reader.getRefCount() > 0);
    IndexSearcher searcher = newSearcher(reader);
    TermRangeQuery query = TermRangeQuery.newStringRange("field", "a", "z", true, true);
    searcher.search(query, 5);
    reader.close();
    try {
      searcher.search(query, 5);
    } catch (AlreadyClosedException _) {
      // expected
    } catch (RejectedExecutionException _) {
      // expected if the searcher has been created with threads since LuceneTestCase
      // closes the thread-pool in a reader close listener
    }
  }

  @Override
  public void tearDown() throws Exception {
    dir.close();
    super.tearDown();
  }
}
