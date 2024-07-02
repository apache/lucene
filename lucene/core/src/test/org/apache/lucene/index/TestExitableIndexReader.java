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

import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.ExitableIndexReader.TimeExceededException;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;

/** Tests the {@link TestExitableIndexReader}. */
public class TestExitableIndexReader extends LuceneTestCase {

  public void testFuzzyQueryRewriteTimeout() throws IOException {
    Directory directory = newDirectory();
    IndexWriter writer =
        new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
    int n = 10000;
    for (int i = 0; i < n; i++) {
      Document d = new Document();
      d.add(newTextField("abc", "ones", Field.Store.YES));
      writer.addDocument(d);
    }
    writer.forceMerge(1);
    writer.commit();
    writer.close();

    FuzzyQuery query = new FuzzyQuery(new Term("field", "abc"), FuzzyQuery.defaultMaxEdits, 1);

    DirectoryReader directoryReader = DirectoryReader.open(directory);

    expectThrows(
        TimeExceededException.class,
        () ->
            new IndexSearcher(new ExitableIndexReader(directoryReader, countingQueryTimeout(4)))
                .search(query, n));

    directoryReader.close();
    directory.close();
  }

  private static QueryTimeout countingQueryTimeout(int timeAllowed) {

    return new QueryTimeout() {
      static int counter;

      @Override
      public boolean shouldExit() {
        counter++;
        if (counter == timeAllowed) {
          return true;
        }
        return false;
      }
    };
  }
}
