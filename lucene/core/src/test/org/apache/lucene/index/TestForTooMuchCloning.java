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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;

public class TestForTooMuchCloning extends LuceneTestCase {

  // Make sure we don't clone IndexInputs too frequently
  // during merging and searching:
  public void test() throws Exception {
    final MockDirectoryWrapper dir = newMockDirectory();
    final TieredMergePolicy tmp = new TieredMergePolicy();
    tmp.setSegmentsPerTier(2);
    final RandomIndexWriter w =
        new RandomIndexWriter(
            random(),
            dir,
            newIndexWriterConfig(new MockAnalyzer(random()))
                // to reduce flakiness on merge clone count
                .setMergeScheduler(new SerialMergeScheduler())
                .setMaxBufferedDocs(2)
                // use a FilterMP otherwise RIW will randomly reconfigure
                // the MP while the test runs
                .setMergePolicy(new FilterMergePolicy(tmp)));
    final int numDocs = 20;
    for (int docs = 0; docs < numDocs; docs++) {
      StringBuilder sb = new StringBuilder();
      for (int terms = 0; terms < 100; terms++) {
        sb.append(TestUtil.randomRealisticUnicodeString(random()));
        sb.append(' ');
      }
      final Document doc = new Document();
      doc.add(new TextField("field", sb.toString(), Field.Store.NO));
      w.addDocument(doc);
    }
    final IndexReader r = w.getReader();
    w.close();
    // System.out.println("merge clone count=" + cloneCount);
    assertTrue(
        "too many calls to IndexInput.clone during merging: " + dir.getInputCloneCount(),
        dir.getInputCloneCount() < 600);

    final IndexSearcher s = newSearcher(r);
    // important: set this after newSearcher, it might have run checkindex
    final int cloneCount = dir.getInputCloneCount();
    // dir.setVerboseClone(true);

    // MTQ that matches all terms so the AUTO_REWRITE should
    // cutover to filter rewrite and reuse a single DocsEnum
    // across all terms;
    final TopDocs hits =
        s.search(
            new TermRangeQuery("field", new BytesRef(), new BytesRef("\uFFFF"), true, true), 10);
    assertTrue(hits.totalHits.value() > 0);
    final int queryCloneCount = dir.getInputCloneCount() - cloneCount;
    // System.out.println("query clone count=" + queryCloneCount);
    // It is rather difficult to reliably predict how many query clone calls will be performed. One
    // important factor is the number of segment partitions being searched, but it depends as well
    // on the terms being indexed, and the distribution of the matches across the documents, which
    // affects how the query gets rewritten and the subsequent number of clone calls it will
    // perform.
    assertTrue(
        "too many calls to IndexInput.clone during TermRangeQuery: " + queryCloneCount,
        queryCloneCount <= Math.max(s.getLeafContexts().size(), s.getSlices().length) * 7);
    r.close();
    dir.close();
  }
}
