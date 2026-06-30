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

import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

public class TestKnownCountDeferredRepro extends LuceneTestCase {

  public void testTermInSetQueryMoreThan16TermsInSegment() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    final int numTerms = 31; // > 16 so the wrapper takes the deferred path
    List<BytesRef> queryTerms = new ArrayList<>();
    for (int i = 0; i < numTerms; i++) {
      String t = "term" + (1000 + i); // fixed-width so lexicographic order is stable
      Document doc = new Document();
      doc.add(newStringField("body", t, Field.Store.NO));
      w.addDocument(doc);
      queryTerms.add(new BytesRef(t));
    }
    w.forceMerge(1); // single segment => all 31 terms match in one segment
    DirectoryReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r);
    // Each term matches exactly one doc, so a correct result == numTerms hits.
    Query q = new TermInSetQuery("body", queryTerms);
    int hits = s.count(q);
    assertEquals("TermInSetQuery dropped a term on the deferred path", numTerms, hits);

    // Cross-check against the ground truth (a plain disjunction of TermQuery).
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    for (BytesRef t : queryTerms) {
      bq.add(new TermQuery(new Term("body", t)), BooleanClause.Occur.SHOULD);
    }
    assertEquals(s.count(bq.build()), hits);

    r.close();
    dir.close();
  }
}
