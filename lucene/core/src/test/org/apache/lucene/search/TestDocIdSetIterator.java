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

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.IOUtils;

public class TestDocIdSetIterator extends LuceneTestCase {
  public void testRangeBasic() throws Exception {
    DocIdSetIterator disi = DocIdSetIterator.range(5, 8);
    assertEquals(-1, disi.docID());
    assertEquals(5, disi.nextDoc());
    assertEquals(6, disi.nextDoc());
    assertEquals(7, disi.nextDoc());
    assertEquals(NO_MORE_DOCS, disi.nextDoc());
  }

  public void testInvalidRange() throws Exception {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          DocIdSetIterator.range(5, 4);
        });
  }

  public void testInvalidMin() throws Exception {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          DocIdSetIterator.range(-1, 4);
        });
  }

  public void testEmpty() throws Exception {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          DocIdSetIterator.range(7, 7);
        });
  }

  public void testAdvance() throws Exception {
    DocIdSetIterator disi = DocIdSetIterator.range(5, 20);
    assertEquals(-1, disi.docID());
    assertEquals(5, disi.nextDoc());
    assertEquals(17, disi.advance(17));
    assertEquals(18, disi.nextDoc());
    assertEquals(19, disi.nextDoc());
    assertEquals(NO_MORE_DOCS, disi.nextDoc());
  }

  public void testRandomWithPostings() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    // large number of docs to generate skip data
    int maxDoc = random().nextInt(10000);
    int requiredDocCount = random().nextInt(500, maxDoc);
    Set<Integer> requiredDocSet = new HashSet<>();
    for (int i = 0; i < requiredDocCount; i++) {
      // the set may contain less than requiredDocCount number of doc, but it's ok
      requiredDocSet.add(random().nextInt(maxDoc));
    }

    for (int i = 0; i < maxDoc; i++) {
      Document doc = new Document();

      if (requiredDocSet.contains(i)) {
        doc.add(newTextField("requiredField", "a " + i, Field.Store.NO));
      }

      doc.add(newTextField("otherField", "c", Field.Store.NO));
      w.addDocument(doc);
    }

    w.forceMerge(1);
    w.commit();

    BooleanQuery.Builder b = new BooleanQuery.Builder();
    b.add(new TermQuery(new Term("requiredField", "a")), BooleanClause.Occur.MUST);
    Query q = b.build();

    IndexReader r = DirectoryReader.open(dir);
    IndexSearcher s = new IndexSearcher(r);
    Weight weight = s.createWeight(q, ScoreMode.TOP_SCORES, 1);
    LeafReaderContext context = s.getIndexReader().leaves().get(0);
    DocIdSetIterator disi = weight.scorer(context).iterator();

    int lastDoc = -1;
    int doc = disi.nextDoc();
    int lastNonMatchingDocID = disi.peekNextNonMatchingDocID();

    while (doc != NO_MORE_DOCS) {
      assertTrue(requiredDocSet.contains(doc));

      if (lastDoc == -1) {
        assertTrue(lastNonMatchingDocID > doc);
      } else {
        // if lastDoc is a valid one, then lastDoc, doc and lastNonMatchingDocID must maintain the
        // below invariant
        if (doc < lastNonMatchingDocID) {
          assertTrue(lastDoc + 1 == doc);
        } else if (doc == lastNonMatchingDocID) {
          assertTrue(lastDoc + 1 == doc);
          lastNonMatchingDocID = disi.peekNextNonMatchingDocID();
        } else {
          assertTrue(lastDoc + 1 < doc);
          lastNonMatchingDocID = disi.peekNextNonMatchingDocID();
        }
      }

      lastDoc = doc;
      doc = disi.nextDoc();
    }

    IOUtils.close(r, w, dir);
  }
}
