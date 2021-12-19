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

package org.apache.lucene.queries.payloads;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.spans.SpanCollector;
import org.apache.lucene.queries.spans.SpanNearQuery;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.queries.spans.SpanTermQuery;
import org.apache.lucene.queries.spans.SpanWeight;
import org.apache.lucene.queries.spans.Spans;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockPayloadAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

public class TestPayloadSpanPositions extends LuceneTestCase {

  static class PayloadSpanCollector implements SpanCollector {

    List<BytesRef> payloads = new ArrayList<>();

    @Override
    public void collectLeaf(PostingsEnum postings, int position, Term term) throws IOException {
      if (postings.getPayload() != null) payloads.add(BytesRef.deepCopyOf(postings.getPayload()));
    }

    @Override
    public void reset() {
      payloads.clear();
    }
  }

  public void testPayloadsPos0() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, new MockPayloadAnalyzer());
    Document doc = new Document();
    doc.add(new TextField("content", new StringReader("a a b c d e a f g h i j a b k k")));
    writer.addDocument(doc);

    final IndexReader readerFromWriter = writer.getReader();
    LeafReader r = getOnlyLeafReader(readerFromWriter);

    PostingsEnum tp = r.postings(new Term("content", "a"), PostingsEnum.ALL);

    int count = 0;
    assertTrue(tp.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    // "a" occurs 4 times
    assertEquals(4, tp.freq());
    assertEquals(0, tp.nextPosition());
    assertEquals(1, tp.nextPosition());
    assertEquals(3, tp.nextPosition());
    assertEquals(6, tp.nextPosition());

    // only one doc has "a"
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, tp.nextDoc());

    IndexSearcher is = newSearcher(getOnlyLeafReader(readerFromWriter));

    SpanTermQuery stq1 = new SpanTermQuery(new Term("content", "a"));
    SpanTermQuery stq2 = new SpanTermQuery(new Term("content", "k"));
    SpanQuery[] sqs = {stq1, stq2};
    SpanNearQuery snq = new SpanNearQuery(sqs, 30, false);

    count = 0;
    boolean sawZero = false;
    if (VERBOSE) {
      System.out.println("\ngetPayloadSpans test");
    }
    PayloadSpanCollector collector = new PayloadSpanCollector();
    Spans pspans =
        snq.createWeight(is, ScoreMode.COMPLETE_NO_SCORES, 1f)
            .getSpans(is.getIndexReader().leaves().get(0), SpanWeight.Postings.PAYLOADS);
    while (pspans.nextDoc() != Spans.NO_MORE_DOCS) {
      while (pspans.nextStartPosition() != Spans.NO_MORE_POSITIONS) {
        if (VERBOSE) {
          System.out.println(
              "doc "
                  + pspans.docID()
                  + ": span "
                  + pspans.startPosition()
                  + " to "
                  + pspans.endPosition());
        }
        collector.reset();
        pspans.collect(collector);
        sawZero |= pspans.startPosition() == 0;
        for (BytesRef payload : collector.payloads) {
          count++;
          if (VERBOSE) {
            System.out.println("  payload: " + Term.toString(payload));
          }
        }
      }
    }
    assertTrue(sawZero);
    assertEquals(8, count);

    // System.out.println("\ngetSpans test");
    Spans spans =
        snq.createWeight(is, ScoreMode.COMPLETE_NO_SCORES, 1f)
            .getSpans(is.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    count = 0;
    sawZero = false;
    while (spans.nextDoc() != Spans.NO_MORE_DOCS) {
      while (spans.nextStartPosition() != Spans.NO_MORE_POSITIONS) {
        count++;
        sawZero |= spans.startPosition() == 0;
        // System.out.println(spans.doc() + " - " + spans.start() + " - " +
        // spans.end());
      }
    }
    assertEquals(4, count);
    assertTrue(sawZero);

    writer.close();
    is.getIndexReader().close();
    dir.close();
  }
}
