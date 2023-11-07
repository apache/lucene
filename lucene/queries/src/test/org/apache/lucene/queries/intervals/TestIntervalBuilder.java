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

/*
 * Code adopted from ASL-licensed Elasticsearch.
 * https://github.com/elastic/elasticsearch/blob/7.10/server/src/test/java/org/elasticsearch/index/query/IntervalBuilderTests.java
 *
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lucene.queries.intervals;

import java.io.IOException;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.CannedTokenStream;
import org.apache.lucene.tests.analysis.Token;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestIntervalBuilder extends LuceneTestCase {
  public void testSimpleTerm() throws IOException {
    CannedTokenStream ts = new CannedTokenStream(new Token("term1", 1, 2));

    IntervalsSource source = IntervalBuilder.analyzeText(new CachingTokenFilter(ts), -1, true);
    IntervalsSource expected = Intervals.term("term1");

    assertEquals(expected, source);
  }

  public void testOrdered() throws IOException {
    CannedTokenStream ts =
        new CannedTokenStream(
            new Token("term1", 1, 2), new Token("term2", 3, 4), new Token("term3", 5, 6));

    IntervalsSource source = IntervalBuilder.analyzeText(new CachingTokenFilter(ts), -1, true);
    IntervalsSource expected =
        Intervals.ordered(
            Intervals.term("term1"), Intervals.term("term2"), Intervals.term("term3"));

    assertEquals(expected, source);
  }

  public void testUnordered() throws IOException {
    CannedTokenStream ts =
        new CannedTokenStream(
            new Token("term1", 1, 2), new Token("term2", 3, 4), new Token("term3", 5, 6));

    IntervalsSource source = IntervalBuilder.analyzeText(new CachingTokenFilter(ts), -1, false);
    IntervalsSource expected =
        Intervals.unordered(
            Intervals.term("term1"), Intervals.term("term2"), Intervals.term("term3"));

    assertEquals(expected, source);
  }

  public void testPhrase() throws IOException {
    CannedTokenStream ts =
        new CannedTokenStream(
            new Token("term1", 1, 2), new Token("term2", 3, 4), new Token("term3", 5, 6));

    IntervalsSource source = IntervalBuilder.analyzeText(new CachingTokenFilter(ts), 0, true);
    IntervalsSource expected =
        Intervals.phrase(Intervals.term("term1"), Intervals.term("term2"), Intervals.term("term3"));

    assertEquals(expected, source);
  }

  public void testPhraseWithStopword() throws IOException {
    CannedTokenStream ts =
        new CannedTokenStream(new Token("term1", 1, 1, 2), new Token("term3", 2, 5, 6));

    IntervalsSource source = IntervalBuilder.analyzeText(new CachingTokenFilter(ts), 0, true);
    IntervalsSource expected =
        Intervals.phrase(Intervals.term("term1"), Intervals.extend(Intervals.term("term3"), 1, 0));

    assertEquals(expected, source);
  }

  public void testEmptyTokenStream() throws IOException {
    CannedTokenStream ts = new CannedTokenStream();
    IntervalsSource source = IntervalBuilder.analyzeText(new CachingTokenFilter(ts), 0, true);
    assertSame(IntervalBuilder.NO_INTERVALS, source);
  }

  public void testSimpleSynonyms() throws IOException {
    CannedTokenStream ts =
        new CannedTokenStream(
            new Token("term1", 1, 2),
            new Token("term2", 3, 4),
            new Token("term4", 0, 3, 4),
            new Token("term3", 5, 6));

    IntervalsSource source = IntervalBuilder.analyzeText(new CachingTokenFilter(ts), -1, true);
    IntervalsSource expected =
        Intervals.ordered(
            Intervals.term("term1"),
            Intervals.or(Intervals.term("term2"), Intervals.term("term4")),
            Intervals.term("term3"));

    assertEquals(expected, source);
  }

  public void testSimpleSynonymsWithGap() throws IOException {
    // term1 [] term2/term3/term4 term5
    CannedTokenStream ts =
        new CannedTokenStream(
            new Token("term1", 1, 2),
            new Token("term2", 2, 3, 4),
            new Token("term3", 0, 3, 4),
            new Token("term4", 0, 3, 4),
            new Token("term5", 5, 6));

    IntervalsSource source = IntervalBuilder.analyzeText(new CachingTokenFilter(ts), -1, true);
    IntervalsSource expected =
        Intervals.ordered(
            Intervals.term("term1"),
            Intervals.extend(
                Intervals.or(
                    Intervals.term("term2"), Intervals.term("term3"), Intervals.term("term4")),
                1,
                0),
            Intervals.term("term5"));
    assertEquals(expected, source);
  }

  public void testGraphSynonyms() throws IOException {
    // term1 term2:2/term3 term4 term5
    CannedTokenStream ts =
        new CannedTokenStream(
            new Token("term1", 1, 2),
            new Token("term2", 1, 3, 4, 2),
            new Token("term3", 0, 3, 4),
            new Token("term4", 5, 6),
            new Token("term5", 6, 7));

    IntervalsSource source = IntervalBuilder.analyzeText(new CachingTokenFilter(ts), -1, true);
    IntervalsSource expected =
        Intervals.ordered(
            Intervals.term("term1"),
            Intervals.or(Intervals.term("term2"), Intervals.phrase("term3", "term4")),
            Intervals.term("term5"));

    assertEquals(expected, source);
  }

  public void testGraphSynonymsWithGaps() throws IOException {
    // term1 [] term2:4/term3 [] [] term4 term5
    CannedTokenStream ts =
        new CannedTokenStream(
            new Token("term1", 1, 2),
            new Token("term2", 2, 3, 4, 4),
            new Token("term3", 0, 3, 4),
            new Token("term4", 3, 5, 6),
            new Token("term5", 6, 7));

    IntervalsSource source = IntervalBuilder.analyzeText(new CachingTokenFilter(ts), -1, true);
    IntervalsSource expected =
        Intervals.ordered(
            Intervals.term("term1"),
            Intervals.or(
                Intervals.extend(Intervals.term("term2"), 1, 0),
                Intervals.phrase(
                    Intervals.extend(Intervals.term("term3"), 1, 0),
                    Intervals.extend(Intervals.term("term4"), 2, 0))),
            Intervals.term("term5"));

    assertEquals(expected, source);
  }

  public void testGraphTerminatesOnGap() throws IOException {
    // term1 term2:2/term3 term4 [] term5
    CannedTokenStream ts =
        new CannedTokenStream(
            new Token("term1", 1, 2),
            new Token("term2", 1, 2, 3, 2),
            new Token("term3", 0, 2, 3),
            new Token("term4", 2, 3),
            new Token("term5", 2, 6, 7));

    IntervalsSource source = IntervalBuilder.analyzeText(new CachingTokenFilter(ts), -1, true);
    IntervalsSource expected =
        Intervals.ordered(
            Intervals.term("term1"),
            Intervals.or(Intervals.term("term2"), Intervals.phrase("term3", "term4")),
            Intervals.extend(Intervals.term("term5"), 1, 0));
    assertEquals(expected, source);
  }

  public void testEmptyIntervals() throws IOException {
    CannedTokenStream ts = new CannedTokenStream();
    IntervalsSource source = IntervalBuilder.analyzeText(new CachingTokenFilter(ts), -1, true);

    Directory directory = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), directory);
    Document doc = new Document();
    doc.add(newStringField("field", "value", Field.Store.NO));
    w.addDocument(doc);
    w.close();
    IndexReader reader = DirectoryReader.open(directory);
    LeafReaderContext ctx = reader.leaves().get(0);

    {
      IntervalIterator it = source.intervals("field", ctx);
      assertEquals(-1, it.docID());
      it.nextDoc();
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.docID());
    }
    {
      IntervalIterator it = source.intervals("field", ctx);
      assertEquals(-1, it.docID());
      it.advance(1);
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.docID());
    }

    reader.close();
    directory.close();
  }
}
