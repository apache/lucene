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

import static org.apache.lucene.tests.util.TestUtil.alwaysPostingsFormat;
import static org.apache.lucene.tests.util.TestUtil.getDefaultPostingsFormat;
import static org.apache.lucene.util.automaton.Operations.DEFAULT_DETERMINIZE_WORK_LIMIT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.Rethrow;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.tests.util.automaton.AutomatonTestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;

public class TestAutomatonQuery extends LuceneTestCase {
  private Directory directory;
  private IndexReader reader;
  private IndexSearcher searcher;

  private static final String FN = "field";

  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    Document doc = new Document();
    Field titleField = newTextField("title", "some title", Field.Store.NO);
    Field field = newTextField(FN, "this is document one 2345", Field.Store.NO);
    Field footerField = newTextField("footer", "a footer", Field.Store.NO);
    doc.add(titleField);
    doc.add(field);
    doc.add(footerField);
    writer.addDocument(doc);
    field.setStringValue("some text from doc two a short piece 5678.91");
    writer.addDocument(doc);
    field.setStringValue(
        "doc three has some different stuff" + " with numbers 1234 5678.9 and letter b");
    writer.addDocument(doc);
    reader = writer.getReader();
    searcher = newSearcher(reader);
    writer.close();
  }

  public void testSubBlock() throws Exception {
    Directory dir = newDirectory();
    // Set minTermBlockSize to 2, maxTermBlockSize to 3, to generate subBlock.
    PostingsFormat postingsFormat = getDefaultPostingsFormat(2, 3);

    IndexWriter writer =
        new IndexWriter(dir, newIndexWriterConfig().setCodec(alwaysPostingsFormat(postingsFormat)));
    String[] categories = new String[] {"regular", "request", "rest", "teacher", "team"};

    for (String category : categories) {
      Document doc = new Document();
      doc.add(newStringField("category", category, Field.Store.YES));
      writer.addDocument(doc);
    }

    IndexReader reader = DirectoryReader.open(writer);
    Automaton automaton =
        Automata.makeBinaryInterval(newBytesRef("reg"), true, newBytesRef("rest"), true);
    AutomatonQuery query = new AutomatonQuery(new Term("category"), automaton, true);
    IndexSearcher searcher = newSearcher(reader);
    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(3, hits.length);

    automaton = Automata.makeBinaryInterval(newBytesRef("tea"), true, newBytesRef("team"), true);
    query = new AutomatonQuery(new Term("category"), automaton, true);
    searcher = newSearcher(reader);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(2, hits.length);

    writer.close();
    reader.close();
    dir.close();
  }

  public void testDeepSubBlock() throws Exception {
    Directory dir = newDirectory();
    // Set minTermBlockSize to 2, maxTermBlockSize to 3, to generate deep subBlock.
    PostingsFormat postingsFormat = getDefaultPostingsFormat(2, 3);

    IndexWriter writer =
        new IndexWriter(dir, newIndexWriterConfig().setCodec(alwaysPostingsFormat(postingsFormat)));
    String[] categories =
        new String[] {
          "regular", "request1", "request2", "request3", "request4", "rest", "teacher", "team"
        };

    for (String category : categories) {
      Document doc = new Document();
      doc.add(newStringField("category", category, Field.Store.YES));
      writer.addDocument(doc);
    }

    IndexReader reader = DirectoryReader.open(writer);
    Automaton automaton =
        Automata.makeBinaryInterval(newBytesRef("reg"), true, newBytesRef("rest"), true);
    AutomatonQuery query = new AutomatonQuery(new Term("category"), automaton);
    IndexSearcher searcher = newSearcher(reader);
    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(6, hits.length);

    automaton = Automata.makeBinaryInterval(newBytesRef("tea"), true, newBytesRef("team"), true);
    query = new AutomatonQuery(new Term("category"), automaton);
    searcher = newSearcher(reader);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(2, hits.length);

    writer.close();
    reader.close();
    dir.close();
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    directory.close();
    super.tearDown();
  }

  private Term newTerm(String value) {
    return new Term(FN, value);
  }

  private long automatonQueryNrHits(AutomatonQuery query) throws IOException {
    if (VERBOSE) {
      System.out.println("TEST: run aq=" + query);
    }
    return searcher.search(query, 5).totalHits.value();
  }

  private void assertAutomatonHits(int expected, Automaton automaton) throws IOException {
    assertEquals(
        expected,
        automatonQueryNrHits(
            new AutomatonQuery(
                newTerm("bogus"), automaton, false, MultiTermQuery.SCORING_BOOLEAN_REWRITE)));
    assertEquals(
        expected,
        automatonQueryNrHits(
            new AutomatonQuery(
                newTerm("bogus"), automaton, false, MultiTermQuery.CONSTANT_SCORE_REWRITE)));
    assertEquals(
        expected,
        automatonQueryNrHits(
            new AutomatonQuery(
                newTerm("bogus"),
                automaton,
                false,
                MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE)));
    assertEquals(
        expected,
        automatonQueryNrHits(
            new AutomatonQuery(
                newTerm("bogus"),
                automaton,
                false,
                MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE)));
  }

  /** Test some very simple automata. */
  public void testAutomata() throws IOException {
    assertAutomatonHits(0, Automata.makeEmpty());
    assertAutomatonHits(0, Automata.makeEmptyString());
    assertAutomatonHits(2, Automata.makeAnyChar());
    assertAutomatonHits(3, Automata.makeAnyString());
    assertAutomatonHits(2, Automata.makeString("doc"));
    assertAutomatonHits(1, Automata.makeChar('a'));
    assertAutomatonHits(2, Automata.makeCharRange('a', 'b'));
    assertAutomatonHits(2, Automata.makeDecimalInterval(1233, 2346, 0));
    assertAutomatonHits(
        1,
        Operations.determinize(
            Automata.makeDecimalInterval(0, 2000, 0), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT));
    assertAutomatonHits(
        2, Operations.union(List.of(Automata.makeChar('a'), Automata.makeChar('b'))));
    assertAutomatonHits(0, Operations.intersection(Automata.makeChar('a'), Automata.makeChar('b')));
    assertAutomatonHits(
        1,
        Operations.minus(
            Automata.makeCharRange('a', 'b'),
            Automata.makeChar('a'),
            DEFAULT_DETERMINIZE_WORK_LIMIT));
  }

  public void testEquals() {
    AutomatonQuery a1 = new AutomatonQuery(newTerm("foobar"), Automata.makeString("foobar"));
    // reference to a1
    AutomatonQuery a2 = a1;
    // same as a1 (accepts the same language, same term)
    AutomatonQuery a3 =
        new AutomatonQuery(
            newTerm("foobar"),
            Operations.concatenate(
                List.of(Automata.makeString("foo"), Automata.makeString("bar"))));
    // different than a1 (same term, but different language)
    AutomatonQuery a4 = new AutomatonQuery(newTerm("foobar"), Automata.makeString("different"));
    // different than a1 (different term, same language)
    AutomatonQuery a5 = new AutomatonQuery(newTerm("blah"), Automata.makeString("foobar"));

    assertEquals(a1.hashCode(), a2.hashCode());
    assertEquals(a1, a2);

    assertEquals(a1.hashCode(), a3.hashCode());
    assertEquals(a1, a3);

    // different class
    AutomatonQuery w1 = new WildcardQuery(newTerm("foobar"));
    // different class
    AutomatonQuery w2 = new RegexpQuery(newTerm("foobar"));

    assertFalse(a1.equals(w1));
    assertFalse(a1.equals(w2));
    assertFalse(w1.equals(w2));
    assertFalse(a1.equals(a4));
    assertFalse(a1.equals(a5));
    assertFalse(a1.equals(null));
  }

  /** Test that rewriting to a single term works as expected, preserves MultiTermQuery semantics. */
  public void testRewriteSingleTerm() throws IOException {
    AutomatonQuery aq = new AutomatonQuery(newTerm("bogus"), Automata.makeString("piece"));
    Terms terms = MultiTerms.getTerms(searcher.getIndexReader(), FN);
    assertTrue(aq.getTermsEnum(terms) instanceof SingleTermsEnum);
    assertEquals(1, automatonQueryNrHits(aq));
  }

  /**
   * Test that rewriting to a prefix query works as expected, preserves MultiTermQuery semantics.
   */
  public void testRewritePrefix() throws IOException {
    Automaton pfx = Automata.makeString("do");
    Automaton prefixAutomaton = Operations.concatenate(List.of(pfx, Automata.makeAnyString()));
    AutomatonQuery aq = new AutomatonQuery(newTerm("bogus"), prefixAutomaton);
    assertEquals(3, automatonQueryNrHits(aq));
  }

  /** Test handling of the empty language */
  public void testEmptyOptimization() throws IOException {
    AutomatonQuery aq = new AutomatonQuery(newTerm("bogus"), Automata.makeEmpty());
    // not yet available: assertTrue(aq.getEnum(searcher.getIndexReader())
    // instanceof EmptyTermEnum);
    Terms terms = MultiTerms.getTerms(searcher.getIndexReader(), FN);
    assertSame(TermsEnum.EMPTY, aq.getTermsEnum(terms));
    assertEquals(0, automatonQueryNrHits(aq));
  }

  public void testHashCodeWithThreads() throws Exception {
    final AutomatonQuery[] queries = new AutomatonQuery[atLeast(100)];
    for (int i = 0; i < queries.length; i++) {
      queries[i] =
          new AutomatonQuery(
              new Term("bogus", "bogus"),
              Operations.determinize(
                  AutomatonTestUtil.randomAutomaton(random()),
                  Operations.DEFAULT_DETERMINIZE_WORK_LIMIT));
    }
    final CountDownLatch startingGun = new CountDownLatch(1);
    int numThreads = TestUtil.nextInt(random(), 2, 5);
    Thread[] threads = new Thread[numThreads];
    for (int threadID = 0; threadID < numThreads; threadID++) {
      Thread thread =
          new Thread() {
            @Override
            public void run() {
              try {
                startingGun.await();
                for (int i = 0; i < queries.length; i++) {
                  queries[i].hashCode();
                }
              } catch (Exception e) {
                Rethrow.rethrow(e);
              }
            }
          };
      threads[threadID] = thread;
      thread.start();
    }
    startingGun.countDown();
    for (Thread thread : threads) {
      thread.join();
    }
  }

  public void testBiggishAutomaton() {
    int numTerms = TEST_NIGHTLY ? 3000 : 500;
    List<BytesRef> terms = new ArrayList<>();
    while (terms.size() < numTerms) {
      terms.add(new BytesRef(TestUtil.randomUnicodeString(random())));
    }
    Collections.sort(terms);
    new AutomatonQuery(new Term("foo", "bar"), Automata.makeStringUnion(terms));
  }
}
