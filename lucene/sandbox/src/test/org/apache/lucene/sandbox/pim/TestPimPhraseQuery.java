package org.apache.lucene.sandbox.pim;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.QueryUtils;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Tests {@link PimPhraseQuery}.
 */
public class TestPimPhraseQuery extends LuceneTestCase {

  private static IndexSearcher searcher;
  private static IndexReader reader;
  private Query query;
  private static Directory directory;

  @BeforeClass
  public static void beforeClass() throws Exception {
    directory = newDirectory();
    Analyzer analyzer =
      new Analyzer() {
        @Override
        public TokenStreamComponents createComponents(String fieldName) {
          return new TokenStreamComponents(new MockTokenizer(MockTokenizer.WHITESPACE, false));
        }

        @Override
        public int getPositionIncrementGap(String fieldName) {
          return 100;
        }
      };
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory, analyzer);

    Document doc = new Document();
    doc.add(newTextField("field", "one two three four five", Field.Store.YES));
    doc.add(newTextField("repeated", "this is a repeated field - first part", Field.Store.YES));
    Field repeatedField =
      newTextField("repeated", "second part of a repeated field", Field.Store.YES);
    doc.add(repeatedField);
    doc.add(newTextField("palindrome", "one two three two one", Field.Store.YES));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(newTextField("nonexist", "phrase exist notexist exist found", Field.Store.YES));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(newTextField("nonexist", "phrase exist notexist exist found", Field.Store.YES));
    writer.addDocument(doc);

    reader = writer.getReader();
    writer.close();

    searcher = new IndexSearcher(reader);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    searcher = null;
    reader.close();
    reader = null;
    directory.close();
    directory = null;
    PimSystemManager.get().shutDown();
  }

  public void testNotCloseEnough() throws Exception {
    query = new PimPhraseQuery(2, "field", "one", "five");
    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(0, hits.length);
    QueryUtils.check(random(), query, searcher);
  }

  public void testBarelyCloseEnough() throws Exception {
    query = new PimPhraseQuery(3, "field", "one", "five");
    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);
    QueryUtils.check(random(), query, searcher);
  }

  /** Ensures slop of 0 works for exact matches, but not reversed */
  public void testExact() throws Exception {
    // slop is zero by default
    query = new PimPhraseQuery("field", "four", "five");
    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("exact match", 1, hits.length);
    QueryUtils.check(random(), query, searcher);

    query = new PimPhraseQuery("field", "two", "one");
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("reverse not exact", 0, hits.length);
    QueryUtils.check(random(), query, searcher);
  }

  public void testSlop1() throws Exception {
    // Ensures slop of 1 works with terms in order.
    query = new PimPhraseQuery(1, "field", "one", "two");
    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("in order", 1, hits.length);
    QueryUtils.check(random(), query, searcher);

    // Ensures slop of 1 does not work for phrases out of order;
    // must be at least 2.
    query = new PimPhraseQuery(1, "field", "two", "one");
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("reversed, slop not 2 or more", 0, hits.length);
    QueryUtils.check(random(), query, searcher);
  }
}
