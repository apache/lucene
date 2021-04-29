package org.apache.lucene.queries.spans;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryRescorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestQueryRescorerWithSpans extends LuceneTestCase {

  private IndexSearcher getSearcher(IndexReader r) {
    IndexSearcher searcher = newSearcher(r);

    // We rely on more tokens = lower score:
    searcher.setSimilarity(new ClassicSimilarity());

    return searcher;
  }

  public void testBasic() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig());

    Document doc = new Document();
    doc.add(newStringField("id", "0", Field.Store.YES));
    doc.add(newTextField("field", "wizard the the the the the oz", Field.Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(newStringField("id", "1", Field.Store.YES));
    // 1 extra token, but wizard and oz are close;
    doc.add(newTextField("field", "wizard oz the the the the the the", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    w.close();

    // Do ordinary BooleanQuery:
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(new TermQuery(new Term("field", "wizard")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("field", "oz")), BooleanClause.Occur.SHOULD);
    IndexSearcher searcher = getSearcher(r);

    TopDocs hits = searcher.search(bq.build(), 10);
    assertEquals(2, hits.totalHits.value);
    assertEquals("0", searcher.doc(hits.scoreDocs[0].doc).get("id"));
    assertEquals("1", searcher.doc(hits.scoreDocs[1].doc).get("id"));

    // Resort using SpanNearQuery:
    SpanTermQuery t1 = new SpanTermQuery(new Term("field", "wizard"));
    SpanTermQuery t2 = new SpanTermQuery(new Term("field", "oz"));
    SpanNearQuery snq = new SpanNearQuery(new SpanQuery[] {t1, t2}, 0, true);

    TopDocs hits3 = QueryRescorer.rescore(searcher, hits, snq, 2.0, 10);

    // Resorting changed the order:
    assertEquals(2, hits3.totalHits.value);
    assertEquals("1", searcher.doc(hits3.scoreDocs[0].doc).get("id"));
    assertEquals("0", searcher.doc(hits3.scoreDocs[1].doc).get("id"));

    r.close();
    dir.close();
  }

  public void testMissingSecondPassScore() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig());

    Document doc = new Document();
    doc.add(newStringField("id", "0", Field.Store.YES));
    doc.add(newTextField("field", "wizard the the the the the oz", Field.Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(newStringField("id", "1", Field.Store.YES));
    // 1 extra token, but wizard and oz are close;
    doc.add(newTextField("field", "wizard oz the the the the the the", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    w.close();

    // Do ordinary BooleanQuery:
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(new TermQuery(new Term("field", "wizard")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("field", "oz")), BooleanClause.Occur.SHOULD);
    IndexSearcher searcher = getSearcher(r);

    TopDocs hits = searcher.search(bq.build(), 10);
    assertEquals(2, hits.totalHits.value);
    assertEquals("0", searcher.doc(hits.scoreDocs[0].doc).get("id"));
    assertEquals("1", searcher.doc(hits.scoreDocs[1].doc).get("id"));

    // Resort using SpanNearQuery:
    SpanTermQuery t1 = new SpanTermQuery(new Term("field", "wizard"));
    SpanTermQuery t2 = new SpanTermQuery(new Term("field", "oz"));
    SpanNearQuery snq = new SpanNearQuery(new SpanQuery[] {t1, t2}, 0, true);

    TopDocs hits3 = QueryRescorer.rescore(searcher, hits, snq, 2.0, 10);

    // Resorting changed the order:
    assertEquals(2, hits3.totalHits.value);
    assertEquals("1", searcher.doc(hits3.scoreDocs[0].doc).get("id"));
    assertEquals("0", searcher.doc(hits3.scoreDocs[1].doc).get("id"));

    r.close();
    dir.close();
  }
}
