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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;

/**
 * Exercises the search path when the composite reader's doc-id space extends beyond {@link
 * Integer#MAX_VALUE}, i.e. a hit's global doc id ({@link ScoreDoc#doc}) is a real {@code long}.
 *
 * <p>Indexing billions of documents is impractical, so this fakes the doc-id space: a few real
 * single-doc segments are wrapped in {@link FilterLeafReader}s that report an inflated {@code
 * maxDoc}. Placed before the "real" leaf in a {@link MultiReader}, they push that leaf's {@link
 * LeafReaderContext#docBase} — and therefore the doc ids of its hits — above {@link
 * Integer#MAX_VALUE}, without any large allocation (a {@link TermQuery} only ever touches the real
 * postings).
 */
public class TestSearchBeyondMaxInt extends LuceneTestCase {

  private static final int NUM_MATCHES = 3;
  private static final int INFLATED = IndexWriter.MAX_DOCS; // ~2.1B; two pads push docBase > 2^31

  /** A leaf reader that claims to hold {@code maxDoc} (all-live) documents but delegates reads. */
  private static final class InflatedLeafReader extends FilterLeafReader {
    private final int inflatedMaxDoc;

    InflatedLeafReader(LeafReader in, int inflatedMaxDoc) {
      super(in);
      this.inflatedMaxDoc = inflatedMaxDoc;
    }

    @Override
    public int maxLeafDoc() {
      return inflatedMaxDoc;
    }

    @Override
    public int numLeafDocs() {
      // claim all inflated docs are live so numDeletedDocs()==0 stays consistent with a null
      // liveDocs
      return inflatedMaxDoc;
    }

    @Override
    public Bits getLiveDocs() {
      return null;
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
      return in.getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return null; // maxDoc no longer matches the delegate: don't advertise as cacheable
    }
  }

  /** A composite reader whose last leaf sits past {@code Integer.MAX_VALUE}, plus its resources. */
  private record BeyondMaxInt(MultiReader reader, DirectoryReader dr, Directory dir, long docBase)
      implements Closeable {
    @Override
    public void close() throws IOException {
      IOUtils.close(reader, dr, dir);
    }
  }

  private BeyondMaxInt buildReaderBeyondMaxInt() throws IOException {
    Directory dir = newDirectory();
    // Two padding segments (no "body" field) and one real segment with the matching docs.
    IndexWriterConfig iwc = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    try (IndexWriter w = new IndexWriter(dir, iwc)) {
      Document pad = new Document();
      pad.add(new StringField("pad", "x", Field.Store.NO));
      w.addDocument(pad);
      w.flush();
      w.addDocument(pad);
      w.flush();
      for (int i = 0; i < NUM_MATCHES; i++) {
        Document doc = new Document();
        doc.add(new StringField("body", "match", Field.Store.NO));
        doc.add(new StoredField("id", i));
        doc.add(new NumericDocValuesField("sort", i));
        w.addDocument(doc);
      }
      w.flush();
    }

    DirectoryReader dr = DirectoryReader.open(dir);
    assertEquals(3, dr.leaves().size());
    // Inflate the two padding leaves so the real leaf's docBase clears Integer.MAX_VALUE.
    List<LeafReader> wrapped = new ArrayList<>();
    wrapped.add(new InflatedLeafReader(dr.leaves().get(0).reader(), INFLATED));
    wrapped.add(new InflatedLeafReader(dr.leaves().get(1).reader(), INFLATED));
    wrapped.add(dr.leaves().get(2).reader()); // real leaf, untouched
    MultiReader reader = new MultiReader(wrapped.toArray(new LeafReader[0]), false);
    assertTrue(reader.maxDoc() > Integer.MAX_VALUE);
    return new BeyondMaxInt(reader, dr, dir, 2L * INFLATED);
  }

  public void testHitsBeyondMaxInt() throws IOException {
    try (BeyondMaxInt b = buildReaderBeyondMaxInt()) {
      MultiReader reader = b.reader();
      long expectedDocBase = b.docBase();
      List<LeafReaderContext> leaves = reader.leaves();
      assertEquals(expectedDocBase + NUM_MATCHES, reader.maxDoc());
      assertEquals(expectedDocBase, leaves.get(2).docBase);

      IndexSearcher searcher = new IndexSearcher(reader);
      TopDocs topDocs = searcher.search(new TermQuery(new Term("body", "match")), NUM_MATCHES);
      assertEquals(NUM_MATCHES, topDocs.totalHits.value());
      assertEquals(NUM_MATCHES, topDocs.scoreDocs.length);

      StoredFields storedFields = reader.storedFields();
      for (int i = 0; i < NUM_MATCHES; i++) {
        ScoreDoc sd = topDocs.scoreDocs[i];
        // A genuine long doc id: docBase (> 2^31) + the leaf-local doc.
        assertTrue("doc id should exceed Integer.MAX_VALUE: " + sd.doc, sd.doc > Integer.MAX_VALUE);
        assertEquals(expectedDocBase + i, sd.doc);
        // Global-doc dispatch: resolve the leaf and read the right stored field back.
        assertEquals(2, ReaderUtil.subIndex(sd.doc, leaves));
        assertEquals(
            Integer.toString(i), storedFields.document(sd.doc).getField("id").numericValue() + "");
      }

      // Paging with a > 2^31 after.doc must work and continue past it.
      TopDocs page1 = searcher.search(new TermQuery(new Term("body", "match")), 1);
      assertEquals(expectedDocBase, page1.scoreDocs[0].doc);
      TopDocs page2 =
          searcher.searchAfter(page1.scoreDocs[0], new TermQuery(new Term("body", "match")), 1);
      assertEquals(1, page2.scoreDocs.length);
      assertEquals(expectedDocBase + 1, page2.scoreDocs[0].doc);

      // explain() must accept the long global doc id.
      Explanation exp =
          searcher.explain(new TermQuery(new Term("body", "match")), expectedDocBase + 1);
      assertTrue(exp.isMatch());
    }
  }

  /** Field sort and {@code _doc} sort must carry long doc ids through {@code searchAfter}. */
  public void testFieldSortBeyondMaxInt() throws IOException {
    try (BeyondMaxInt b = buildReaderBeyondMaxInt()) {
      long expectedDocBase = b.docBase();
      IndexSearcher searcher = new IndexSearcher(b.reader());
      Query query = new TermQuery(new Term("body", "match"));

      for (Sort sort :
          List.of(
              new Sort(new SortField("sort", SortField.Type.LONG)),
              new Sort(SortField.FIELD_DOC))) {
        TopDocs all = searcher.search(query, NUM_MATCHES, sort);
        for (int i = 0; i < NUM_MATCHES; i++) {
          assertEquals(expectedDocBase + i, all.scoreDocs[i].doc);
        }
        // _doc sort exposes the doc id itself as the (long) sort value.
        if (sort.getSort()[0] == SortField.FIELD_DOC) {
          Object value = ((FieldDoc) all.scoreDocs[0]).fields[0];
          assertEquals(Long.valueOf(expectedDocBase), value);
        }

        // Page one hit at a time; every after.doc after the first is > 2^31.
        FieldDoc after = null;
        for (int i = 0; i < NUM_MATCHES; i++) {
          TopDocs page = searcher.searchAfter(after, query, 1, sort);
          assertEquals(1, page.scoreDocs.length);
          assertEquals(expectedDocBase + i, page.scoreDocs[0].doc);
          after = (FieldDoc) page.scoreDocs[0];
        }
      }
    }
  }

  /**
   * The composite-as-one-leaf helpers address the doc space through int APIs, so they must reject a
   * {@code > 2^31} reader with a clear message rather than a bare integer overflow.
   */
  public void testCompositeHelpersRejectBeyondMaxInt() throws IOException {
    try (BeyondMaxInt b = buildReaderBeyondMaxInt()) {
      MultiReader reader = b.reader();
      UnsupportedOperationException terms =
          expectThrows(
              UnsupportedOperationException.class, () -> MultiTerms.getTerms(reader, "body"));
      assertTrue(terms.getMessage(), terms.getMessage().contains("per-leaf"));
      UnsupportedOperationException dv =
          expectThrows(
              UnsupportedOperationException.class,
              () -> MultiDocValues.getNumericValues(reader, "sort"));
      assertTrue(dv.getMessage(), dv.getMessage().contains("per-leaf"));
    }
  }

  /**
   * {@link IndexSearcher#count} returns a {@code long}: a whole-index match count can exceed {@link
   * Integer#MAX_VALUE}. {@link MatchAllDocsQuery} answers via {@link Weight#count} (the inflated
   * leaf sizes), so the count clears 2^31 without ever iterating billions of docs.
   */
  public void testCountBeyondMaxInt() throws IOException {
    try (BeyondMaxInt b = buildReaderBeyondMaxInt()) {
      MultiReader reader = b.reader();
      IndexSearcher searcher = new IndexSearcher(reader);
      long count = searcher.count(new MatchAllDocsQuery());
      assertTrue("count should exceed Integer.MAX_VALUE: " + count, count > Integer.MAX_VALUE);
      // Every doc is live across the inflated leaves and the real one, so the count is maxDoc.
      assertEquals(reader.maxDoc(), count);
    }
  }
}
