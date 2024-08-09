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
package org.apache.lucene.search.uhighlight;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermVectors;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.junit.Test;

/**
 * Tests highlighting for matters *expressly* relating to term vectors.
 *
 * <p>This test DOES NOT represent all testing for highlighting when term vectors are used. Other
 * tests pick the offset source at random (to include term vectors) and in-effect test term vectors
 * generally.
 */
public class TestUnifiedHighlighterTermVec extends UnifiedHighlighterTestBase {

  public TestUnifiedHighlighterTermVec() {
    super(randomFieldType(random()));
  }

  public void testTermVecButNoPositions1() throws Exception {
    testTermVecButNoPositions("x", "y", "y x", "<b>y</b> <b>x</b>");
  }

  public void testTermVecButNoPositions2() throws Exception {
    testTermVecButNoPositions("y", "x", "y x", "<b>y</b> <b>x</b>");
  }

  public void testTermVecButNoPositions3() throws Exception {
    testTermVecButNoPositions("zzz", "yyy", "zzz yyy", "<b>zzz</b> <b>yyy</b>");
  }

  public void testTermVecButNoPositions4() throws Exception {
    testTermVecButNoPositions("zzz", "yyy", "yyy zzz", "<b>yyy</b> <b>zzz</b>");
  }

  public void testTermVecButNoPositions(String aaa, String bbb, String indexed, String expected)
      throws Exception {
    final FieldType tvNoPosType = new FieldType(TextField.TYPE_STORED);
    tvNoPosType.setStoreTermVectors(true);
    tvNoPosType.setStoreTermVectorOffsets(true);
    tvNoPosType.freeze();

    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Field body = new Field("body", indexed, tvNoPosType);
    Document document = new Document();
    document.add(body);
    iw.addDocument(document);
    try (IndexReader ir = iw.getReader()) {
      iw.close();
      IndexSearcher searcher = newSearcher(ir);
      BooleanQuery query =
          new BooleanQuery.Builder()
              .add(new TermQuery(new Term("body", aaa)), BooleanClause.Occur.MUST)
              .add(new TermQuery(new Term("body", bbb)), BooleanClause.Occur.MUST)
              .build();
      TopDocs topDocs = searcher.search(query, 10);
      assertEquals(1, topDocs.totalHits.value);
      UnifiedHighlighter highlighter = UnifiedHighlighter.builder(searcher, indexAnalyzer).build();
      String[] snippets = highlighter.highlight("body", query, topDocs, 2);
      assertEquals(1, snippets.length);
      assertTrue(snippets[0], snippets[0].contains(expected));
    }
  }

  public void testFetchTermVecsOncePerDoc() throws IOException {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    // Declare some number of fields with random field type; but at least one will have term
    // vectors.
    final int numTvFields = 1 + random().nextInt(3);
    List<String> fields = new ArrayList<>(numTvFields);
    List<FieldType> fieldTypes = new ArrayList<>(numTvFields);
    for (int i = 0; i < numTvFields; i++) {
      fields.add("body" + i);
      fieldTypes.add(randomFieldType(random()));
    }
    // ensure at least one has TVs by setting one randomly to it:
    fieldTypes.set(random().nextInt(fieldTypes.size()), tvType);

    final int numDocs = 1 + random().nextInt(3);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      for (String field : fields) {
        doc.add(new Field(field, "some test text", tvType));
      }
      iw.addDocument(doc);
    }

    // Wrap the reader to ensure we only fetch TVs once per doc
    DirectoryReader originalReader = iw.getReader();
    IndexReader ir = new AssertOnceTermVecDirectoryReader(originalReader);
    iw.close();

    IndexSearcher searcher =
        newSearcher(ir, false); // wrapping the reader messes up our counting logic
    UnifiedHighlighter highlighter = UnifiedHighlighter.builder(searcher, indexAnalyzer).build();
    BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
    for (String field : fields) {
      queryBuilder.add(new TermQuery(new Term(field, "test")), BooleanClause.Occur.MUST);
    }
    BooleanQuery query = queryBuilder.build();
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(numDocs, topDocs.totalHits.value);
    Map<String, String[]> fieldToSnippets =
        highlighter.highlightFields(fields.toArray(new String[numTvFields]), query, topDocs);
    String[] expectedSnippetsByDoc = new String[numDocs];
    Arrays.fill(expectedSnippetsByDoc, "some <b>test</b> text");
    for (String field : fields) {
      assertArrayEquals(expectedSnippetsByDoc, fieldToSnippets.get(field));
    }

    ir.storedFields().document(0); // ensure this works because the ir hasn't been closed
    ir.close();
  }

  private static class AssertOnceTermVecDirectoryReader extends FilterDirectoryReader {
    static final SubReaderWrapper SUB_READER_WRAPPER =
        new SubReaderWrapper() {
          @Override
          public LeafReader wrap(LeafReader reader) {
            return new FilterLeafReader(reader) {
              final BitSet seenDocIDs = new BitSet();

              @Override
              public TermVectors termVectors() throws IOException {
                TermVectors orig = in.termVectors();
                return new TermVectors() {
                  @Override
                  public Fields get(int docID) throws IOException {
                    assertFalse(
                        "Should not request TVs for doc more than once.", seenDocIDs.get(docID));
                    seenDocIDs.set(docID);
                    return orig.get(docID);
                  }
                };
              }

              @Override
              public CacheHelper getCoreCacheHelper() {
                return null;
              }

              @Override
              public CacheHelper getReaderCacheHelper() {
                return null;
              }
            };
          }
        };

    AssertOnceTermVecDirectoryReader(DirectoryReader in) throws IOException {
      super(in, SUB_READER_WRAPPER);
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
      return new AssertOnceTermVecDirectoryReader(in);
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return null;
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUserFailedToIndexOffsets() throws IOException {
    FieldType fieldType = new FieldType(tvType); // note: it's indexed too
    fieldType.setStoreTermVectorPositions(random().nextBoolean());
    fieldType.setStoreTermVectorOffsets(false);

    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);
    Document doc = new Document();
    doc.add(new Field("body", "term vectors", fieldType));
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter.Builder uhBuilder = new UnifiedHighlighter.Builder(searcher, indexAnalyzer);
    UnifiedHighlighter highlighter =
        new UnifiedHighlighter(uhBuilder) {
          @Override
          protected Set<HighlightFlag> getFlags(String field) {
            return Collections.emptySet(); // no WEIGHT_MATCHES
          }
        };
    TermQuery query = new TermQuery(new Term("body", "vectors"));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    try {
      highlighter.highlight("body", query, topDocs, 1); // should throw
    } finally {
      ir.close();
    }
  }
}
