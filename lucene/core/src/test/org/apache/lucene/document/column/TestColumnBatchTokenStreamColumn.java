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
package org.apache.lucene.document.column;

import static org.apache.lucene.document.column.ColumnBatchTestUtil.ArrayLongColumn;
import static org.apache.lucene.document.column.ColumnBatchTestUtil.ArrayTokenStreamColumn;
import static org.apache.lucene.document.column.ColumnBatchTestUtil.simpleBatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.CannedTokenStream;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.analysis.Token;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

/** Tests for {@link TokenStreamColumn} batch indexing. */
public class TestColumnBatchTokenStreamColumn extends LuceneTestCase {

  private static FieldType invertedType() {
    FieldType type = new FieldType();
    type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    type.setTokenized(true);
    type.freeze();
    return type;
  }

  private static TokenStream canned(String... terms) {
    Token[] tokens = new Token[terms.length];
    int offset = 0;
    for (int i = 0; i < terms.length; i++) {
      tokens[i] = new Token(terms[i], offset, offset + terms[i].length());
      offset += terms[i].length() + 1;
    }
    int finalOffset = terms.length == 0 ? 0 : tokens[terms.length - 1].endOffset();
    return new CannedTokenStream(0, finalOffset, tokens);
  }

  private static List<Integer> positions(LeafReader leaf, String field, String term)
      throws IOException {
    List<Integer> all = new ArrayList<>();
    Terms terms = leaf.terms(field);
    if (terms == null) {
      return all;
    }
    TermsEnum te = terms.iterator();
    if (te.seekExact(new BytesRef(term)) == false) {
      return all;
    }
    PostingsEnum pe = te.postings(null, PostingsEnum.POSITIONS);
    int doc;
    while ((doc = pe.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      int freq = pe.freq();
      for (int i = 0; i < freq; i++) {
        all.add(doc * 1000 + pe.nextPosition());
      }
    }
    return all;
  }

  public void testTokenStreamInversion() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));

    int[] docIds = {0, 1};
    TokenStream[] streams = {canned("quick", "brown", "fox"), canned("lazy", "brown", "dog")};
    w.addBatch(simpleBatch(2, new ArrayTokenStreamColumn("body", invertedType(), docIds, streams)));

    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher s = new IndexSearcher(r);

    assertEquals(2, s.count(new TermQuery(new Term("body", "brown"))));
    assertEquals(1, s.count(new TermQuery(new Term("body", "quick"))));
    assertEquals(1, s.count(new TermQuery(new Term("body", "dog"))));
    assertEquals(0, s.count(new TermQuery(new Term("body", "missing"))));

    // Positions are honored: "quick brown" matches doc 0, "brown quick" does not.
    assertEquals(1, s.count(new PhraseQuery("body", "quick", "brown")));
    assertEquals(0, s.count(new PhraseQuery("body", "brown", "quick")));

    r.close();
    w.close();
    dir.close();
  }

  /** The inverted postings must be identical to adding the same token stream via addDocument. */
  public void testParityWithAddDocument() throws IOException {
    String[][] docs = {{"the", "quick", "brown", "fox"}, {"a", "lazy", "brown", "dog"}};

    Directory batchDir = newDirectory();
    IndexWriter bw = new IndexWriter(batchDir, newIndexWriterConfig(new MockAnalyzer(random())));
    TokenStream[] streams = {canned(docs[0]), canned(docs[1])};
    bw.addBatch(
        simpleBatch(
            2, new ArrayTokenStreamColumn("body", invertedType(), new int[] {0, 1}, streams)));
    DirectoryReader br = DirectoryReader.open(bw);
    LeafReader batchLeaf = getOnlyLeafReader(br);

    Directory docDir = newDirectory();
    IndexWriter dw = new IndexWriter(docDir, newIndexWriterConfig(new MockAnalyzer(random())));
    for (String[] doc : docs) {
      Document d = new Document();
      d.add(new Field("body", canned(doc), invertedType()));
      dw.addDocument(d);
    }
    DirectoryReader dr = DirectoryReader.open(dw);
    LeafReader docLeaf = getOnlyLeafReader(dr);

    for (String term : new String[] {"the", "quick", "brown", "fox", "a", "lazy", "dog"}) {
      assertEquals(
          "positions mismatch for term " + term,
          positions(docLeaf, "body", term),
          positions(batchLeaf, "body", term));
    }

    br.close();
    bw.close();
    batchDir.close();
    dr.close();
    dw.close();
    docDir.close();
  }

  public void testMultiValuedAccumulatesFreq() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));

    // Doc 0 has two values (repeated batch doc-id), doc 1 has one value.
    int[] docIds = {0, 0, 1};
    TokenStream[] streams = {canned("brown", "fox"), canned("brown", "dog"), canned("red", "cat")};
    w.addBatch(simpleBatch(2, new ArrayTokenStreamColumn("body", invertedType(), docIds, streams)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    // "brown" appears once in each of doc 0's two values → freq 2 in doc 0.
    TermsEnum te = leaf.terms("body").iterator();
    assertTrue(te.seekExact(new BytesRef("brown")));
    PostingsEnum pe = te.postings(null, PostingsEnum.FREQS);
    assertEquals(0, pe.nextDoc());
    assertEquals(2, pe.freq());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, pe.nextDoc());

    IndexSearcher s = new IndexSearcher(r);
    assertEquals(1, s.count(new TermQuery(new Term("body", "fox"))));
    assertEquals(1, s.count(new TermQuery(new Term("body", "cat"))));

    r.close();
    w.close();
    dir.close();
  }

  public void testMixedBatchKeepsOtherColumnsColumnar() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));

    TokenStream[] streams = {canned("hello", "world"), canned("goodbye", "world")};
    w.addBatch(
        simpleBatch(
            2,
            new ArrayTokenStreamColumn("body", invertedType(), new int[] {0, 1}, streams),
            new ArrayLongColumn(
                "num", NumericDocValuesField.TYPE, new int[] {0, 1}, new long[] {10, 20})));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    IndexSearcher s = new IndexSearcher(r);

    assertEquals(2, s.count(new TermQuery(new Term("body", "world"))));

    NumericDocValues dv = leaf.getNumericDocValues("num");
    assertEquals(0, dv.nextDoc());
    assertEquals(10, dv.longValue());
    assertEquals(1, dv.nextDoc());
    assertEquals(20, dv.longValue());

    r.close();
    w.close();
    dir.close();
  }

  public void testConstructorRejectsNonIndexed() {
    FieldType type = new FieldType();
    type.setTokenized(true);
    type.freeze();
    expectThrows(
        IllegalArgumentException.class,
        () ->
            new ArrayTokenStreamColumn(
                "body", type, new int[] {0}, new TokenStream[] {canned("x")}));
  }

  public void testConstructorRejectsNonTokenized() {
    FieldType type = new FieldType();
    type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    type.setTokenized(false);
    type.freeze();
    expectThrows(
        IllegalArgumentException.class,
        () ->
            new ArrayTokenStreamColumn(
                "body", type, new int[] {0}, new TokenStream[] {canned("x")}));
  }

  public void testRejectsStored() throws IOException {
    FieldType type = new FieldType();
    type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    type.setTokenized(true);
    type.setStored(true);
    type.freeze();
    assertAddBatchRejected(type);
  }

  public void testRejectsDocValues() throws IOException {
    FieldType type = new FieldType();
    type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    type.setTokenized(true);
    type.setDocValuesType(DocValuesType.SORTED);
    type.freeze();
    assertAddBatchRejected(type);
  }

  public void testRejectsPoints() throws IOException {
    FieldType type = new FieldType();
    type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    type.setTokenized(true);
    type.setDimensions(1, Integer.BYTES);
    type.freeze();
    assertAddBatchRejected(type);
  }

  private void assertAddBatchRejected(FieldType type) throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    expectThrows(
        IllegalArgumentException.class,
        () ->
            w.addBatch(
                simpleBatch(
                    1,
                    new ArrayTokenStreamColumn(
                        "body", type, new int[] {0}, new TokenStream[] {canned("x")}))));
    w.close();
    dir.close();
  }
}
