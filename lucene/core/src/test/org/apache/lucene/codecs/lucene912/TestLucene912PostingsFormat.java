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
package org.apache.lucene.codecs.lucene912;

import java.io.IOException;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FeatureField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BasePostingsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;

public class TestLucene912PostingsFormat extends BasePostingsFormatTestCase {

  @Override
  protected Codec getCodec() {
    return TestUtil.alwaysPostingsFormat(new Lucene912PostingsFormat());
  }

  public void test1Postings() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(null);
    iwc.setCodec(getCodec());
    IndexWriter iw = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new StringField("", "something", Field.Store.NO));
    iw.addDocument(doc);
    DirectoryReader ir = DirectoryReader.open(iw);
    LeafReader ar = getOnlyLeafReader(ir);
    assertEquals(1, ar.getFieldInfos().size());
    Terms terms = ar.terms("");
    assertNotNull(terms);
    TermsEnum termsEnum = terms.iterator();
    assertNotNull(termsEnum.next());
    assertEquals(termsEnum.term(), new BytesRef("something"));
    PostingsEnum pe = termsEnum.postings(null, PostingsEnum.NONE);
    assertEquals(0, pe.nextDoc());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, pe.nextDoc());
    assertNull(termsEnum.next());
    ir.close();
    iw.close();
    dir.close();
  }

  public void test2Postings() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(null);
    iwc.setCodec(getCodec());
    IndexWriter iw = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new StringField("", "something", Field.Store.NO));
    iw.addDocument(doc);
    iw.addDocument(doc);
    DirectoryReader ir = DirectoryReader.open(iw);
    LeafReader ar = getOnlyLeafReader(ir);
    assertEquals(1, ar.getFieldInfos().size());
    Terms terms = ar.terms("");
    assertNotNull(terms);
    TermsEnum termsEnum = terms.iterator();
    assertNotNull(termsEnum.next());
    assertEquals(termsEnum.term(), new BytesRef("something"));
    PostingsEnum pe = termsEnum.postings(null, PostingsEnum.NONE);
    assertEquals(0, pe.nextDoc());
    assertEquals(1, pe.nextDoc());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, pe.nextDoc());
    assertNull(termsEnum.next());
    ir.close();
    iw.close();
    dir.close();
  }

  public void test130Postings() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(null);
    iwc.setCodec(getCodec());
    IndexWriter iw = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new StringField("", "something", Field.Store.NO));
    for (int i = 0; i < 130; ++i) {
      iw.addDocument(doc);
    }
    DirectoryReader ir = DirectoryReader.open(iw);
    LeafReader ar = getOnlyLeafReader(ir);
    assertEquals(1, ar.getFieldInfos().size());
    Terms terms = ar.terms("");
    assertNotNull(terms);
    TermsEnum termsEnum = terms.iterator();
    assertNotNull(termsEnum.next());
    assertEquals(termsEnum.term(), new BytesRef("something"));
    PostingsEnum pe = termsEnum.postings(null, PostingsEnum.NONE);
    for (int i = 0; i < 130; ++i) {
      assertEquals(i, pe.nextDoc());
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, pe.nextDoc());
    assertNull(termsEnum.next());
    ir.close();
    iw.close();
    dir.close();
  }

  public void test258Postings() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(null);
    iwc.setCodec(getCodec());
    IndexWriter iw = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new StringField("", "something", Field.Store.NO));
    for (int i = 0; i < 258; ++i) {
      iw.addDocument(doc);
    }
    DirectoryReader ir = DirectoryReader.open(iw);
    LeafReader ar = getOnlyLeafReader(ir);
    assertEquals(1, ar.getFieldInfos().size());
    Terms terms = ar.terms("");
    assertNotNull(terms);
    TermsEnum termsEnum = terms.iterator();
    assertNotNull(termsEnum.next());
    assertEquals(termsEnum.term(), new BytesRef("something"));
    PostingsEnum pe = termsEnum.postings(null, PostingsEnum.NONE);
    for (int i = 0; i < 258; ++i) {
      assertEquals(i, pe.nextDoc());
    }
    pe = termsEnum.postings(null, PostingsEnum.NONE);
    assertEquals(257, pe.advance(257));
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, pe.nextDoc());
    assertNull(termsEnum.next());
    ir.close();
    iw.close();
    dir.close();
  }

  public void test1030Postings() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(null);
    iwc.setCodec(getCodec());
    IndexWriter iw = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new StringField("", "something", Field.Store.NO));
    for (int i = 0; i < 1030; ++i) {
      iw.addDocument(doc);
    }
    DirectoryReader ir = DirectoryReader.open(iw);
    LeafReader ar = getOnlyLeafReader(ir);
    assertEquals(1, ar.getFieldInfos().size());
    Terms terms = ar.terms("");
    assertNotNull(terms);
    TermsEnum termsEnum = terms.iterator();
    assertNotNull(termsEnum.next());
    assertEquals(termsEnum.term(), new BytesRef("something"));
    PostingsEnum pe = termsEnum.postings(null, PostingsEnum.NONE);
    for (int i = 0; i < 1030; ++i) {
      assertEquals(i, pe.nextDoc());
    }
    pe = termsEnum.postings(null, PostingsEnum.NONE);
    assertEquals(1029, pe.advance(1029));
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, pe.nextDoc());
    assertNull(termsEnum.next());
    ir.close();
    iw.close();
    dir.close();
  }

  public void test1PostingAndFreq() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(null);
    iwc.setCodec(getCodec());
    IndexWriter iw = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new FeatureField("", "something", 1.5f));
    iw.addDocument(doc);
    DirectoryReader ir = DirectoryReader.open(iw);
    LeafReader ar = getOnlyLeafReader(ir);
    assertEquals(1, ar.getFieldInfos().size());
    Terms terms = ar.terms("");
    assertNotNull(terms);
    TermsEnum termsEnum = terms.iterator();
    assertNotNull(termsEnum.next());
    assertEquals(termsEnum.term(), new BytesRef("something"));
    PostingsEnum pe = termsEnum.postings(null, PostingsEnum.FREQS);
    assertEquals(0, pe.nextDoc());
    assertEquals(1.5f, Float.intBitsToFloat(pe.freq() << 15), 0f);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, pe.nextDoc());
    assertNull(termsEnum.next());
    ir.close();
    iw.close();
    dir.close();
  }

  public void test2PostingAndFreq() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(null);
    iwc.setCodec(getCodec());
    IndexWriter iw = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new FeatureField("", "something", 2f));
    iw.addDocument(doc);
    doc = new Document();
    doc.add(new FeatureField("", "something", 3f));
    iw.addDocument(doc);
    DirectoryReader ir = DirectoryReader.open(iw);
    LeafReader ar = getOnlyLeafReader(ir);
    assertEquals(1, ar.getFieldInfos().size());
    Terms terms = ar.terms("");
    assertNotNull(terms);
    TermsEnum termsEnum = terms.iterator();
    assertNotNull(termsEnum.next());
    assertEquals(termsEnum.term(), new BytesRef("something"));
    PostingsEnum pe = termsEnum.postings(null, PostingsEnum.FREQS);
    assertEquals(0, pe.nextDoc());
    assertEquals(2f, Float.intBitsToFloat(pe.freq() << 15), 0f);
    assertEquals(1, pe.nextDoc());
    assertEquals(3f, Float.intBitsToFloat(pe.freq() << 15), 0f);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, pe.nextDoc());
    assertNull(termsEnum.next());
    ir.close();
    iw.close();
    dir.close();
  }

  public void test130PostingAndFreq() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(null);
    iwc.setCodec(getCodec());
    IndexWriter iw = new IndexWriter(dir, iwc);
    for (int i = 0; i < 130; ++i) {
      Document doc = new Document();
      doc.add(new FeatureField("", "something", i + 1));
      iw.addDocument(doc);
    }
    DirectoryReader ir = DirectoryReader.open(iw);
    LeafReader ar = getOnlyLeafReader(ir);
    assertEquals(1, ar.getFieldInfos().size());
    Terms terms = ar.terms("");
    assertNotNull(terms);
    TermsEnum termsEnum = terms.iterator();
    assertNotNull(termsEnum.next());
    assertEquals(termsEnum.term(), new BytesRef("something"));
    PostingsEnum pe = termsEnum.postings(null, PostingsEnum.FREQS);
    for (int i = 0; i < 130; ++i) {
      assertEquals(i, pe.nextDoc());
      int freq = pe.freq();
      assertEquals(i + 1, Float.intBitsToFloat(freq << 15), 0f);
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, pe.nextDoc());
    pe = termsEnum.postings(null, PostingsEnum.FREQS);
    assertEquals(129, pe.advance(129));
    assertEquals(130f, Float.intBitsToFloat(pe.freq() << 15), 0f);
    assertNull(termsEnum.next());
    ir.close();
    iw.close();
    dir.close();
  }

  public void test10000PostingAndFreqAndPos() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new KeywordAnalyzer());
    iwc.setCodec(getCodec());
    IndexWriter iw = new IndexWriter(dir, iwc);
    for (int i = 0; i < 10000; ++i) {
      Document doc = new Document();
      doc.add(new TextField("", "something", Store.NO));
      iw.addDocument(doc);
    }
    DirectoryReader ir = DirectoryReader.open(iw);
    LeafReader ar = getOnlyLeafReader(ir);
    assertEquals(1, ar.getFieldInfos().size());
    Terms terms = ar.terms("");
    assertNotNull(terms);
    TermsEnum termsEnum = terms.iterator();
    assertNotNull(termsEnum.next());
    assertEquals(termsEnum.term(), new BytesRef("something"));
    PostingsEnum pe = termsEnum.postings(null, PostingsEnum.POSITIONS);
    for (int i = 0; i < 10000; ++i) {
      assertEquals(i, pe.nextDoc());
      assertEquals(1, pe.freq());
      assertEquals(0, pe.nextPosition());
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, pe.nextDoc());
    assertNull(termsEnum.next());
    ir.close();
    iw.close();
    dir.close();
  }

}
