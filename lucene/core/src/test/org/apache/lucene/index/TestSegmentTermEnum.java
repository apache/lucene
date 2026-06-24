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
package org.apache.lucene.index;

import static org.apache.lucene.tests.util.TestUtil.alwaysPostingsFormat;
import static org.apache.lucene.tests.util.TestUtil.getDefaultPostingsFormat;

import java.io.IOException;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene103.blocktree.SegmentTermsEnum;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;

public class TestSegmentTermEnum extends LuceneTestCase {

  Directory dir;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
  }

  @Override
  public void tearDown() throws Exception {
    dir.close();
    super.tearDown();
  }

  // TODO: Remove this test case after debugging.
  public void testDeepSubBlock() throws Exception {
    Directory dir = newDirectory();
    // Set minTermBlockSize to 2, maxTermBlockSize to 3, to generate deep subBlock.
    PostingsFormat postingsFormat = getDefaultPostingsFormat(2, 3);

    IndexWriter writer =
        new IndexWriter(dir, newIndexWriterConfig().setCodec(alwaysPostingsFormat(postingsFormat)));
    String[] categories =
        new String[] {
          "regular",
          "request1",
          "request2",
          "request3",
          "request4",
          "rest1",
          "rest2",
          "rest3",
          "rest4"
        };

    for (String category : categories) {
      Document doc = new Document();
      doc.add(newStringField("category", category, Field.Store.YES));
      writer.addDocument(doc);
    }
    writer.forceMerge(1);

    IndexReader reader = DirectoryReader.open(writer);
    SegmentTermsEnum termsEnum =
        (SegmentTermsEnum) (getOnlyLeafReader(reader).terms("category").iterator());

    // Test seekCeil.
    assertEquals(TermsEnum.SeekStatus.FOUND, termsEnum.seekCeil(new BytesRef("regular")));
    assertEquals(TermsEnum.SeekStatus.FOUND, termsEnum.seekCeil(new BytesRef("request1")));
    assertEquals(TermsEnum.SeekStatus.FOUND, termsEnum.seekCeil(new BytesRef("request2")));
    assertEquals(TermsEnum.SeekStatus.FOUND, termsEnum.seekCeil(new BytesRef("request3")));
    assertEquals(TermsEnum.SeekStatus.FOUND, termsEnum.seekCeil(new BytesRef("request4")));
    assertEquals(TermsEnum.SeekStatus.FOUND, termsEnum.seekCeil(new BytesRef("rest1")));
    assertEquals(TermsEnum.SeekStatus.FOUND, termsEnum.seekCeil(new BytesRef("rest2")));
    assertEquals(TermsEnum.SeekStatus.FOUND, termsEnum.seekCeil(new BytesRef("rest3")));
    assertEquals(TermsEnum.SeekStatus.FOUND, termsEnum.seekCeil(new BytesRef("rest4")));

    // Test seekExact.
    assertTrue(termsEnum.seekExact(new BytesRef("regular")));
    assertTrue(termsEnum.seekExact(new BytesRef("request1")));
    assertTrue(termsEnum.seekExact(new BytesRef("request2")));
    assertTrue(termsEnum.seekExact(new BytesRef("request3")));
    assertTrue(termsEnum.seekExact(new BytesRef("request4")));
    assertTrue(termsEnum.seekExact(new BytesRef("rest1")));
    assertTrue(termsEnum.seekExact(new BytesRef("rest2")));
    assertTrue(termsEnum.seekExact(new BytesRef("rest3")));
    assertTrue(termsEnum.seekExact(new BytesRef("rest4")));

    writer.close();
    reader.close();
    dir.close();
  }

  public void testMultiSubBlocksToRoot() throws Exception {
    Directory dir = newDirectory();
    // Set minTermBlockSize to 2, maxTermBlockSize to 3, to generate deep subBlock.
    PostingsFormat postingsFormat = getDefaultPostingsFormat(2, 3);

    IndexWriter writer =
        new IndexWriter(dir, newIndexWriterConfig().setCodec(alwaysPostingsFormat(postingsFormat)));
    String[] categories =
        new String[] {
          "cat1", "cat2", "dog1", "dog2", "eat1", "eat2", "request1", "request2", "rest1", "rest2"
        };

    for (String category : categories) {
      Document doc = new Document();
      doc.add(newStringField("category", category, Field.Store.YES));
      writer.addDocument(doc);
    }

    writer.forceMerge(1);

    IndexReader reader = DirectoryReader.open(writer);
    SegmentTermsEnum termsEnum =
        (SegmentTermsEnum) (getOnlyLeafReader(reader).terms("category").iterator());

    assertEquals(TermsEnum.SeekStatus.FOUND, termsEnum.seekCeil(new BytesRef("cat1")));

    writer.close();
    reader.close();
    dir.close();
  }

  public void testTermEnum() throws IOException {
    IndexWriter writer = null;

    writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));

    // ADD 100 documents with term : aaa
    // add 100 documents with terms: aaa bbb
    // Therefore, term 'aaa' has document frequency of 200 and term 'bbb' 100
    for (int i = 0; i < 100; i++) {
      addDoc(writer, "aaa");
      addDoc(writer, "aaa bbb");
    }

    writer.close();

    // verify document frequency of terms in an multi segment index
    verifyDocFreq();

    // merge segments
    writer =
        new IndexWriter(
            dir, newIndexWriterConfig(new MockAnalyzer(random())).setOpenMode(OpenMode.APPEND));
    writer.forceMerge(1);
    writer.close();

    // verify document frequency of terms in a single segment index
    verifyDocFreq();
  }

  public void testPrevTermAtEnd() throws IOException {
    IndexWriter writer =
        new IndexWriter(
            dir,
            newIndexWriterConfig(new MockAnalyzer(random()))
                .setCodec(TestUtil.alwaysPostingsFormat(TestUtil.getDefaultPostingsFormat())));
    addDoc(writer, "aaa bbb");
    writer.close();
    LeafReader reader = getOnlyLeafReader(DirectoryReader.open(dir));
    TermsEnum terms = reader.terms("content").iterator();
    assertNotNull(terms.next());
    assertEquals("aaa", terms.term().utf8ToString());
    assertNotNull(terms.next());
    long ordB;
    try {
      ordB = terms.ord();
    } catch (UnsupportedOperationException _) {
      // ok -- codec is not required to support ord
      reader.close();
      return;
    }
    assertEquals("bbb", terms.term().utf8ToString());
    assertNull(terms.next());

    terms.seekExact(ordB);
    assertEquals("bbb", terms.term().utf8ToString());
    reader.close();
  }

  private void verifyDocFreq() throws IOException {
    IndexReader reader = DirectoryReader.open(dir);
    TermsEnum termEnum = MultiTerms.getTerms(reader, "content").iterator();

    // create enumeration of all terms
    // go to the first term (aaa)
    termEnum.next();
    // assert that term is 'aaa'
    assertEquals("aaa", termEnum.term().utf8ToString());
    assertEquals(200, termEnum.docFreq());
    // go to the second term (bbb)
    termEnum.next();
    // assert that term is 'bbb'
    assertEquals("bbb", termEnum.term().utf8ToString());
    assertEquals(100, termEnum.docFreq());

    // create enumeration of terms after term 'aaa',
    // including 'aaa'
    termEnum.seekCeil(new BytesRef("aaa"));
    // assert that term is 'aaa'
    assertEquals("aaa", termEnum.term().utf8ToString());
    assertEquals(200, termEnum.docFreq());
    // go to term 'bbb'
    termEnum.next();
    // assert that term is 'bbb'
    assertEquals("bbb", termEnum.term().utf8ToString());
    assertEquals(100, termEnum.docFreq());
    reader.close();
  }

  private void addDoc(IndexWriter writer, String value) throws IOException {
    Document doc = new Document();
    doc.add(newTextField("content", value, Field.Store.NO));
    writer.addDocument(doc);
  }
}
