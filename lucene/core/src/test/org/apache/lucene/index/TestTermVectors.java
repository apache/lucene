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

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;

import java.io.IOException;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.BaseTermVectorsFormatTestCase;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

public class TestTermVectors extends LuceneTestCase {

  private IndexWriter createWriter(Directory dir) throws IOException {
    return new IndexWriter(
        dir, newIndexWriterConfig(new MockAnalyzer(random())).setMaxBufferedDocs(2));
  }

  private void createDir(Directory dir) throws IOException {
    IndexWriter writer = createWriter(dir);
    writer.addDocument(createDoc());
    writer.close();
  }

  private Document createDoc() {
    Document doc = new Document();
    final FieldType ft = new FieldType(TextField.TYPE_STORED);
    ft.setStoreTermVectors(true);
    ft.setStoreTermVectorOffsets(true);
    ft.setStoreTermVectorPositions(true);
    doc.add(newField("c", "aaa", ft));
    return doc;
  }

  private void verifyIndex(Directory dir) throws IOException {
    IndexReader r = DirectoryReader.open(dir);
    TermVectors termVectors = r.termVectors();
    int numDocs = r.numDocs();
    for (int i = 0; i < numDocs; i++) {
      assertNotNull(
          "term vectors should not have been null for document " + i,
          termVectors.get(i).terms("c"));
    }
    r.close();
  }

  public void testFullMergeAddDocs() throws Exception {
    Directory target = newDirectory();
    IndexWriter writer = createWriter(target);
    // with maxBufferedDocs=2, this results in two segments, so that forceMerge
    // actually does something.
    for (int i = 0; i < 4; i++) {
      writer.addDocument(createDoc());
    }
    writer.forceMerge(1);
    writer.close();

    verifyIndex(target);
    target.close();
  }

  public void testFullMergeAddIndexesDir() throws Exception {
    Directory[] input = new Directory[] {newDirectory(), newDirectory()};
    Directory target = newDirectory();

    for (Directory dir : input) {
      createDir(dir);
    }

    IndexWriter writer = createWriter(target);
    writer.addIndexes(input);
    writer.forceMerge(1);
    writer.close();

    verifyIndex(target);

    IOUtils.close(target, input[0], input[1]);
  }

  public void testFullMergeAddIndexesReader() throws Exception {
    Directory[] input = new Directory[] {newDirectory(), newDirectory()};
    Directory target = newDirectory();

    for (Directory dir : input) {
      createDir(dir);
    }

    IndexWriter writer = createWriter(target);
    for (Directory dir : input) {
      DirectoryReader r = DirectoryReader.open(dir);
      TestUtil.addIndexesSlowly(writer, r);
      r.close();
    }
    writer.forceMerge(1);
    writer.close();

    verifyIndex(target);
    IOUtils.close(target, input[0], input[1]);
  }

  /**
   * Assert that a merged segment has payloads set up in fieldInfo, if at least 1 segment has
   * payloads for this field.
   */
  public void testMergeWithPayloads() throws Exception {
    final FieldType ft1 = new FieldType(TextField.TYPE_NOT_STORED);
    ft1.setStoreTermVectors(true);
    ft1.setStoreTermVectorOffsets(true);
    ft1.setStoreTermVectorPositions(true);
    ft1.setStoreTermVectorPayloads(true);
    ft1.freeze();

    final int numDocsInSegment = 10;
    for (boolean hasPayloads : new boolean[] {false, true}) {
      Directory dir = newDirectory();
      IndexWriterConfig indexWriterConfig =
          new IndexWriterConfig(new MockAnalyzer(random())).setMaxBufferedDocs(numDocsInSegment);
      IndexWriter writer = new IndexWriter(dir, indexWriterConfig);
      TokenStreamGenerator tkg1 = new TokenStreamGenerator(hasPayloads);
      TokenStreamGenerator tkg2 = new TokenStreamGenerator(!hasPayloads);

      // create one segment with payloads, and another without payloads
      for (int i = 0; i < numDocsInSegment; i++) {
        Document doc = new Document();
        doc.add(new Field("c", tkg1.newTokenStream(), ft1));
        writer.addDocument(doc);
      }
      for (int i = 0; i < numDocsInSegment; i++) {
        Document doc = new Document();
        doc.add(new Field("c", tkg2.newTokenStream(), ft1));
        writer.addDocument(doc);
      }

      IndexReader reader1 = DirectoryReader.open(writer);
      assertEquals(2, reader1.leaves().size());
      assertEquals(
          hasPayloads,
          reader1.leaves().get(0).reader().getFieldInfos().fieldInfo("c").hasPayloads());
      assertNotEquals(
          hasPayloads,
          reader1.leaves().get(1).reader().getFieldInfos().fieldInfo("c").hasPayloads());

      writer.forceMerge(1);
      IndexReader reader2 = DirectoryReader.open(writer);
      assertEquals(1, reader2.leaves().size());
      // assert that in the merged segments payloads set up for the field
      assertTrue(reader2.leaves().get(0).reader().getFieldInfos().fieldInfo("c").hasPayloads());

      IOUtils.close(writer, reader1, reader2, dir);
    }
  }

  /** A generator for token streams with optional null payloads */
  private static class TokenStreamGenerator {
    private final String[] terms;
    private final BytesRef[] termBytes;
    private final boolean hasPayloads;

    public TokenStreamGenerator(boolean hasPayloads) {
      this.hasPayloads = hasPayloads;
      final int termsCount = 10;
      terms = new String[termsCount];
      termBytes = new BytesRef[termsCount];
      for (int i = 0; i < termsCount; ++i) {
        terms[i] = TestUtil.randomRealisticUnicodeString(random());
        termBytes[i] = new BytesRef(terms[i]);
      }
    }

    public TokenStream newTokenStream() {
      return new OptionalNullPayloadTokenStream(TestUtil.nextInt(random(), 1, 5), terms, termBytes);
    }

    private class OptionalNullPayloadTokenStream
        extends BaseTermVectorsFormatTestCase.RandomTokenStream {
      public OptionalNullPayloadTokenStream(
          int len, String[] sampleTerms, BytesRef[] sampleTermBytes) {
        super(len, sampleTerms, sampleTermBytes);
      }

      @Override
      protected BytesRef randomPayload() {
        if (hasPayloads == false) {
          return null;
        }
        final int len = randomIntBetween(1, 5);
        final BytesRef payload = new BytesRef(len);
        random().nextBytes(payload.bytes);
        payload.length = len;
        return payload;
      }
    }
  }
}
