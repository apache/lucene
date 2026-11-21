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
package org.apache.lucene.codecs.bloom;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.bloom.BloomFilteringPostingsFormat.BloomFilteredFieldsProducer.BloomFilteredTerms;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterLeafReader.FilterTerms;
import org.apache.lucene.index.FilterLeafReader.FilterTermsEnum;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.codecs.bloom.TestBloomFilteredLucenePostings;
import org.apache.lucene.tests.index.BasePostingsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;

/** Basic tests for BloomPostingsFormat */
public class TestBloomPostingsFormat extends BasePostingsFormatTestCase {
  private final Codec codec = TestUtil.alwaysPostingsFormat(new TestBloomFilteredLucenePostings());

  @Override
  protected Codec getCodec() {
    return codec;
  }

  public void testTermStateDoesNotSeekAgain() throws Exception {
    try (Directory directory = newDirectory();
        IndexWriter writer =
            new IndexWriter(
                directory,
                newIndexWriterConfig()
                    .setCodec(
                        TestUtil.alwaysPostingsFormat(TestUtil.getDefaultPostingsFormat())))) {
      for (String text : new String[] {"alpha alpha", "bravo", "alpha"}) {
        Document document = new Document();
        document.add(new TextField("field", text, Field.Store.NO));
        writer.addDocument(document);
      }
      writer.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        AtomicInteger dictionarySeeks = new AtomicInteger();
        Terms delegate =
            new FilterTerms(getOnlyLeafReader(reader).terms("field")) {
              @Override
              public TermsEnum iterator() throws IOException {
                return new FilterTermsEnum(super.iterator()) {
                  @Override
                  public boolean seekExact(BytesRef text) throws IOException {
                    dictionarySeeks.incrementAndGet();
                    return super.seekExact(text);
                  }
                };
              }
            };
        BytesRef alpha = new BytesRef("alpha");
        BytesRef bravo = new BytesRef("bravo");
        FuzzySet filter = FuzzySet.createSetBasedOnMaxMemory(1024);
        filter.addValue(alpha);
        filter.addValue(bravo);
        Terms terms = new BloomFilteredTerms(delegate, filter);
        TermsEnum iterator = terms.iterator();
        assertTrue(iterator.seekExact(alpha));
        TermState alphaState = iterator.termState();
        assertTrue(iterator.seekExact(bravo));
        TermState bravoState = iterator.termState();

        iterator.seekExact(alpha, alphaState);
        assertEquals(alpha, iterator.term());
        assertEquals(2, iterator.docFreq());
        assertEquals(3, iterator.totalTermFreq());
        PostingsEnum postings = iterator.postings(null, PostingsEnum.FREQS);
        assertEquals(0, postings.nextDoc());
        assertEquals(2, postings.freq());
        assertEquals(2, postings.nextDoc());
        assertEquals(1, postings.freq());
        assertEquals(PostingsEnum.NO_MORE_DOCS, postings.nextDoc());
        assertEquals(bravo, iterator.next());
        assertEquals("Restoring a term state must not repeat the lookup", 2, dictionarySeeks.get());

        TermsEnum freshIterator = terms.iterator();
        freshIterator.seekExact(bravo, bravoState);
        assertEquals(bravo, freshIterator.term());
        assertEquals(1, freshIterator.docFreq());
        postings = freshIterator.postings(null, PostingsEnum.NONE);
        assertEquals(1, postings.nextDoc());
        assertEquals(PostingsEnum.NO_MORE_DOCS, postings.nextDoc());
        assertEquals("A fresh iterator must also reuse the saved state", 2, dictionarySeeks.get());
      }
    }
  }
}
