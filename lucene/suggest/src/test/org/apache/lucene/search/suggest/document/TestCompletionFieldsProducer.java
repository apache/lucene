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
package org.apache.lucene.search.suggest.document;

import static org.hamcrest.Matchers.containsString;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.store.BaseDirectoryWrapper;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.hamcrest.MatcherAssert;

public class TestCompletionFieldsProducer extends LuceneTestCase {

  public void testDictionaryIntegrity() throws Exception {
    checkCorruptDictionary(false);
  }

  public void testMergeInstanceDictionaryIntegrity() throws Exception {
    checkCorruptDictionary(true);
  }

  private void checkCorruptDictionary(boolean merging) throws Exception {
    try (BaseDirectoryWrapper dir = newDirectory();
        Analyzer analyzer = new MockAnalyzer(random())) {
      dir.setCheckIndexOnClose(false);
      writeIndex(dir, analyzer);
      String dictionary = null;
      for (String file : dir.listAll()) {
        if (file.endsWith("." + CompletionPostingsFormat.DICT_EXTENSION)) {
          assertNull(dictionary);
          dictionary = file;
        }
      }
      assertNotNull(dictionary);
      byte[] bytes;
      try (IndexInput input = dir.openInput(dictionary, IOContext.DEFAULT)) {
        bytes = new byte[Math.toIntExact(input.length())];
        input.readBytes(bytes, 0, bytes.length);
      }
      // Leave the header and footer intact so opening the reader still succeeds.
      bytes[bytes.length - CodecUtil.footerLength() - 1] ^= 1;
      dir.deleteFile(dictionary);
      try (IndexOutput output = dir.createOutput(dictionary, IOContext.DEFAULT)) {
        output.writeBytes(bytes, bytes.length);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        SegmentReader leaf = (SegmentReader) getOnlyLeafReader(reader);
        FieldsProducer producer = leaf.getPostingsReader();
        if (merging) {
          producer = producer.getMergeInstance();
        }
        FieldsProducer checkedProducer = producer;
        CorruptIndexException exception =
            expectThrows(CorruptIndexException.class, () -> checkedProducer.checkIntegrity(null));
        MatcherAssert.assertThat(exception.getMessage(), containsString("checksum failed"));
      }
    }
  }

  public void testValidDictionaryIntegrity() throws Exception {
    try (BaseDirectoryWrapper dir = newDirectory();
        Analyzer analyzer = new MockAnalyzer(random())) {
      writeIndex(dir, analyzer);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        SegmentReader leaf = (SegmentReader) getOnlyLeafReader(reader);
        leaf.checkIntegrity();
        leaf.getPostingsReader().getMergeInstance().checkIntegrity(null);
        SuggestIndexSearcher searcher = new SuggestIndexSearcher(reader);
        TopSuggestDocs suggestions =
            searcher.suggest(
                new PrefixCompletionQuery(analyzer, new Term("suggest", "sug")), 1, false);
        assertEquals(1, suggestions.scoreLookupDocs().length);
        assertEquals("suggestion", suggestions.scoreLookupDocs()[0].key.toString());
        leaf.checkIntegrity();
      }
    }
  }

  private void writeIndex(BaseDirectoryWrapper dir, Analyzer analyzer) throws Exception {
    try (IndexWriter writer =
        new IndexWriter(
            dir,
            TestSuggestField.iwcWithSuggestField(analyzer, "suggest").setUseCompoundFile(false))) {
      Document document = new Document();
      document.add(new SuggestField("suggest", "suggestion", 7));
      writer.addDocument(document);
    }
  }
}
