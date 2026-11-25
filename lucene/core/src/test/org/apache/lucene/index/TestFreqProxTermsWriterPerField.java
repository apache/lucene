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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;

/**
 * Checks that the in-memory postings written by {@link FreqProxTermsWriterPerField} are flushed
 * correctly for every {@link IndexOptions} layout of {@link
 * FreqProxTermsWriterPerField.FreqProxPostingsArray}.
 */
public class TestFreqProxTermsWriterPerField extends LuceneTestCase {

  private static final IndexOptions[] INDEX_OPTIONS = {
    IndexOptions.DOCS,
    IndexOptions.DOCS_AND_FREQS,
    IndexOptions.DOCS_AND_FREQS_AND_POSITIONS,
    IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS
  };

  /** One occurrence of a term in a document. */
  private record Occurrence(int position, int startOffset, int endOffset) {}

  public void testDocs() throws IOException {
    doTestRandom(IndexOptions.DOCS);
  }

  public void testDocsAndFreqs() throws IOException {
    doTestRandom(IndexOptions.DOCS_AND_FREQS);
  }

  public void testDocsAndFreqsAndPositions() throws IOException {
    doTestRandom(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
  }

  public void testDocsAndFreqsAndPositionsAndOffsets() throws IOException {
    doTestRandom(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
  }

  /** Index several fields with different index options in the same documents. */
  public void testMixedIndexOptions() throws IOException {
    doTestRandom(INDEX_OPTIONS);
  }

  private void doTestRandom(IndexOptions... indexOptions) throws IOException {
    // a small vocabulary makes terms repeat within and across documents, a large one forces the
    // postings array to grow many times
    final int vocabularySize =
        random().nextBoolean()
            ? TestUtil.nextInt(random(), 1, 20)
            : TestUtil.nextInt(random(), 100, 5000);
    final int numDocs = atLeast(200);

    // expected postings: field -> term -> docID -> occurrences
    List<Map<String, Map<Integer, List<Occurrence>>>> expected = new ArrayList<>();
    FieldType[] fieldTypes = new FieldType[indexOptions.length];
    for (int i = 0; i < indexOptions.length; i++) {
      FieldType ft = new FieldType();
      ft.setTokenized(true);
      ft.setIndexOptions(indexOptions[i]);
      ft.freeze();
      fieldTypes[i] = ft;
      expected.add(new TreeMap<>());
    }

    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc =
          new IndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false));
      iwc.setMergePolicy(NoMergePolicy.INSTANCE);
      iwc.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
      iwc.setRAMBufferSizeMB(IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB);
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int docID = 0; docID < numDocs; docID++) {
          Document doc = new Document();
          for (int f = 0; f < indexOptions.length; f++) {
            int numTokens = random().nextInt(30);
            StringBuilder text = new StringBuilder();
            for (int pos = 0; pos < numTokens; pos++) {
              if (pos > 0) {
                text.append(' ');
              }
              String term = "t" + random().nextInt(vocabularySize);
              int startOffset = text.length();
              text.append(term);
              expected
                  .get(f)
                  .computeIfAbsent(term, _ -> new TreeMap<>())
                  .computeIfAbsent(docID, _ -> new ArrayList<>())
                  .add(new Occurrence(pos, startOffset, text.length()));
            }
            doc.add(new Field("f" + f, text.toString(), fieldTypes[f]));
          }
          w.addDocument(doc);
        }
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(1, reader.leaves().size());
        LeafReader leaf = reader.leaves().get(0).reader();
        for (int f = 0; f < indexOptions.length; f++) {
          assertPostings(leaf, "f" + f, indexOptions[f], expected.get(f));
        }
      }
    }
  }

  private static void assertPostings(
      LeafReader leaf,
      String field,
      IndexOptions indexOptions,
      Map<String, Map<Integer, List<Occurrence>>> expected)
      throws IOException {
    boolean hasFreqs = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    boolean hasPositions = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    boolean hasOffsets =
        indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;

    Terms terms = leaf.terms(field);
    if (expected.isEmpty()) {
      assertNull(terms);
      return;
    }
    assertNotNull(terms);
    assertEquals(expected.size(), terms.size());

    TermsEnum termsEnum = terms.iterator();
    PostingsEnum postings = null;
    for (Map.Entry<String, Map<Integer, List<Occurrence>>> termEntry : expected.entrySet()) {
      assertTrue(termsEnum.seekExact(new BytesRef(termEntry.getKey())));
      assertEquals(termEntry.getValue().size(), termsEnum.docFreq());
      postings = termsEnum.postings(postings, PostingsEnum.ALL);
      for (Map.Entry<Integer, List<Occurrence>> docEntry : termEntry.getValue().entrySet()) {
        String msg = "field=" + field + " term=" + termEntry.getKey() + " doc=" + docEntry.getKey();
        assertEquals(msg, docEntry.getKey().intValue(), postings.nextDoc());
        List<Occurrence> occurrences = docEntry.getValue();
        if (hasFreqs) {
          assertEquals(msg, occurrences.size(), postings.freq());
        }
        if (hasPositions) {
          for (Occurrence occurrence : occurrences) {
            assertEquals(msg, occurrence.position(), postings.nextPosition());
            if (hasOffsets) {
              assertEquals(msg, occurrence.startOffset(), postings.startOffset());
              assertEquals(msg, occurrence.endOffset(), postings.endOffset());
            }
          }
        }
      }
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
    }
  }
}
