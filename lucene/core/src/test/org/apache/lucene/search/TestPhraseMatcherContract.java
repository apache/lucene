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

import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;

/**
 * Tests the {@link PhraseMatcher} contract, particularly the ability to call {@link
 * PhraseMatcher#maxFreq()} before {@link PhraseMatcher#reset()}.
 */
public class TestPhraseMatcherContract extends LuceneTestCase {

  public void testExactPhraseMatcherContract() throws IOException {
    try (Directory dir = newDirectory()) {
      try (RandomIndexWriter fw = new RandomIndexWriter(random(), dir)) {
        Document doc = new Document();
        doc.add(new TextField("body", "the quick brown fox", Field.Store.NO));
        fw.addDocument(doc);
      }
      try (IndexReader reader = DirectoryReader.open(dir)) {
        LeafReaderContext context = reader.leaves().get(0);
        PhraseQuery.PostingsAndFreq[] postings = new PhraseQuery.PostingsAndFreq[2];

        Term t1 = new Term("body", "quick");
        Term t2 = new Term("body", "brown");

        org.apache.lucene.index.Terms bodyTerms = context.reader().terms("body");
        org.apache.lucene.index.TermsEnum te1 = bodyTerms.iterator();
        te1.seekExact(t1.bytes());
        PostingsEnum pe1 = te1.impacts(PostingsEnum.ALL);

        org.apache.lucene.index.TermsEnum te2 = bodyTerms.iterator();
        te2.seekExact(t2.bytes());
        PostingsEnum pe2 = te2.impacts(PostingsEnum.ALL);

        postings[0] =
            new PhraseQuery.PostingsAndFreq(pe1, (org.apache.lucene.index.ImpactsEnum) pe1, 0, t1);
        postings[1] =
            new PhraseQuery.PostingsAndFreq(pe2, (org.apache.lucene.index.ImpactsEnum) pe2, 1, t2);

        IndexSearcher searcher = new IndexSearcher(reader);
        SimScorer scorer = new BM25Similarity().scorer(1f, searcher.collectionStatistics("body"));
        ExactPhraseMatcher matcher =
            new ExactPhraseMatcher(postings, ScoreMode.TOP_SCORES, scorer, 1f);

        DocIdSetIterator approximation = matcher.approximation();
        int docId = approximation.nextDoc();
        assertNotEquals(DocIdSetIterator.NO_MORE_DOCS, docId);

        // CALL maxFreq BEFORE reset
        float maxFreq = matcher.maxFreq();
        assertTrue(maxFreq >= 1.0f);

        // Then reset
        matcher.reset();
        assertTrue("Should have a match in doc " + docId, matcher.nextMatch());
        assertFalse(matcher.nextMatch());
      }
    }
  }

  public void testSloppyPhraseMatcherContract() throws IOException {
    try (Directory dir = newDirectory()) {
      try (RandomIndexWriter fw = new RandomIndexWriter(random(), dir)) {
        Document doc = new Document();
        doc.add(new TextField("body", "the quick silver fox", Field.Store.NO));
        fw.addDocument(doc);
      }
      try (IndexReader reader = DirectoryReader.open(dir)) {
        LeafReaderContext context = reader.leaves().get(0);
        Term t1 = new Term("body", "quick");
        Term t2 = new Term("body", "fox");
        PhraseQuery.PostingsAndFreq[] postings = new PhraseQuery.PostingsAndFreq[2];

        org.apache.lucene.index.Terms bodyTerms = context.reader().terms("body");
        org.apache.lucene.index.TermsEnum te1 = bodyTerms.iterator();
        te1.seekExact(t1.bytes());
        PostingsEnum pe1 = te1.impacts(PostingsEnum.ALL);

        org.apache.lucene.index.TermsEnum te2 = bodyTerms.iterator();
        te2.seekExact(t2.bytes());
        PostingsEnum pe2 = te2.impacts(PostingsEnum.ALL);

        postings[0] =
            new PhraseQuery.PostingsAndFreq(pe1, (org.apache.lucene.index.ImpactsEnum) pe1, 0, t1);
        postings[1] =
            new PhraseQuery.PostingsAndFreq(pe2, (org.apache.lucene.index.ImpactsEnum) pe2, 1, t2);

        IndexSearcher searcher = new IndexSearcher(reader);
        SimScorer scorer = new BM25Similarity().scorer(1f, searcher.collectionStatistics("body"));
        // distance 2 allows "quick silver fox"
        SloppyPhraseMatcher matcher =
            new SloppyPhraseMatcher(postings, 2, ScoreMode.TOP_SCORES, scorer, 1f, false);

        DocIdSetIterator approximation = matcher.approximation();
        int docId = approximation.nextDoc();
        assertNotEquals(DocIdSetIterator.NO_MORE_DOCS, docId);

        // CALL maxFreq BEFORE reset
        float maxFreq = matcher.maxFreq();
        assertTrue(maxFreq >= 1.0f);

        // Then reset
        matcher.reset();
        assertTrue("Should have a match in doc " + docId, matcher.nextMatch());
        assertFalse(matcher.nextMatch());
      }
    }
  }
}
