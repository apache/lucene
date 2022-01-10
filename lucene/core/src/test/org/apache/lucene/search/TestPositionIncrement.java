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
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

/** Term position unit test. */
public class TestPositionIncrement extends LuceneTestCase {

  static final boolean VERBOSE = false;

  public void testSetPosition() throws Exception {
    Analyzer analyzer =
        new Analyzer() {
          @Override
          public TokenStreamComponents createComponents(String fieldName) {
            return new TokenStreamComponents(
                new Tokenizer() {
                  // TODO: use CannedTokenStream
                  private final String[] TOKENS = {"1", "2", "3", "4", "5"};
                  private final int[] INCREMENTS = {1, 2, 1, 0, 1};
                  private int i = 0;

                  PositionIncrementAttribute posIncrAtt =
                      addAttribute(PositionIncrementAttribute.class);
                  CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
                  OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

                  @Override
                  public boolean incrementToken() {
                    if (i == TOKENS.length) return false;
                    clearAttributes();
                    termAtt.append(TOKENS[i]);
                    offsetAtt.setOffset(i, i);
                    posIncrAtt.setPositionIncrement(INCREMENTS[i]);
                    i++;
                    return true;
                  }

                  @Override
                  public void reset() throws IOException {
                    super.reset();
                    this.i = 0;
                  }
                });
          }
        };
    Directory store = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), store, analyzer);
    Document d = new Document();
    d.add(newTextField("field", "bogus", Field.Store.YES));
    writer.addDocument(d);
    IndexReader reader = writer.getReader();
    writer.close();

    IndexSearcher searcher = newSearcher(reader);

    PostingsEnum pos =
        MultiTerms.getTermPostingsEnum(searcher.getIndexReader(), "field", new BytesRef("1"));
    pos.nextDoc();
    // first token should be at position 0
    assertEquals(0, pos.nextPosition());

    pos = MultiTerms.getTermPostingsEnum(searcher.getIndexReader(), "field", new BytesRef("2"));
    pos.nextDoc();
    // second token should be at position 2
    assertEquals(2, pos.nextPosition());

    PhraseQuery q;
    ScoreDoc[] hits;

    q = new PhraseQuery("field", "1", "2");
    hits = searcher.search(q, 1000).scoreDocs;
    assertEquals(0, hits.length);

    // same as previous, using the builder with implicit positions
    PhraseQuery.Builder builder = new PhraseQuery.Builder();
    builder.add(new Term("field", "1"));
    builder.add(new Term("field", "2"));
    q = builder.build();
    hits = searcher.search(q, 1000).scoreDocs;
    assertEquals(0, hits.length);

    // same as previous, just specify positions explicitely.
    builder = new PhraseQuery.Builder();
    builder.add(new Term("field", "1"), 0);
    builder.add(new Term("field", "2"), 1);
    q = builder.build();
    hits = searcher.search(q, 1000).scoreDocs;
    assertEquals(0, hits.length);

    // specifying correct positions should find the phrase.
    builder = new PhraseQuery.Builder();
    builder.add(new Term("field", "1"), 0);
    builder.add(new Term("field", "2"), 2);
    q = builder.build();
    hits = searcher.search(q, 1000).scoreDocs;
    assertEquals(1, hits.length);

    q = new PhraseQuery("field", "2", "3");
    hits = searcher.search(q, 1000).scoreDocs;
    assertEquals(1, hits.length);

    q = new PhraseQuery("field", "3", "4");
    hits = searcher.search(q, 1000).scoreDocs;
    assertEquals(0, hits.length);

    // phrase query would find it when correct positions are specified.
    builder = new PhraseQuery.Builder();
    builder.add(new Term("field", "3"), 0);
    builder.add(new Term("field", "4"), 0);
    q = builder.build();
    hits = searcher.search(q, 1000).scoreDocs;
    assertEquals(1, hits.length);

    // phrase query should fail for non existing searched term
    // even if there exist another searched terms in the same searched position.
    builder = new PhraseQuery.Builder();
    builder.add(new Term("field", "3"), 0);
    builder.add(new Term("field", "9"), 0);
    q = builder.build();
    hits = searcher.search(q, 1000).scoreDocs;
    assertEquals(0, hits.length);

    // multi-phrase query should succed for non existing searched term
    // because there exist another searched terms in the same searched position.
    MultiPhraseQuery.Builder mqb = new MultiPhraseQuery.Builder();
    mqb.add(new Term[] {new Term("field", "3"), new Term("field", "9")}, 0);
    hits = searcher.search(mqb.build(), 1000).scoreDocs;
    assertEquals(1, hits.length);

    q = new PhraseQuery("field", "2", "4");
    hits = searcher.search(q, 1000).scoreDocs;
    assertEquals(1, hits.length);

    q = new PhraseQuery("field", "3", "5");
    hits = searcher.search(q, 1000).scoreDocs;
    assertEquals(1, hits.length);

    q = new PhraseQuery("field", "4", "5");
    hits = searcher.search(q, 1000).scoreDocs;
    assertEquals(1, hits.length);

    q = new PhraseQuery("field", "2", "5");
    hits = searcher.search(q, 1000).scoreDocs;
    assertEquals(0, hits.length);

    reader.close();
    store.close();
  }
}
