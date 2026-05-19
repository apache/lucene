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
package org.apache.lucene.search.join;

import static org.apache.lucene.search.ScoreMode.TOP_SCORES;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BitSet;

public class TestBlockJoinScorer extends LuceneTestCase {
  public void testScoreNone() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w =
        new RandomIndexWriter(
            random(),
            dir,
            newIndexWriterConfig()
                .setMergePolicy(
                    // retain doc id order
                    newLogMergePolicy()));
    w.w.getConfig().getCodec().compoundFormat().setShouldUseCompoundFile(random().nextBoolean());
    List<Document> docs = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      docs.clear();
      for (int j = 0; j < i; j++) {
        Document child = new Document();
        child.add(newStringField("value", Integer.toString(j), Field.Store.YES));
        docs.add(child);
      }
      Document parent = new Document();
      parent.add(newStringField("docType", "parent", Field.Store.NO));
      parent.add(newStringField("value", Integer.toString(i), Field.Store.YES));
      docs.add(parent);
      w.addDocuments(docs);
    }
    w.forceMerge(1);

    IndexReader reader = w.getReader();
    w.close();
    IndexSearcher searcher = newSearcher(reader);

    // Create a filter that defines "parent" documents in the index - in this case resumes
    BitSetProducer parentsFilter =
        new QueryBitSetProducer(new TermQuery(new Term("docType", "parent")));
    CheckJoinIndex.check(reader, parentsFilter);

    Query childQuery = MatchAllDocsQuery.INSTANCE;
    ToParentBlockJoinQuery query =
        new ToParentBlockJoinQuery(childQuery, parentsFilter, ScoreMode.None);

    Weight weight = searcher.createWeight(searcher.rewrite(query), TOP_SCORES, 1);
    LeafReaderContext context = searcher.getIndexReader().leaves().get(0);

    Scorer scorer = weight.scorer(context);
    BitSet bits = parentsFilter.getBitSet(reader.leaves().get(0));
    int parent = 0;
    for (int i = 0; i < 9; i++) {
      parent = bits.nextSetBit(parent + 1);
      assertEquals(parent, scorer.iterator().nextDoc());
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

    ScorerSupplier ss = weight.scorerSupplier(context);
    ss.setTopLevelScoringClause();
    scorer = ss.get(Long.MAX_VALUE);
    scorer.setMinCompetitiveScore(0f);
    parent = 0;
    for (int i = 0; i < 9; i++) {
      parent = bits.nextSetBit(parent + 1);
      assertEquals(parent, scorer.iterator().nextDoc());
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

    ss = weight.scorerSupplier(context);
    ss.setTopLevelScoringClause();
    scorer = ss.get(Long.MAX_VALUE);
    scorer.setMinCompetitiveScore(Math.nextUp(0f));
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

    ss = weight.scorerSupplier(context);
    ss.setTopLevelScoringClause();
    scorer = ss.get(Long.MAX_VALUE);
    assertEquals(2, scorer.iterator().nextDoc());
    scorer.setMinCompetitiveScore(Math.nextUp(0f));
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

    reader.close();
    dir.close();
  }

  public void testScoreMax() throws IOException {
    try (Directory dir = newDirectory()) {
      try (RandomIndexWriter w =
          new RandomIndexWriter(
              random(),
              dir,
              newIndexWriterConfig()
                  .setMergePolicy(
                      // retain doc id order
                      newLogMergePolicy()))) {

        w.w
            .getConfig()
            .getCodec()
            .compoundFormat()
            .setShouldUseCompoundFile(random().nextBoolean());
        for (String[][] values :
            Arrays.asList(
                new String[][] {{"A", "B"}, {"A", "B", "C"}},
                new String[][] {{"A"}, {"B"}},
                new String[][] {{}},
                new String[][] {{"A", "B", "C"}, {"A", "B", "C", "D"}},
                new String[][] {{"B"}},
                new String[][] {{"B", "C"}, {"A", "B"}, {"A", "C"}})) {

          List<Document> docs = new ArrayList<>();
          for (String[] value : values) {
            Document childDoc = new Document();
            childDoc.add(newStringField("type", "child", Field.Store.NO));
            for (String v : value) {
              childDoc.add(newStringField("value", v, Field.Store.NO));
            }
            docs.add(childDoc);
          }

          Document parentDoc = new Document();
          parentDoc.add(newStringField("type", "parent", Field.Store.NO));
          docs.add(parentDoc);

          w.addDocuments(docs);
        }

        w.forceMerge(1);
      }

      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);

        BooleanQuery childQuery =
            new BooleanQuery.Builder()
                .add(
                    new BoostQuery(
                        new ConstantScoreQuery(new TermQuery(new Term("value", "A"))), 2),
                    BooleanClause.Occur.SHOULD)
                .add(
                    new ConstantScoreQuery(new TermQuery(new Term("value", "B"))),
                    BooleanClause.Occur.SHOULD)
                .add(
                    new BoostQuery(
                        new ConstantScoreQuery(new TermQuery(new Term("value", "C"))), 3),
                    BooleanClause.Occur.SHOULD)
                .add(
                    new BoostQuery(
                        new ConstantScoreQuery(new TermQuery(new Term("value", "D"))), 4),
                    BooleanClause.Occur.SHOULD)
                .build();
        BitSetProducer parentsFilter =
            new QueryBitSetProducer(new TermQuery(new Term("type", "parent")));
        ToParentBlockJoinQuery parentQuery =
            new ToParentBlockJoinQuery(childQuery, parentsFilter, ScoreMode.Max);

        Weight weight = searcher.createWeight(searcher.rewrite(parentQuery), TOP_SCORES, 1);
        ScorerSupplier ss = weight.scorerSupplier(searcher.getIndexReader().leaves().get(0));
        ss.setTopLevelScoringClause();
        Scorer scorer = ss.get(Long.MAX_VALUE);

        assertEquals(2, scorer.iterator().nextDoc());
        assertEquals(2 + 1 + 3, scorer.score(), 0);

        assertEquals(5, scorer.iterator().nextDoc());
        assertEquals(2, scorer.score(), 0);

        assertEquals(10, scorer.iterator().nextDoc());
        assertEquals(2 + 1 + 3 + 4, scorer.score(), 0);

        assertEquals(12, scorer.iterator().nextDoc());
        assertEquals(1, scorer.score(), 0);

        assertEquals(16, scorer.iterator().nextDoc());
        assertEquals(2 + 3, scorer.score(), 0);

        assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

        ss = weight.scorerSupplier(searcher.getIndexReader().leaves().get(0));
        ss.setTopLevelScoringClause();
        scorer = ss.get(Long.MAX_VALUE);
        scorer.setMinCompetitiveScore(6);

        assertEquals(2, scorer.iterator().nextDoc());
        assertEquals(2 + 1 + 3, scorer.score(), 0);

        assertEquals(10, scorer.iterator().nextDoc());
        assertEquals(2 + 1 + 3 + 4, scorer.score(), 0);

        assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

        ss = weight.scorerSupplier(searcher.getIndexReader().leaves().get(0));
        ss.setTopLevelScoringClause();
        scorer = ss.get(Long.MAX_VALUE);

        assertEquals(2, scorer.iterator().nextDoc());
        assertEquals(2 + 1 + 3, scorer.score(), 0);

        scorer.setMinCompetitiveScore(11);

        assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());
      }
    }
  }
}
