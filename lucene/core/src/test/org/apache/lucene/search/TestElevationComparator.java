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
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

public class TestElevationComparator extends LuceneTestCase {
  private Directory directory;
  private IndexReader reader;
  private IndexSearcher searcher;
  private final Map<BytesRef, Integer> priority = new HashMap<>();

  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    IndexWriter writer =
        new IndexWriter(
            directory,
            newIndexWriterConfig(new MockAnalyzer(random()))
                .setMaxBufferedDocs(2)
                .setMergePolicy(newLogMergePolicy(1000))
                .setSimilarity(new ClassicSimilarity()));
    writer.addDocument(adoc(new String[] {"id", "a", "title", "ipod", "str_s", "a"}));
    writer.addDocument(adoc(new String[] {"id", "b", "title", "ipod ipod", "str_s", "b"}));
    writer.addDocument(adoc(new String[] {"id", "c", "title", "ipod ipod ipod", "str_s", "c"}));
    writer.addDocument(adoc(new String[] {"id", "x", "title", "boosted", "str_s", "x"}));
    writer.addDocument(adoc(new String[] {"id", "y", "title", "boosted boosted", "str_s", "y"}));
    writer.addDocument(
        adoc(new String[] {"id", "z", "title", "boosted boosted boosted", "str_s", "z"}));

    reader = DirectoryReader.open(writer);
    writer.close();

    searcher = newSearcher(reader);
    searcher.setSimilarity(new BM25Similarity());
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    reader.close();
    directory.close();
  }

  public void testSorting() throws Throwable {
    runTest(false);
  }

  public void testSortingReversed() throws Throwable {
    runTest(true);
  }

  private void runTest(boolean reversed) throws Throwable {

    BooleanQuery.Builder newq = new BooleanQuery.Builder();
    TermQuery query = new TermQuery(new Term("title", "ipod"));

    newq.add(query, BooleanClause.Occur.SHOULD);
    newq.add(getElevatedQuery(new String[] {"id", "a", "id", "x"}), BooleanClause.Occur.SHOULD);

    Sort sort =
        new Sort(
            new SortField("id", new ElevationComparatorSource(priority), false),
            new SortField(null, SortField.Type.SCORE, reversed));

    TopDocs topDocs =
        searcher.search(
            newq.build(), TopFieldCollector.createSharedManager(sort, 50, null, Integer.MAX_VALUE));
    int nDocsReturned = topDocs.scoreDocs.length;

    assertEquals(4, nDocsReturned);

    // 0 & 3 were elevated
    assertEquals(0, topDocs.scoreDocs[0].doc);
    assertEquals(3, topDocs.scoreDocs[1].doc);

    if (reversed) {
      assertEquals(1, topDocs.scoreDocs[2].doc);
      assertEquals(2, topDocs.scoreDocs[3].doc);
    } else {
      assertEquals(2, topDocs.scoreDocs[2].doc);
      assertEquals(1, topDocs.scoreDocs[3].doc);
    }

    /*
     StoredFields storedFields = searcher.storedFields();
     for (int i = 0; i < nDocsReturned; i++) {
      ScoreDoc scoreDoc = topDocs.scoreDocs[i];
      ids[i] = scoreDoc.doc;
      scores[i] = scoreDoc.score;
      documents[i] = storedFields.document(ids[i]);
      System.out.println("ids[i] = " + ids[i]);
      System.out.println("documents[i] = " + documents[i]);
      System.out.println("scores[i] = " + scores[i]);
    }
     */
  }

  private Query getElevatedQuery(String[] vals) {
    BooleanQuery.Builder b = new BooleanQuery.Builder();
    int max = (vals.length / 2) + 5;
    for (int i = 0; i < vals.length - 1; i += 2) {
      b.add(new TermQuery(new Term(vals[i], vals[i + 1])), BooleanClause.Occur.SHOULD);
      priority.put(new BytesRef(vals[i + 1]), Integer.valueOf(max--));
      // System.out.println(" pri doc=" + vals[i+1] + " pri=" + (1+max));
    }
    BooleanQuery q = b.build();
    return new BoostQuery(q, 0f);
  }

  private Document adoc(String[] vals) {
    Document doc = new Document();
    for (int i = 0; i < vals.length - 2; i += 2) {
      doc.add(newTextField(vals[i], vals[i + 1], Field.Store.YES));
      if (vals[i].equals("id")) {
        doc.add(new SortedDocValuesField(vals[i], new BytesRef(vals[i + 1])));
      }
    }
    return doc;
  }
}

class ElevationComparatorSource extends FieldComparatorSource {
  private final Map<BytesRef, Integer> priority;

  public ElevationComparatorSource(final Map<BytesRef, Integer> boosts) {
    this.priority = boosts;
  }

  @Override
  public FieldComparator<Integer> newComparator(
      final String fieldname, final int numHits, boolean enableSkipping, boolean reversed) {
    return new FieldComparator<Integer>() {

      private final int[] values = new int[numHits];
      int bottomVal;

      @Override
      public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
        return new LeafFieldComparator() {

          @Override
          public void setBottom(int slot) {
            bottomVal = values[slot];
          }

          @Override
          public int compareTop(int doc) {
            throw new UnsupportedOperationException();
          }

          private int docVal(int doc) throws IOException {
            SortedDocValues idIndex = DocValues.getSorted(context.reader(), fieldname);
            if (idIndex.advance(doc) == doc) {
              final BytesRef term = idIndex.lookupOrd(idIndex.ordValue());
              Integer prio = priority.get(term);
              return prio == null ? 0 : prio.intValue();
            } else {
              return 0;
            }
          }

          @Override
          public int compareBottom(int doc) throws IOException {
            return docVal(doc) - bottomVal;
          }

          @Override
          public void copy(int slot, int doc) throws IOException {
            values[slot] = docVal(doc);
          }

          @Override
          public void setScorer(Scorable scorer) {}
        };
      }

      @Override
      public int compare(int slot1, int slot2) {
        // values will be small enough that there is no overflow concern
        return values[slot2] - values[slot1];
      }

      @Override
      public int compareValues(Integer first, Integer second) {
        // values will be small enough that there is no overflow concern
        return second - first;
      }

      @Override
      public void setTopValue(Integer value) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Integer value(int slot) {
        return Integer.valueOf(values[slot]);
      }
    };
  }
}
