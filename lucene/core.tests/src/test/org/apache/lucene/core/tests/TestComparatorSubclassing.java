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
package org.apache.lucene.core.tests;

import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.comparators.LongComparator;
import org.apache.lucene.search.comparators.NumericComparator;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestComparatorSubclassing extends LuceneTestCase {

  public void testPluggableCompetitiveIterators() throws IOException {

    boolean[] updated = new boolean[] {false};

    LongComparator extendedComparator =
        new LongComparator(10, "field", 0L, true, Pruning.NONE) {
          @Override
          public LeafFieldComparator getLeafComparator(LeafReaderContext context)
              throws IOException {
            return new LongLeafComparator(context) {
              @Override
              protected NumericComparator<Long>.CompetitiveDISIBuilder buildCompetitiveDISIBuilder()
                  throws IOException {
                int maxDoc = context.reader().maxDoc();
                int jumpTo = Math.max(0, maxDoc - 10);
                return new CompetitiveDISIBuilder(this) {
                  @Override
                  protected boolean hasMissingDocs() {
                    return false;
                  }

                  @Override
                  protected void doUpdateCompetitiveIterator() {
                    updateCompetitiveIterator(DocIdSetIterator.range(jumpTo, maxDoc));
                    updated[0] = true;
                  }
                };
              }
            };
          }
        };

    try (var dir = newDirectory();
        var writer = new RandomIndexWriter(random(), dir)) {
      for (int i = 0; i < 2000; i++) {
        Document doc = new Document();
        doc.add(NumericDocValuesField.indexedField("field", i));
        writer.addDocument(doc);
      }
      writer.commit();

      try (var reader = writer.getReader()) {
        IndexSearcher searcher = new IndexSearcher(reader);
        Sort sort =
            new Sort(
                new SortField(
                    "field",
                    new FieldComparatorSource() {
                      @Override
                      public FieldComparator<?> newComparator(
                          String fieldname, int numHits, Pruning pruning, boolean reversed) {
                        return extendedComparator;
                      }
                    }));

        TopDocs td = searcher.search(MatchAllDocsQuery.INSTANCE, 10, sort);
        assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, td.totalHits.relation());
        assertTrue(updated[0]);
      }
    }
  }
}
