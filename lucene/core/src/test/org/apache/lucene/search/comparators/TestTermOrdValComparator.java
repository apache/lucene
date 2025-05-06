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
package org.apache.lucene.search.comparators;

import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestTermOrdValComparator extends LuceneTestCase {

  public void testIntoBitSetBugIssue14517() throws IOException {
    final int maxDoc = 5_000;
    try (Directory dir = new ByteBuffersDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
        // high max doc to have a high number of unique values so that the competitive iterator is
        // initialized with `docsWithField` rather than specific (< 1024) terms
        for (int i = 0; i < maxDoc; ++i) {
          Document doc = new Document();
          // make the field to be sparse, so that the iterator is initialized with `docsWithField`
          if (i % 2 == 0) {
            doc.add(new StringField("field", "value", Field.Store.NO));
            doc.add(new KeywordField("sort", Integer.toString(i), Field.Store.NO));
          }
          w.addDocument(doc);
        }
        w.forceMerge(1);
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReaderContext context = reader.leaves().get(0);
        IndexSearcher searcher = new IndexSearcher(reader);
        Query query = new TermQuery(new Term("field", "value"));
        Weight weight =
            searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, RANDOM_MULTIPLIER);
        SortField sortField = KeywordField.newSortField("sort", false, SortedSetSelector.Type.MIN);
        sortField.setMissingValue(SortField.STRING_LAST);
        Sort sort = new Sort(sortField);
        Collector collector = new TopFieldCollectorManager(sort, 10, 10).newCollector();
        LeafCollector leafCollector = collector.getLeafCollector(context);
        BulkScorer bulkScorer = weight.bulkScorer(context);
        // split on this specific doc ID so that the current doc of the competitive iterator
        // and the current doc of `docsWithField` are out of sync,
        // because the competitive iterator was just updated.
        bulkScorer.score(leafCollector, null, 0, 22);
        bulkScorer.score(leafCollector, null, 22, maxDoc);
      }
    }
  }
}
