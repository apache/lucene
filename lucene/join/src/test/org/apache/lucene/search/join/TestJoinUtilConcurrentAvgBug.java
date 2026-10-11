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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.IndexSearcher.LeafReaderContextPartition;
import org.apache.lucene.search.IndexSearcher.LeafSlice;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

/** */
public class TestJoinUtilConcurrentAvgBug extends LuceneTestCase {

  public void testAvgJoinWithMultipleCollectorsFails() throws IOException, InterruptedException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig();
    iwc.setMergePolicy(NoMergePolicy.INSTANCE); // keep 2 distinct segments
    IndexWriter w = new IndexWriter(dir, iwc);

    // segment 1
    Document doc = new Document();
    doc.add(new TextField("description", "match", Field.Store.NO));
    doc.add(new StringField("join_field", "a", Field.Store.NO));
    doc.add(new SortedDocValuesField("join_field", new BytesRef("a")));
    w.addDocument(doc);
    w.commit();

    // segment 2: a distinct join value so the merged term set grows past segment 1's own count.
    doc = new Document();
    doc.add(new TextField("description", "match", Field.Store.NO));
    doc.add(new StringField("join_field", "b", Field.Store.NO));
    doc.add(new SortedDocValuesField("join_field", new BytesRef("b")));
    w.addDocument(doc);
    w.commit();

    DirectoryReader reader = DirectoryReader.open(w);
    w.close();

    // Force two slices (one per segment) collected concurrently via an executor, so that
    // GenericTermsCollector.Manager#reduce receives more than one collector.
    ExecutorService executor = Executors.newFixedThreadPool(2);
    IndexSearcher searcher =
        new IndexSearcher(reader, executor) {
          @Override
          protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
            LeafSlice[] slices = new LeafSlice[leaves.size()];
            for (int i = 0; i < leaves.size(); i++) {
              slices[i] =
                  new LeafSlice(
                      List.of(LeafReaderContextPartition.createForEntireSegment(leaves.get(i))));
            }
            return slices;
          }
        };

    Query fromQuery = new TermQuery(new org.apache.lucene.index.Term("description", "match"));

    try {
      Query joinQuery =
          JoinUtil.createJoinQuery(
              "join_field",
              false, // single value per document
              "join_field",
              fromQuery,
              searcher,
              ScoreMode.Avg);
      TopDocs topDocs = searcher.search(joinQuery, 10);
      assertEquals(2, topDocs.totalHits.value());
    } finally {
      reader.close();
      dir.close();
      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.SECONDS);
    }
  }
}
