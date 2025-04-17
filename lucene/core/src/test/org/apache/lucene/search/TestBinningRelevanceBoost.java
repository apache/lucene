/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

public class TestBinningRelevanceBoost extends LuceneTestCase {

  public void testBinningImprovesRecallUnderTruncation() throws Exception {
    final int totalDocs = 5000;
    final int relevantEvery = 200;
    final int truncationLimit = 20;

    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setCodec(TestUtil.getDefaultCodec());
    iwc.setMaxBufferedDocs(100);
    iwc.setUseCompoundFile(false);

    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    ft.setTokenized(true);
    ft.putAttribute("postingsFormat", "Lucene101");
    ft.putAttribute("doBinning", "true");
    ft.putAttribute("bin.count", "4");
    ft.freeze();

    try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc)) {
      for (int i = 0; i < totalDocs; i++) {
        Document doc = new Document();
        String content =
            (i % relevantEvery == 0) ? "lucene highly relevant text" : "noise filler irrelevant";
        doc.add(new StoredField("docID", i));
        doc.add(new Field("content", content, ft));
        writer.addDocument(doc);
      }
      writer.commit();
    }

    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher baseline = newSearcher(reader);
    baseline.setSimilarity(new BM25Similarity());

    AnytimeRankingSearcher anytimeRankingSearcher =
        new AnytimeRankingSearcher(baseline, 10, truncationLimit, "content");

    TermQuery query = new TermQuery(new Term("content", "lucene"));

    AtomicInteger baselineHits = new AtomicInteger();
    baseline.search(
        query,
        new CollectorManager<SimpleCollector, Void>() {
          @Override
          public SimpleCollector newCollector() {
            return new SimpleCollector() {
              private int seen = 0;

              @Override
              public void collect(int doc) throws IOException {
                if (doc % relevantEvery == 0) {
                  baselineHits.incrementAndGet();
                }
                if (++seen >= truncationLimit) {
                  throw new CollectionTerminatedException();
                }
              }

              @Override
              public void doSetNextReader(LeafReaderContext context) {}

              @Override
              public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE_NO_SCORES;
              }
            };
          }

          @Override
          public Void reduce(Collection<SimpleCollector> collectors) {
            return null;
          }
        });

    TopDocs anytimeRankingResults = anytimeRankingSearcher.search(query);
    int anytimeRankingHits = 0;
    for (ScoreDoc sd : anytimeRankingResults.scoreDocs) {
      if (sd.doc % relevantEvery == 0) {
        anytimeRankingHits++;
      }
    }

    assertTrue(
        "AnytimeRankingSearch should retrieve more relevant results under truncation. "
            + "Baseline hits="
            + baselineHits.get()
            + ", AnytimeRankingSearch hits="
            + anytimeRankingHits,
        anytimeRankingHits > baselineHits.get());

    reader.close();
    dir.close();
  }
}
