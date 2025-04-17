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

import org.apache.lucene.codecs.lucene103.Lucene103Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

import java.util.Arrays;

public class TestAnytimeRankingNDCG extends LuceneTestCase {

  public void testNDCGDoesNotDiverge() throws Exception {
    MockDirectoryWrapper dir = new MockDirectoryWrapper(random(), new ByteBuffersDirectory());
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(new Lucene103Codec());

    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    ft.setTokenized(true);
    ft.putAttribute("postingsFormat", "Lucene101");
    ft.putAttribute("doBinning", "true");
    ft.putAttribute("bin.count", "4");
    ft.freeze();

    try (IndexWriter writer = new IndexWriter(dir, iwc)) {
      for (int i = 0; i < 500; i++) {
        Document doc = new Document();
        StringBuilder content = new StringBuilder();

        if (i % 50 == 0) {
          content.append("lucene high quality scoring search");
        } else {
          content.append("random filler ").append(TestUtil.randomSimpleString(random()));
        }

        doc.add(new Field("field", content.toString(), ft));
        writer.addDocument(doc);
      }
      writer.commit();
    }

    DirectoryReader reader = DirectoryReader.open(dir);
    IndexSearcher baseline = new IndexSearcher(reader);
    baseline.setSimilarity(new BM25Similarity());

    AnytimeRankingSearcher anytime = new AnytimeRankingSearcher(baseline, 10, 5, "field");

    TopDocs baselineDocs = baseline.search(new TermQuery(new Term("field", "lucene")), 10);
    TopDocs anytimeDocs = anytime.search(new TermQuery(new Term("field", "lucene")));

    double baselineNDCG = computeNDCG(baselineDocs);
    double anytimeNDCG = computeNDCG(anytimeDocs);

    // Ensure that NDCG does not diverge by more than 0.1
    assertEquals("NDCG values should be close", baselineNDCG, anytimeNDCG, 0.1);

    reader.close();
    dir.close();
  }

  private double computeNDCG(TopDocs topDocs) {
    int[] gains = new int[topDocs.scoreDocs.length];
    for (int i = 0; i < topDocs.scoreDocs.length; i++) {
      gains[i] = (topDocs.scoreDocs[i].doc % 50 == 0) ? 3 : 0;
    }

    double dcg = 0.0;
    for (int i = 0; i < gains.length; i++) {
      int rel = gains[i];
      if (rel > 0) {
        dcg += (Math.pow(2, rel) - 1) / (Math.log(i + 2) / Math.log(2));
      }
    }

    Arrays.sort(gains);
    double idcg = 0.0;
    for (int i = gains.length - 1; i >= 0; i--) {
      int rel = gains[i];
      if (rel > 0) {
        int rank = gains.length - i;
        idcg += (Math.pow(2, rel) - 1) / (Math.log(rank + 1) / Math.log(2));
      }
    }

    return idcg == 0.0 ? 0.0 : dcg / idcg;
  }
}
