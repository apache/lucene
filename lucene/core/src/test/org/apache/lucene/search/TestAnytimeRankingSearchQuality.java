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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestAnytimeRankingSearchQuality extends LuceneTestCase {

  public void testAnytimeRankingPreservesQuality() throws Exception {
    Path tempDir = createTempDir("anytime-quality-test");
    Directory dir = FSDirectory.open(tempDir);

    FieldType fieldType = new FieldType(TextField.TYPE_NOT_STORED);
    fieldType.setTokenized(true);
    fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    fieldType.putAttribute("postingsFormat", "Lucene101");
    fieldType.putAttribute("doBinning", "true");
    fieldType.putAttribute("bin.count", "4");
    fieldType.freeze();

    IndexWriterConfig config = new IndexWriterConfig(new MockAnalyzer(random()));
    config.setCodec(new Lucene103Codec());
    config.setUseCompoundFile(false);

    int totalDocs = 10000;
    List<Integer> relevantDocIds = new ArrayList<>();

    try (IndexWriter writer = new IndexWriter(dir, config)) {
      for (int i = 0; i < totalDocs; i++) {
        Document doc = new Document();
        String content;
        if (i % 100 == 0) {
          content = "lucene fast ranking quality";
          relevantDocIds.add(i);
        } else {
          content = "random filler noise text";
        }
        doc.add(new Field("field", content, fieldType));
        writer.addDocument(doc);
      }
      writer.commit();
    }

    DirectoryReader reader = DirectoryReader.open(dir);
    IndexSearcher baselineSearcher = new IndexSearcher(reader);
    baselineSearcher.setSimilarity(new BM25Similarity());

    AnytimeRankingSearcher anytimeSearcher = new AnytimeRankingSearcher(reader, 10, 5, "field");

    TermQuery query = new TermQuery(new Term("field", "lucene"));
    TopDocs baseline = baselineSearcher.search(query, 10);
    TopDocs anytime = anytimeSearcher.search(query);

    List<Integer> topRelevant = new ArrayList<>();
    for (ScoreDoc sd : baseline.scoreDocs) {
      if (relevantDocIds.contains(sd.doc)) {
        topRelevant.add(sd.doc);
      }
    }

    double ndcgBaseline = computeNDCG(baseline, topRelevant);
    double ndcgAnytime = computeNDCG(anytime, topRelevant);

    double recallBaseline = computeRecall(baseline, topRelevant);
    double recallAnytime = computeRecall(anytime, topRelevant);

    double precisionBaseline = computePrecision(baseline, topRelevant);
    double precisionAnytime = computePrecision(anytime, topRelevant);

    assertTrue("NDCG should not degrade significantly", ndcgAnytime >= ndcgBaseline - 0.1);
    assertTrue(
        "Recall should remain within 90% of baseline", recallAnytime >= 0.9 * recallBaseline);
    assertTrue("Precision should remain competitive", precisionAnytime >= 0.9 * precisionBaseline);

    assertPositionDeltaWithinTolerance(
        baseline,
        anytime,
        topRelevant,
        3, // max allowed rank shift
        1.5, // avg allowed rank shift
        2 // max missed relevant docs
        );

    reader.close();
    dir.close();
  }

  private double computeNDCG(TopDocs docs, List<Integer> relevantDocIds) {
    double dcg = 0.0;
    double idcg = 0.0;

    for (int i = 0; i < docs.scoreDocs.length; i++) {
      int docId = docs.scoreDocs[i].doc;
      if (relevantDocIds.contains(docId)) {
        dcg += (Math.pow(2, 3) - 1) / (Math.log(i + 2) / Math.log(2));
      }
    }

    int relCount = Math.min(relevantDocIds.size(), docs.scoreDocs.length);
    for (int i = 0; i < relCount; i++) {
      idcg += (Math.pow(2, 3) - 1) / (Math.log(i + 2) / Math.log(2));
    }

    return idcg == 0 ? 0 : dcg / idcg;
  }

  private double computeRecall(TopDocs docs, List<Integer> relevantDocIds) {
    int hits = 0;
    for (ScoreDoc sd : docs.scoreDocs) {
      if (relevantDocIds.contains(sd.doc)) {
        hits++;
      }
    }
    return hits / (double) relevantDocIds.size();
  }

  private double computePrecision(TopDocs docs, List<Integer> relevantDocIds) {
    int hits = 0;
    for (ScoreDoc sd : docs.scoreDocs) {
      if (relevantDocIds.contains(sd.doc)) {
        hits++;
      }
    }
    return hits / (double) docs.scoreDocs.length;
  }

  private void assertPositionDeltaWithinTolerance(
      TopDocs baseline,
      TopDocs anytime,
      List<Integer> relevantDocIds,
      int maxAllowedDelta,
      double avgAllowedDelta,
      int maxMissedRelevant) {

    Map<Integer, Integer> baselineRanks = new HashMap<>();
    Map<Integer, Integer> anytimeRanks = new HashMap<>();

    for (int i = 0; i < baseline.scoreDocs.length; i++) {
      baselineRanks.put(baseline.scoreDocs[i].doc, i);
    }
    for (int i = 0; i < anytime.scoreDocs.length; i++) {
      anytimeRanks.put(anytime.scoreDocs[i].doc, i);
    }

    int matched = 0;
    int totalDelta = 0;
    int maxDelta = 0;
    int missed = 0;

    for (int docID : relevantDocIds) {
      Integer posBaseline = baselineRanks.get(docID);
      Integer posAnytime = anytimeRanks.get(docID);

      if (posBaseline != null && posAnytime != null) {
        int delta = Math.abs(posBaseline - posAnytime);
        totalDelta += delta;
        maxDelta = Math.max(maxDelta, delta);
        matched++;
      } else {
        missed++;
      }
    }

    if (matched > 0) {
      double avgDelta = totalDelta / (double) matched;

      assertTrue(
          "Average position delta should be ≤ " + avgAllowedDelta + " but was " + avgDelta,
          avgDelta <= avgAllowedDelta);

      assertTrue(
          "Max position delta should be ≤ " + maxAllowedDelta + " but was " + maxDelta,
          maxDelta <= maxAllowedDelta);

      assertTrue(
          "Missed relevant docs should be ≤ " + maxMissedRelevant + " but was " + missed,
          missed <= maxMissedRelevant);
    } else {
      fail("No relevant docs matched in both baseline and anytime search results");
    }
  }
}
