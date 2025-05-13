/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.lucene.search;

import java.io.IOException;
import org.apache.lucene.codecs.lucene103.Lucene103Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Test;

public class TestAnytimeRankingRelevanceMatch extends LuceneTestCase {

  private static final int DOC_COUNT = 20000;
  private static final int RELEVANT_FREQ = 200;
  private static final int TOP_K = 10;

  @Test
  public void testRelevanceMetricsComparison() throws IOException {
    Directory dir = new MMapDirectory(createTempDir("anytime-rank-quality"));
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(new Lucene103Codec());
    iwc.setUseCompoundFile(false);
    iwc.setMaxBufferedDocs(100);

    FieldType fieldType = new FieldType();
    fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    fieldType.setTokenized(true);
    fieldType.setStored(false);
    fieldType.putAttribute("postingsFormat", "Lucene101");
    fieldType.putAttribute("doBinning", "true");
    fieldType.putAttribute("bin.count", "4");
    fieldType.freeze();

    try (IndexWriter writer = new IndexWriter(dir, iwc)) {
      for (int i = 0; i < DOC_COUNT; i++) {
        Document doc = new Document();
        String content =
            (i % RELEVANT_FREQ == 0)
                ? "lucene ranking relevance quality"
                : "filler content unrelated noise";
        doc.add(new Field("field", content, fieldType));
        writer.addDocument(doc);
      }
      writer.commit();
    }

    try (IndexReader reader = DirectoryReader.open(dir)) {
      IndexSearcher baselineSearcher = new IndexSearcher(reader);
      baselineSearcher.setSimilarity(new BM25Similarity());

      TermQuery query = new TermQuery(new Term("field", "lucene"));

      TopDocs baseline = baselineSearcher.search(query, TOP_K);
      try (AnytimeRankingSearcher anytime = new AnytimeRankingSearcher(reader, TOP_K, 5, "field")) {
        TopDocs anytimeDocs = anytime.search(query);

        float baselineRecall = recall(baseline);
        float anytimeRecall = recall(anytimeDocs);
        float baselineMRR = mrr(baseline);
        float anytimeMRR = mrr(anytimeDocs);
        float baselineNDCG = ndcg(baseline);
        float anytimeNDCG = ndcg(anytimeDocs);

        assertTrue(
            "Anytime recall should be within 90% of baseline",
            anytimeRecall >= 0.9f * baselineRecall);
        assertTrue(
            "Anytime NDCG should be within 90% of baseline", anytimeNDCG >= 0.9f * baselineNDCG);
        assertTrue(
            "Anytime MRR should be within 90% of baseline", anytimeMRR >= 0.9f * baselineMRR);
      }
    }

    dir.close();
  }

  private static float recall(TopDocs docs) {
    int relevant = 0;
    for (ScoreDoc sd : docs.scoreDocs) {
      if (sd.doc % RELEVANT_FREQ == 0) {
        relevant++;
      }
    }
    return relevant / (float) TOP_K;
  }

  private static float mrr(TopDocs docs) {
    for (int i = 0; i < docs.scoreDocs.length; i++) {
      if (docs.scoreDocs[i].doc % RELEVANT_FREQ == 0) {
        return 1.0f / (i + 1);
      }
    }
    return 0f;
  }

  private static float ndcg(TopDocs docs) {
    float dcg = 0f;
    float idcg = 0f;
    int rank = 1;
    int found = 0;

    for (ScoreDoc sd : docs.scoreDocs) {
      if (sd.doc % RELEVANT_FREQ == 0) {
        dcg += (float) (1.0f / (Math.log(rank + 1) / Math.log(2)));
        found++;
      }
      rank++;
    }

    for (int i = 1; i <= found; i++) {
      idcg += (float) (1.0f / (Math.log(i + 1) / Math.log(2)));
    }

    return idcg > 0 ? dcg / idcg : 0f;
  }
}
