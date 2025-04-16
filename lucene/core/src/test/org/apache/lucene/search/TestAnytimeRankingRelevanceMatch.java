/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 */
package org.apache.lucene.search;

import java.io.IOException;
import java.util.Locale;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.codecs.lucene101.Lucene101Codec;
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
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Test;

public class TestAnytimeRankingRelevanceMatch extends LuceneTestCase {

  private static final int DOC_COUNT = 20000;
  private static final int RELEVANT_FREQ = 200;

  @Test
  public void testRelevanceMetricsComparison() throws Exception {
    Directory dir = new MMapDirectory(createTempDir("relevance-test"));
    IndexWriterConfig config = new IndexWriterConfig(new SingleTokenAnalyzer());
    config.setCodec(new Lucene101Codec());
    config.setUseCompoundFile(false);
    config.setMaxBufferedDocs(100);

    FieldType ft = new FieldType();
    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    ft.setTokenized(true);
    ft.setStored(false);
    ft.putAttribute("postingsFormat", "Lucene101");
    ft.putAttribute("doBinning", "true");
    ft.putAttribute("bin.count", "4");
    ft.freeze();

    try (IndexWriter writer = new IndexWriter(dir, config)) {
      for (int i = 0; i < DOC_COUNT; i++) {
        Document doc = new Document();
        String content =
            (i % RELEVANT_FREQ == 0) ? "lucene precision recall document" : "random filler content";
        doc.add(new Field("field", content, ft));
        writer.addDocument(doc);
      }
      writer.commit();
    }

    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher baselineSearcher = new IndexSearcher(reader);
    baselineSearcher.setSimilarity(new BM25Similarity());

    AnytimeRankingSearcher anytime = new AnytimeRankingSearcher(baselineSearcher, 10, 3, "field");

    TermQuery query = new TermQuery(new Term("field", "lucene"));
    TopDocs baseline = baselineSearcher.search(query, 10);
    TopDocs anytimeResults = anytime.search(query);

    float baselineRecall = countRelevant(baseline) / 10.0f;
    float anytimeRecall = countRelevant(anytimeResults) / 10.0f;

    float baselineMRR = computeMRR(baseline);
    float anytimeMRR = computeMRR(anytimeResults);

    float baselineNDCG = computeNDCG(baseline);
    float anytimeNDCG = computeNDCG(anytimeResults);

    System.out.printf(
        Locale.ROOT,
        "Recall: baseline=%.2f anytime=%.2f | MRR: baseline=%.2f anytime=%.2f | NDCG: baseline=%.2f anytime=%.2f%n",
        baselineRecall,
        anytimeRecall,
        baselineMRR,
        anytimeMRR,
        baselineNDCG,
        anytimeNDCG);

    assertTrue("Anytime recall should be >= baseline", anytimeRecall >= baselineRecall);
    assertTrue("Anytime MRR should be >= baseline", anytimeMRR >= baselineMRR);
    assertTrue("Anytime NDCG should be >= baseline", anytimeNDCG >= baselineNDCG);

    reader.close();
    dir.close();
  }

  private int countRelevant(TopDocs topDocs) {
    int relevant = 0;
    for (ScoreDoc doc : topDocs.scoreDocs) {
      if (doc.doc % RELEVANT_FREQ == 0) {
        relevant++;
      }
    }
    return relevant;
  }

  private float computeMRR(TopDocs topDocs) {
    for (int i = 0; i < topDocs.scoreDocs.length; i++) {
      if (topDocs.scoreDocs[i].doc % RELEVANT_FREQ == 0) {
        return 1.0f / (i + 1);
      }
    }
    return 0f;
  }

  private float computeNDCG(TopDocs topDocs) {
    float dcg = 0f;
    float idcg = 0f;
    int count = 0;
    for (int i = 0; i < topDocs.scoreDocs.length; i++) {
      int rank = i + 1;
      if (topDocs.scoreDocs[i].doc % RELEVANT_FREQ == 0) {
        dcg += (float) (1.0 / (Math.log(rank + 1) / Math.log(2)));
        count++;
      }
    }
    for (int j = 1; j <= count; j++) {
      idcg += (float) (1.0 / (Math.log(j + 1) / Math.log(2)));
    }
    return idcg > 0 ? dcg / idcg : 0f;
  }

  private static final class SingleTokenAnalyzer extends Analyzer {
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
      Tokenizer tokenizer =
          new Tokenizer() {
            private final CharTermAttribute termAttr = addAttribute(CharTermAttribute.class);
            private boolean emitted = false;

            @Override
            public boolean incrementToken() {
              if (emitted) return false;
              clearAttributes();
              termAttr.append("lucene");
              emitted = true;
              return true;
            }

            @Override
            public void reset() throws IOException {
              super.reset();
              emitted = false;
            }
          };
      return new TokenStreamComponents(tokenizer);
    }
  }
}
