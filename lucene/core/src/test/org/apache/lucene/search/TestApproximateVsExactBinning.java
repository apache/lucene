/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.lucene.search;

import org.apache.lucene.codecs.lucene103.Lucene103Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
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
import org.junit.Test;

public class TestApproximateVsExactBinning extends LuceneTestCase {

  private static final int DOC_COUNT = 10000;
  private static final int RELEVANT_EVERY = 100;

  @Test
  public void testRelevanceParityBetweenExactAndApproxBinning() throws Exception {
    Directory exactDir = FSDirectory.open(createTempDir("exact-binning"));
    Directory approxDir = FSDirectory.open(createTempDir("approx-binning"));

    FieldType baseFieldType = new FieldType();
    baseFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    baseFieldType.setTokenized(true);
    baseFieldType.setStored(false);
    baseFieldType.putAttribute("postingsFormat", "Lucene101");
    baseFieldType.putAttribute("bin.count", "4");
    baseFieldType.freeze();

    FieldType exactFieldType = new FieldType(baseFieldType);
    exactFieldType.putAttribute("doBinning", "true");
    exactFieldType.putAttribute("bin.builder", "exact");

    FieldType approxFieldType = new FieldType(baseFieldType);
    approxFieldType.putAttribute("doBinning", "true");
    approxFieldType.putAttribute("bin.builder", "approx");

    IndexWriterConfig exactConfig = new IndexWriterConfig(new MockAnalyzer(random()));
    exactConfig.setCodec(new Lucene103Codec());
    exactConfig.setUseCompoundFile(false);

    IndexWriterConfig approxConfig = new IndexWriterConfig(new MockAnalyzer(random()));
    approxConfig.setCodec(new Lucene103Codec());
    approxConfig.setUseCompoundFile(false);

    try (IndexWriter writer = new IndexWriter(exactDir, exactConfig)) {
      for (int i = 0; i < DOC_COUNT; i++) {
        Document doc = new Document();
        String content = (i % RELEVANT_EVERY == 0) ? "lucene fast retrieval" : "irrelevant";
        doc.add(new Field("field", content, exactFieldType));
        writer.addDocument(doc);
      }
      writer.commit();
    }

    try (IndexWriter writer = new IndexWriter(approxDir, approxConfig)) {
      for (int i = 0; i < DOC_COUNT; i++) {
        Document doc = new Document();
        String content = (i % RELEVANT_EVERY == 0) ? "lucene fast retrieval" : "irrelevant";
        doc.add(new Field("field", content, approxFieldType));
        writer.addDocument(doc);
      }
      writer.commit();
    }

    DirectoryReader exactReader = DirectoryReader.open(exactDir);
    DirectoryReader approxReader = DirectoryReader.open(approxDir);

    IndexSearcher exactSearcher = new IndexSearcher(exactReader);
    IndexSearcher approxSearcher = new IndexSearcher(approxReader);
    exactSearcher.setSimilarity(new BM25Similarity());
    approxSearcher.setSimilarity(new BM25Similarity());

    TermQuery query = new TermQuery(new Term("field", "lucene"));

    TopDocs exactTop = exactSearcher.search(query, 10);
    TopDocs approxTop = approxSearcher.search(query, 10);

    int relevantExact = countRelevant(exactTop);
    int relevantApprox = countRelevant(approxTop);

    float recallExact = relevantExact / 10.0f;
    float recallApprox = relevantApprox / 10.0f;

    assertTrue("Approximate recall should be close to exact", recallApprox >= 0.9 * recallExact);

    float ndcgExact = computeNDCG(exactTop);
    float ndcgApprox = computeNDCG(approxTop);

    assertTrue("Approximate NDCG should be close to exact", ndcgApprox >= 0.9 * ndcgExact);

    exactReader.close();
    approxReader.close();
    exactDir.close();
    approxDir.close();
  }

  private int countRelevant(TopDocs topDocs) {
    int count = 0;
    for (ScoreDoc sd : topDocs.scoreDocs) {
      if (sd.doc % RELEVANT_EVERY == 0) {
        count++;
      }
    }
    return count;
  }

  private float computeNDCG(TopDocs topDocs) {
    float dcg = 0f;
    float idcg = 0f;
    int found = 0;
    for (int i = 0; i < topDocs.scoreDocs.length; i++) {
      int rank = i + 1;
      if (topDocs.scoreDocs[i].doc % RELEVANT_EVERY == 0) {
        dcg += (float) (1f / (Math.log(rank + 1) / Math.log(2)));
        found++;
      }
    }
    for (int i = 1; i <= found; i++) {
      idcg += (float) (1f / (Math.log(i + 1) / Math.log(2)));
    }
    return idcg > 0f ? dcg / idcg : 0f;
  }
}
