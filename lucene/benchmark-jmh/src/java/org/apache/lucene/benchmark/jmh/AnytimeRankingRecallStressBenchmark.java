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
package org.apache.lucene.benchmark.jmh;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

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
import org.apache.lucene.search.AnytimeRankingSearcher;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
@Fork(1)
public class AnytimeRankingRecallStressBenchmark {

  private Directory directory;
  private IndexReader reader;
  private IndexSearcher baselineSearcher;
  private AnytimeRankingSearcher anytimeSearcher;
  private TermQuery query;

  @Param({"20000"})
  private int docCount;

  @Param({"1", "3"})
  private int sla;

  @Setup
  public void setup() throws Exception {
    Path path = Files.createTempDirectory("recall-stress");
    directory = new MMapDirectory(path);
    IndexWriterConfig config = new IndexWriterConfig(new SingleTokenAnalyzer());
    config.setCodec(new Lucene101Codec());
    config.setUseCompoundFile(false);
    config.setMaxBufferedDocs(100);

    FieldType ft = new FieldType();
    ft.setTokenized(true);
    ft.setStored(false);
    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    ft.putAttribute("postingsFormat", "Lucene101");
    ft.putAttribute("doBinning", "true");
    ft.putAttribute("bin.count", "4");
    ft.freeze();

    try (IndexWriter writer = new IndexWriter(directory, config)) {
      for (int i = 0; i < docCount; i++) {
        Document doc = new Document();
        String content = (i % 200 == 0) ? "lucene precision recall document" : "random junk text";
        doc.add(new Field("field", content, ft));
        writer.addDocument(doc);
      }
      writer.commit();
    }

    reader = DirectoryReader.open(directory);
    baselineSearcher = new IndexSearcher(reader);
    baselineSearcher.setSimilarity(new BM25Similarity());
    anytimeSearcher = new AnytimeRankingSearcher(baselineSearcher, 10, sla, "field");
    query = new TermQuery(new Term("field", "lucene"));
  }

  @Benchmark
  public float baselineRecall() throws IOException {
    TopDocs topDocs = baselineSearcher.search(query, 10);
    return countRelevant(topDocs) / 10.0f;
  }

  @Benchmark
  public float anytimeRecall() throws IOException {
    TopDocs topDocs = anytimeSearcher.search(query);
    return countRelevant(topDocs) / 10.0f;
  }

  @Benchmark
  public float baselineMRR() throws IOException {
    TopDocs topDocs = baselineSearcher.search(query, 10);
    return computeMRR(topDocs);
  }

  @Benchmark
  public float anytimeMRR() throws IOException {
    TopDocs topDocs = anytimeSearcher.search(query);
    return computeMRR(topDocs);
  }

  @Benchmark
  public float baselineNDCG() throws IOException {
    TopDocs topDocs = baselineSearcher.search(query, 10);
    return computeNDCG(topDocs);
  }

  @Benchmark
  public float anytimeNDCG() throws IOException {
    TopDocs topDocs = anytimeSearcher.search(query);
    return computeNDCG(topDocs);
  }

  private int countRelevant(TopDocs topDocs) {
    int relevant = 0;
    for (ScoreDoc doc : topDocs.scoreDocs) {
      if (doc.doc % 200 == 0) {
        relevant++;
      }
    }
    return relevant;
  }

  private float computeMRR(TopDocs topDocs) {
    for (int i = 0; i < topDocs.scoreDocs.length; i++) {
      if (topDocs.scoreDocs[i].doc % 200 == 0) {
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
      if (topDocs.scoreDocs[i].doc % 200 == 0) {
        dcg += 1.0 / (Math.log(rank + 1) / Math.log(2));
        count++;
      }
    }
    for (int j = 1; j <= count; j++) {
      idcg += 1.0 / (Math.log(j + 1) / Math.log(2));
    }
    return idcg > 0 ? dcg / idcg : 0f;
  }

  @TearDown
  public void tearDown() throws IOException {
    reader.close();
    directory.close();
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