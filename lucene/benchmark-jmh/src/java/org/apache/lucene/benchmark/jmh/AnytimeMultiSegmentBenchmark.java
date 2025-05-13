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
package org.apache.lucene.benchmark.jmh;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.codecs.lucene103.Lucene103Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.AnytimeRankingSearcher;
import org.apache.lucene.search.IndexSearcher;
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

/** Benchmark for multi-segment bin-aware search with AnytimeRanking. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@Fork(1)
public class AnytimeMultiSegmentBenchmark {

  private Directory dir;
  private IndexReader reader;
  private IndexSearcher searcher;
  private AnytimeRankingSearcher anytimeSearcher;
  private TermQuery query;

  @Param({"1000", "5000"})
  private int docCount;

  @Param({"true", "false"})
  private boolean binning;

  @Setup
  public void setup() throws Exception {
    Path path = Files.createTempDirectory("benchmark-segments");
    dir = new MMapDirectory(path);
    IndexWriterConfig iwc = new IndexWriterConfig(new FixedTokenAnalyzer());
    iwc.setCodec(new Lucene103Codec());
    iwc.setUseCompoundFile(false);
    iwc.setMaxBufferedDocs(50); // force multiple flushes

    FieldType type = new FieldType(TextField.TYPE_NOT_STORED);
    type.setTokenized(true);
    type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    if (binning) {
      type.putAttribute("postingsFormat", "Lucene101");
      type.putAttribute("doBinning", "true");
      type.putAttribute("bin.count", "4");
    }
    type.freeze();

    try (IndexWriter writer = new IndexWriter(dir, iwc)) {
      for (int i = 0; i < docCount; i++) {
        Document doc = new Document();
        String content = (i % 3 == 0) ? "lucene fast ranking test" : "filler junk";
        doc.add(new Field("field", content, type));
        writer.addDocument(doc);
      }
      writer.commit();
    }

    reader = DirectoryReader.open(dir);
    searcher = new IndexSearcher(reader);
    searcher.setSimilarity(new BM25Similarity());

    anytimeSearcher = new AnytimeRankingSearcher(reader, 10, 5, "field");
    query = new TermQuery(new Term("field", "lucene"));
  }

  @Benchmark
  public TopDocs baselineSearch() throws IOException {
    return searcher.search(query, 10);
  }

  @Benchmark
  public TopDocs anytimeSearch() throws IOException {
    return anytimeSearcher.search(query);
  }

  @TearDown
  public void tearDown() throws Exception {
    reader.close();
    dir.close();
  }

  /** Simple analyzer that emits a single token. */
  private static final class FixedTokenAnalyzer extends Analyzer {
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
      Tokenizer tokenizer =
          new Tokenizer() {
            private final CharTermAttribute termAttr = addAttribute(CharTermAttribute.class);
            private boolean emitted = false;

            @Override
            public boolean incrementToken() {
              if (emitted) {
                return false;
              }
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
