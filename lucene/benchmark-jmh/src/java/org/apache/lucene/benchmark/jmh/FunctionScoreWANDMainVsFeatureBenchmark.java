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
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.FunctionScoreQuery;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * JMH Micro-benchmark comparing actual FunctionScoreQuery search throughput
 * on main branch vs feature/function-score-wand-skip-index branch over 1 Million Lucene index documents.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 3, time = 3)
@Fork(
    value = 1,
    warmups = 1,
    jvmArgsAppend = {"-Xmx8g", "-Xms8g"})
public class FunctionScoreWANDMainVsFeatureBenchmark {

  @State(Scope.Benchmark)
  public static class BenchmarkState {

    @Param({"1000000"})
    public int numDocs;

    @Param({"100"})
    public int topK;

    public Directory dir;
    public IndexReader reader;
    public IndexSearcher searcher;
    public Query functionScoreQuery;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      dir = new ByteBuffersDirectory();
      IndexWriterConfig iwc = new IndexWriterConfig();
      iwc.setRAMBufferSizeMB(256);

      try (IndexWriter writer = new IndexWriter(dir, iwc)) {
        Random random = new Random(42);
        for (int i = 0; i < numDocs; i++) {
          Document doc = new Document();
          // 10% of documents match "match_term"
          if (i % 10 == 0) {
            doc.add(new TextField("body", "match_term token", Field.Store.NO));
          } else {
            doc.add(new TextField("body", "other_term token", Field.Store.NO));
          }
          long scoreVal = random.nextInt(100000);
          doc.add(new NumericDocValuesField("score_field", scoreVal));
          writer.addDocument(doc);
        }
        writer.commit();
      }

      reader = DirectoryReader.open(dir);
      searcher = new IndexSearcher(reader);

      Query baseQuery = new TermQuery(new Term("body", "match_term"));
      DoubleValuesSource valueSource = DoubleValuesSource.fromLongField("score_field");
      functionScoreQuery = new FunctionScoreQuery(baseQuery, valueSource);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
      reader.close();
      dir.close();
    }
  }

  @Benchmark
  public TopDocs searchFunctionScoreQuery(BenchmarkState state) throws IOException {
    return state.searcher.search(state.functionScoreQuery, state.topK);
  }
}
