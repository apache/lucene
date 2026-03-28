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
import java.util.concurrent.TimeUnit;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 3)
@Measurement(iterations = 10, time = 5)
@Fork(
    value = 3,
    jvmArgsAppend = {"-Xmx2g", "-Xms2g"})
public class PhraseScorerBenchmark {

  private Directory dir;
  private IndexReader reader;
  private IndexSearcher searcher;
  private PhraseQuery exactQuery;
  private PhraseQuery sloppyQuery;

  @Setup(Level.Trial)
  public void setUp() throws IOException {
    dir = new MMapDirectory(java.nio.file.Files.createTempDirectory("benchmark"));
    IndexWriterConfig config = new IndexWriterConfig();
    try (IndexWriter writer = new IndexWriter(dir, config)) {
      // Create a corpus where most docs contain the individual query terms but only a small
      // fraction contain the actual phrase. This maximises the number of documents whose maxFreq
      // upper-bound check allows short-circuiting.
      for (int i = 0; i < 1_000_000; i++) {
        Document doc = new Document();
        if (i % 1000 == 0) {
          // 0.1% of docs: exact phrase match
          doc.add(
              new TextField(
                  "text", "the quick brown fox jumped over the lazy dog", Field.Store.NO));
        } else if (i % 2 == 0) {
          // 50% of docs: terms present but not as a phrase (high freq, no match)
          StringBuilder sb = new StringBuilder("quick ");
          for (int j = 0; j < 100; j++) sb.append("padding ");
          sb.append("fox");
          doc.add(new TextField("text", sb.toString(), Field.Store.NO));
        } else {
          // 50% of docs: no query terms at all
          doc.add(new TextField("text", "unrelated words", Field.Store.NO));
        }
        writer.addDocument(doc);
      }
    }
    reader = DirectoryReader.open(dir);
    searcher = new IndexSearcher(reader);
    exactQuery = new PhraseQuery("text", "quick", "brown", "fox");
    sloppyQuery = new PhraseQuery(10, "text", "quick", "fox");
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    reader.close();
    dir.close();
  }

  @Benchmark
  public TopDocs benchmarkExactTopScores() throws IOException {
    return searcher.search(exactQuery, 10);
  }

  @Benchmark
  public TopDocs benchmarkSloppyTopScores() throws IOException {
    return searcher.search(sloppyQuery, 10);
  }
}
