/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.benchmark.jmh;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.codecs.ApproximateDocGraphBuilder;
import org.apache.lucene.codecs.DocGraphBuilder;
import org.apache.lucene.codecs.SparseEdgeGraph;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.openjdk.jmh.annotations.*;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@Fork(1)
public class BinningGraphBuilderBenchmark {

  @Param({"10000", "50000"})
  private int docCount;

  @Param({"exact", "approx"})
  private String mode;

  private Path tempDir;
  private Directory directory;
  private LeafReader leaf;

  @Setup(Level.Invocation)
  public void setup() throws IOException {
    tempDir = Files.createTempDirectory("binning-benchmark");
    directory = new MMapDirectory(tempDir);
    FieldType fieldType = new FieldType(TextField.TYPE_NOT_STORED);
    fieldType.setTokenized(true);
    fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    fieldType.freeze();

    IndexWriterConfig config = new IndexWriterConfig(new SingleTokenAnalyzer());
    config.setUseCompoundFile(false);
    try (IndexWriter writer = new IndexWriter(directory, config)) {
      for (int i = 0; i < docCount; i++) {
        Document doc = new Document();
        String text = "token_" + (i % 100) + " filler text " + i;
        doc.add(new Field("field", text, fieldType));
        writer.addDocument(doc);
      }
      writer.commit();
    }

    leaf = DirectoryReader.open(directory).leaves().get(0).reader();
  }

  @Benchmark
  public SparseEdgeGraph buildGraph() throws IOException {
    if ("approx".equals(mode)) {
      return new ApproximateDocGraphBuilder("field", 10).build(leaf);
    } else {
      return new DocGraphBuilder("field", 10).build(leaf);
    }
  }

  @TearDown(Level.Invocation)
  public void cleanup() throws IOException {
    if (directory != null) {
      directory.close();
    }
    if (tempDir != null && Files.exists(tempDir)) {
      try (Stream<Path> paths = Files.walk(tempDir)) {
        paths
            .sorted(Comparator.reverseOrder())
            .forEach(
                path -> {
                  try {
                    Files.deleteIfExists(path);
                  } catch (IOException _) {
                  }
                });
      }
    }
  }

  private static final class SingleTokenAnalyzer extends Analyzer {
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
      Tokenizer tokenizer =
          new Tokenizer() {
            private final CharTermAttribute attr = addAttribute(CharTermAttribute.class);
            private boolean done = false;

            @Override
            public boolean incrementToken() throws IOException {
              if (done) return false;
              clearAttributes();
              attr.append("lucene");
              done = true;
              return true;
            }

            @Override
            public void reset() throws IOException {
              super.reset();
              done = false;
            }
          };
      return new TokenStreamComponents(tokenizer);
    }

    @Override
    protected Reader initReader(String fieldName, Reader reader) {
      return reader;
    }
  }
}
