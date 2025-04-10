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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.codecs.lucene101.Lucene101Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;

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

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@Fork(1)
public class BinningIndexingLatencyBenchmark {

  @Param({"10000"})
  private int docCount;

  @Param({"true", "false"})
  private boolean binningEnabled;

  @Param({"exact", "approx", "auto"})
  private String graphBuilder;

  private Path tempDir;
  private FieldType fieldType;

  @Setup(Level.Trial)
  public void setup() {
    fieldType = new FieldType(TextField.TYPE_NOT_STORED);
    fieldType.setTokenized(true);
    fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);

    if (binningEnabled) {
      fieldType.putAttribute("postingsFormat", "Lucene101");
      fieldType.putAttribute("doBinning", "true");
      fieldType.putAttribute("bin.count", "4");
      fieldType.putAttribute("graph.builder", graphBuilder);
    }

    fieldType.freeze();
  }

  @Benchmark
  public void indexBatch() throws IOException {
    tempDir = Files.createTempDirectory("lucene-binning-benchmark");
    try (Directory dir = new MMapDirectory(tempDir)) {
      IndexWriterConfig config = new IndexWriterConfig(new SingleTokenAnalyzer());
      config.setCodec(new Lucene101Codec());
      config.setUseCompoundFile(false);

      try (IndexWriter writer = new IndexWriter(dir, config)) {
        for (int i = 0; i < docCount; i++) {
          Document doc = new Document();
          String content = (i % 200 == 0) ? "lucene relevant content" : "noise filler text " + i;
          doc.add(new Field("field", content, fieldType));
          writer.addDocument(doc);
        }
        writer.commit();
      }
    } finally {
      Files.walk(tempDir)
          .sorted(Comparator.reverseOrder())
          .forEach(path -> {
            try {
              Files.deleteIfExists(path);
            } catch (IOException ignored) {
              // best-effort cleanup
            }
          });
    }
  }

  private static final class SingleTokenAnalyzer extends Analyzer {
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
      Tokenizer tokenizer = new Tokenizer() {
        private final CharTermAttribute attr = addAttribute(CharTermAttribute.class);
        private boolean emitted = false;

        @Override
        public boolean incrementToken() {
          if (emitted) {
            return false;
          }
          clearAttributes();
          attr.append("lucene");
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

    @Override
    protected Reader initReader(String fieldName, Reader reader) {
      return reader;
    }
  }
}