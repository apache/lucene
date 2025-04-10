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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.codecs.ApproximateDocBinner;
import org.apache.lucene.codecs.ApproximateDocGraphBuilder;
import org.apache.lucene.codecs.DocBinningGraphBuilder;
import org.apache.lucene.codecs.DocGraphBuilder;
import org.apache.lucene.codecs.FieldsLeafReader;
import org.apache.lucene.codecs.SparseEdgeGraph;
import org.apache.lucene.codecs.lucene101.Lucene101Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Terms;
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
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@Fork(1)
@State(Scope.Thread)
public class DocBinningCostBenchmark {

  private static final String FIELD = "field";
  private static final String TOKEN = "lucene";

  private Directory directory;
  private IndexReader reader;
  private LeafReader leafReader;

  @Param({"10000"})
  private int docCount;

  @Param({"4"})
  private int binCount;

  @Setup(Level.Trial)
  public void setup() throws Exception {
    Path temp = Files.createTempDirectory("dcc-binning");
    directory = new MMapDirectory(temp);
    IndexWriterConfig config = new IndexWriterConfig(new SingleTokenAnalyzer());
    config.setUseCompoundFile(false);
    config.setCodec(new Lucene101Codec());

    FieldType type = new FieldType();
    type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    type.setTokenized(true);
    type.setStored(false);
    type.putAttribute("doBinning", "true");
    type.putAttribute("bin.count", Integer.toString(binCount));
    type.freeze();

    try (IndexWriter writer = new IndexWriter(directory, config)) {
      for (int i = 0; i < docCount; i++) {
        Document doc = new Document();
        doc.add(new Field(FIELD, TOKEN, type));
        writer.addDocument(doc);
      }
      writer.commit();
    }

    reader = DirectoryReader.open(directory);
    Terms terms = reader.leaves().get(0).reader().terms(FIELD);
    leafReader = new FieldsLeafReader(java.util.Collections.singletonMap(FIELD, terms), docCount);
  }

  @Benchmark
  public int[] fullGraphBinning() throws IOException {
    DocGraphBuilder builder = new DocGraphBuilder(FIELD, DocGraphBuilder.DEFAULT_MAX_EDGES);
    SparseEdgeGraph graph = builder.build(leafReader);
    return DocBinningGraphBuilder.computeBins(graph, docCount, binCount);
  }

  @Benchmark
  public int[] approximateGraphBinning() throws IOException {
    ApproximateDocGraphBuilder builder =
        new ApproximateDocGraphBuilder(FIELD, DocGraphBuilder.DEFAULT_MAX_EDGES);
    SparseEdgeGraph graph = builder.build(leafReader);
    return ApproximateDocBinner.assign(graph, docCount, binCount);
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    reader.close();
    directory.close();
  }

  /** Minimal synthetic analyzer that emits a fixed token — avoids analyzer module dependency. */
  private static final class SingleTokenAnalyzer extends Analyzer {
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
      Tokenizer tokenizer =
          new Tokenizer() {
            private final CharTermAttribute attr = addAttribute(CharTermAttribute.class);
            private boolean emitted = false;

            @Override
            public boolean incrementToken() {
              if (emitted) return false;
              clearAttributes();
              attr.append(TOKEN);
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
