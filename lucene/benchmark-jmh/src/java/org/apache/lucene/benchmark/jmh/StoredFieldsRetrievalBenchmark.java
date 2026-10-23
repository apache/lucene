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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
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

/**
 * Benchmarks stored fields retrieval with the default codec (BEST_SPEED) for documents of different
 * shapes: an id plus a source blob per document, as stored by search engines built on Lucene.
 * {@link #randomAccess} fetches a sorted batch of random documents through a fresh {@link
 * StoredFields}, like a search fetch phase; {@link #mergeInstance} reads all documents
 * sequentially.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@Fork(
    value = 1,
    jvmArgsAppend = {"-Xmx2g", "-Xms2g", "-XX:+AlwaysPreTouch"})
public class StoredFieldsRetrievalBenchmark {

  /**
   * Shape of the stored source: small JSON log lines, English-like text, sparse vectors as JSON
   * (token to weight), dense vectors as JSON, and incompressible bytes.
   */
  @Param({"json_logs", "text", "sparse_vector_json", "dense_vector_json", "random"})
  public String docType;

  @Param({"10"})
  public int batchSize;

  private Path path;
  private Directory dir;
  private DirectoryReader reader;
  private LeafReader leaf;
  private int[][] batches;
  private int nextBatch;
  private final Visitor visitor = new Visitor();

  @Setup(Level.Trial)
  public void setup() throws IOException {
    path = Files.createTempDirectory("storedFieldsRetrieval");
    dir = MMapDirectory.open(path);
    Random random = new Random(0);
    String[] vocabulary = vocabulary(random, 10_000);
    // ~200MB of raw source per doc type, so the index does not fit in CPU caches
    long targetBytes = 200L << 20;
    try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
      long bytes = 0;
      for (int id = 0; bytes < targetBytes; id++) {
        byte[] source = source(random, vocabulary);
        Document doc = new Document();
        doc.add(new StoredField("_id", Integer.toString(id).getBytes(StandardCharsets.UTF_8)));
        doc.add(new StoredField("_source", source));
        writer.addDocument(doc);
        bytes += source.length;
      }
      writer.forceMerge(1);
    }
    reader = DirectoryReader.open(dir);
    leaf = reader.leaves().get(0).reader();

    batches = new int[1024][batchSize];
    for (int[] batch : batches) {
      for (int i = 0; i < batchSize; i++) {
        batch[i] = random.nextInt(leaf.maxDoc());
      }
      Arrays.sort(batch);
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    reader.close();
    dir.close();
    try (Stream<Path> walk = Files.walk(path)) {
      walk.sorted(Comparator.reverseOrder())
          .forEach(
              p -> {
                try {
                  Files.delete(p);
                } catch (IOException _) {
                }
              });
    }
  }

  @Benchmark
  public long randomAccess() throws IOException {
    int[] batch = batches[nextBatch++ & (batches.length - 1)];
    StoredFields storedFields = leaf.storedFields();
    for (int doc : batch) {
      storedFields.document(doc, visitor);
    }
    return visitor.bytes;
  }

  @Benchmark
  @Measurement(iterations = 5, time = 10)
  public long mergeInstance() throws IOException {
    StoredFieldsReader mergeReader = ((CodecReader) leaf).getFieldsReader().getMergeInstance();
    for (int doc = 0; doc < leaf.maxDoc(); doc++) {
      mergeReader.document(doc, visitor);
    }
    return visitor.bytes;
  }

  private byte[] source(Random random, String[] vocabulary) {
    StringBuilder sb = new StringBuilder();
    switch (docType) {
      case "json_logs" ->
          sb.append(
              String.format(
                  Locale.ROOT,
                  "{\"@timestamp\":%d,\"clientip\":\"%d.%d.%d.%d\",\"request\":\"GET /%s/%s.html"
                      + " HTTP/1.0\",\"status\":%d,\"size\":%d}",
                  893964617 + random.nextInt(1_000_000),
                  random.nextInt(256),
                  random.nextInt(256),
                  random.nextInt(256),
                  random.nextInt(256),
                  zipf(random, vocabulary),
                  zipf(random, vocabulary),
                  random.nextInt(10) == 0 ? 404 : 200,
                  random.nextInt(100_000)));
      case "text" -> {
        sb.append("{\"body\":\"");
        for (int i = 0, n = 100 + random.nextInt(200); i < n; i++) {
          sb.append(zipf(random, vocabulary)).append(i % 15 == 14 ? ". " : " ");
        }
        sb.append("\"}");
      }
      case "sparse_vector_json" -> {
        sb.append("{\"embedding\":{");
        int token = 0;
        for (int i = 0, n = 100 + random.nextInt(100); i < n; i++) {
          token += 1 + random.nextInt(200);
          sb.append(i == 0 ? "\"" : ",\"").append(token).append("\":");
          sb.append(String.format(Locale.ROOT, "%.3f", random.nextFloat() * 2));
        }
        sb.append("}}");
      }
      case "dense_vector_json" -> {
        sb.append("{\"embedding\":[");
        for (int i = 0; i < 768; i++) {
          sb.append(i == 0 ? "" : ",").append(random.nextFloat() * 2 - 1);
        }
        sb.append("]}");
      }
      case "random" -> {
        byte[] bytes = new byte[1024];
        random.nextBytes(bytes);
        return bytes;
      }
      default -> throw new IllegalArgumentException(docType);
    }
    return sb.toString().getBytes(StandardCharsets.UTF_8);
  }

  private static String[] vocabulary(Random random, int size) {
    String[] words = new String[size];
    for (int i = 0; i < size; i++) {
      char[] chars = new char[2 + random.nextInt(9)];
      for (int j = 0; j < chars.length; j++) {
        chars[j] = (char) ('a' + random.nextInt(26));
      }
      words[i] = new String(chars);
    }
    return words;
  }

  /** Picks a word with a roughly Zipfian distribution, as in natural language. */
  private static String zipf(Random random, String[] vocabulary) {
    return vocabulary[(int) Math.pow(vocabulary.length, random.nextDouble()) - 1];
  }

  private static final class Visitor extends StoredFieldVisitor {
    long bytes;

    @Override
    public Status needsField(FieldInfo fieldInfo) {
      return Status.YES;
    }

    @Override
    public void binaryField(FieldInfo fieldInfo, byte[] value) {
      bytes += value.length;
    }
  }
}
