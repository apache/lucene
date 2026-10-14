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
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.BinarySortField;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
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
 * Measures the cost of building an index sorted on a single-valued bytes field, comparing {@link
 * BinarySortField} (over {@link org.apache.lucene.index.BinaryDocValues BinaryDocValues}) against a
 * {@code SortField.Type.STRING} sort (over {@link org.apache.lucene.index.SortedDocValues
 * SortedDocValues}).
 *
 * <p>Indexing uses the default flush and merge configuration, so documents are flushed naturally
 * and the merge policy schedules merges as the index grows. {@link #indexWithNaturalMerges}
 * measures the time to index every document and wait for the resulting merges to finish; {@link
 * #indexThenForceMerge} additionally force-merges down to a single segment.
 *
 * <p>The {@code cardinality} parameter controls how many distinct values the field holds, and
 * {@code valueLength} the length in bytes of each value.
 */
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@Fork(
    value = 1,
    jvmArgsAppend = {"-Xmx8g", "-Xms8g", "-XX:+AlwaysPreTouch"})
public class IndexSortDocValuesBenchmark {

  @Param({"1000000"})
  public int docCount;

  @Param({"16"})
  public int valueLength;

  /** "binary" uses {@link BinarySortField}; "sorted" uses {@code SortField.Type.STRING}. */
  @Param({"binary", "sorted"})
  public String dvType;

  /** "low" cycles a small set of distinct values; "high" gives each document a unique value. */
  @Param({"low", "high"})
  public String cardinality;

  private byte[][] values;
  private Path path;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    Random random = new Random(42);
    int distinct = cardinality.equals("low") ? 128 : docCount;
    byte[][] pool = new byte[distinct][];
    for (int i = 0; i < distinct; i++) {
      byte[] b = new byte[valueLength];
      random.nextBytes(b);
      pool[i] = b;
    }
    // assign one value per doc; shuffle so the field is unsorted on input
    values = new byte[docCount][];
    for (int i = 0; i < docCount; i++) {
      values[i] = pool[cardinality.equals("low") ? random.nextInt(distinct) : i];
    }
    for (int i = docCount - 1; i > 0; i--) {
      int j = random.nextInt(i + 1);
      byte[] tmp = values[i];
      values[i] = values[j];
      values[j] = tmp;
    }
    path = Files.createTempDirectory("indexSortDocValuesBenchmark");
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    IOUtils.rm(path);
  }

  private Sort indexSort() {
    if (dvType.equals("binary")) {
      return new Sort(new BinarySortField("dv", false));
    } else {
      return new Sort(new SortField("dv", SortField.Type.STRING, false));
    }
  }

  private long index(boolean forceMerge) throws IOException {
    Path runPath = Files.createTempDirectory(path, dvType);
    try (Directory dir = new MMapDirectory(runPath)) {
      IndexWriterConfig iwc = new IndexWriterConfig().setIndexSort(indexSort());
      long checksum;
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        Document doc = new Document();
        ReusableField field = dvType.equals("binary") ? new BinaryField() : new SortedField();
        doc.add(field.field());
        for (int i = 0; i < docCount; i++) {
          field.setValue(values[i]);
          w.addDocument(doc);
        }
        if (forceMerge) {
          w.forceMerge(1);
        }
        checksum = w.getDocStats().maxDoc;
        // Closing the writer (try-with-resources) flushes, runs and awaits the pending merges that
        // the merge policy scheduled while indexing, and commits, so the close is part of the
        // measured region.
      }
      return checksum;
    } finally {
      IOUtils.rm(runPath);
    }
  }

  @Benchmark
  public long indexWithNaturalMerges() throws IOException {
    return index(false);
  }

  @Benchmark
  public long indexThenForceMerge() throws IOException {
    return index(true);
  }

  /** Small abstraction over the two reusable doc-values field types. */
  private interface ReusableField {
    Field field();

    void setValue(byte[] value);
  }

  private static final class BinaryField implements ReusableField {
    private final BinaryDocValuesField field = new BinaryDocValuesField("dv", new BytesRef());

    @Override
    public Field field() {
      return field;
    }

    @Override
    public void setValue(byte[] value) {
      field.setBytesValue(value);
    }
  }

  private static final class SortedField implements ReusableField {
    private final SortedDocValuesField field = new SortedDocValuesField("dv", new BytesRef());

    @Override
    public Field field() {
      return field;
    }

    @Override
    public void setValue(byte[] value) {
      field.setBytesValue(value);
    }
  }
}
