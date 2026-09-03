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
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
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
import org.openjdk.jmh.annotations.Param;
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
public class NearestNeighborBenchmark {

  @Param({"1", "10", "100"})
  private int topN;

  @Param({"100000", "1000000"})
  private int numDocs;

  private static final int NUM_QUERY_POINTS = 1000;
  private static final long SEED = 0xDEADBEEFCAFEBABEL;

  private Directory dir;
  private IndexReader reader;
  private IndexSearcher searcher;
  private double[] queryLats;
  private double[] queryLons;
  private int queryIndex;

  @Setup(Level.Trial)
  public void setUp() throws IOException {
    Random random = new Random(SEED);
    dir = new MMapDirectory(java.nio.file.Files.createTempDirectory("benchmark"));
    IndexWriterConfig config = new IndexWriterConfig();
    try (IndexWriter writer = new IndexWriter(dir, config)) {
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        double lat = (random.nextDouble() * 180.0) - 90.0;
        double lon = (random.nextDouble() * 360.0) - 180.0;
        doc.add(new LatLonPoint("point", lat, lon));
        writer.addDocument(doc);
      }
    }
    reader = DirectoryReader.open(dir);
    searcher = new IndexSearcher(reader);

    queryLats = new double[NUM_QUERY_POINTS];
    queryLons = new double[NUM_QUERY_POINTS];
    for (int i = 0; i < NUM_QUERY_POINTS; i++) {
      queryLats[i] = (random.nextDouble() * 180.0) - 90.0;
      queryLons[i] = (random.nextDouble() * 360.0) - 180.0;
    }
    queryIndex = 0;
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    reader.close();
    dir.close();
  }

  @Benchmark
  public TopDocs benchmarkNearest() throws IOException {
    double queryLat = queryLats[queryIndex];
    double queryLon = queryLons[queryIndex];
    queryIndex = (queryIndex + 1) % NUM_QUERY_POINTS;
    return LatLonPoint.nearest(searcher, "point", queryLat, queryLon, topN);
  }
}
