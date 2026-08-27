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

package org.apache.lucene.search;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.NamedThreadFactory;

public class TestDoubleValuesRetriever extends LuceneTestCase {

  /** Same-thread executor, so the concurrency wrapper is exercised without real threads. */
  private static final Executor DIRECT_EXECUTOR = Runnable::run;

  /**
   * Value stored in field "a" for the doc whose global id is {@code globalId}. Chosen to be a
   * distinctive, invertible function of the global id so the test can predict every value.
   */
  private static long valueA(int globalId) {
    return globalId * 10L + 1;
  }

  /** Value stored in field "b"; a different function so columns can't be confused. */
  private static long valueB(int globalId) {
    return globalId * 100L + 7;
  }

  public void testEmptyDocIds() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
      for (int i = 0; i < 5; i++) {
        writer.addDocument(docWith(i));
      }
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        double[][] values =
            DoubleValuesRetriever.retrieve(
                reader,
                new int[0],
                new DoubleValuesSource[] {DoubleValuesSource.fromLongField("a")},
                DIRECT_EXECUTOR);
        assertEquals(0, values.length);
      }
    }
  }

  /** Single segment, multiple sources, deliberately unsorted input: output must be in input order. */
  public void testPreservesInputOrder() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
      for (int i = 0; i < 10; i++) {
        writer.addDocument(docWith(i));
      }
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        int[] docIds = {5, 4, 3, 2, 1};
        DoubleValuesSource[] sources = {
          DoubleValuesSource.fromLongField("a"), DoubleValuesSource.fromLongField("b")
        };
        double[][] values =
            DoubleValuesRetriever.retrieve(reader, docIds, sources, DIRECT_EXECUTOR);

        assertEquals(docIds.length, values.length);
        for (int i = 0; i < docIds.length; i++) {
          int globalId = docIds[i];
          assertEquals(2, values[i].length);
          assertEquals((double) valueA(globalId), values[i][0], 0.0);
          assertEquals((double) valueB(globalId), values[i][1], 0.0);
        }
      }
    }
  }

  /** A doc missing the requested field yields Double.NaN for that source. */
  public void testMissingValueIsNaN() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
      // Doc 0 has both fields; doc 1 has only "a" (missing "b").
      Document d0 = new Document();
      d0.add(new NumericDocValuesField("a", valueA(0)));
      d0.add(new NumericDocValuesField("b", valueB(0)));
      writer.addDocument(d0);
      Document d1 = new Document();
      d1.add(new NumericDocValuesField("a", valueA(1)));
      writer.addDocument(d1);

      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        DoubleValuesSource[] sources = {
          DoubleValuesSource.fromLongField("a"), DoubleValuesSource.fromLongField("b")
        };
        double[][] values =
            DoubleValuesRetriever.retrieve(reader, new int[] {0, 1}, sources, DIRECT_EXECUTOR);

        assertEquals((double) valueA(0), values[0][0], 0.0);
        assertEquals((double) valueB(0), values[0][1], 0.0);
        assertEquals((double) valueA(1), values[1][0], 0.0);
        assertTrue("missing value should be NaN", Double.isNaN(values[1][1]));
      }
    }
  }

  /** Sources that need scores are rejected up front, since this retriever supplies none. */
  public void testRejectsScoreNeedingSource() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
      writer.addDocument(docWith(0));
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        DoubleValuesSource[] sources = {DoubleValuesSource.SCORES};
        expectThrows(
            IllegalArgumentException.class,
            () ->
                DoubleValuesRetriever.retrieve(
                    reader, new int[] {0}, sources, DIRECT_EXECUTOR));
      }
    }
  }

  /** The adapter's own guards; {@code globalDocIds}/{@code executor} nulls are the engine's contract. */
  public void testNullArgumentsRejected() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
      writer.addDocument(docWith(0));
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        DoubleValuesSource[] sources = {DoubleValuesSource.fromLongField("a")};
        int[] docIds = {0};
        expectThrows(
            NullPointerException.class,
            () -> DoubleValuesRetriever.retrieve(null, docIds, sources, DIRECT_EXECUTOR));
        expectThrows(
            NullPointerException.class,
            () -> DoubleValuesRetriever.retrieve(reader, docIds, null, DIRECT_EXECUTOR));
      }
    }
  }

  /**
   * Randomized round-trip against a real thread pool: build random segments, request a random
   * subset of doc IDs in random order, and verify every returned value matches the known function
   * of its global doc id, in input order.
   */
  public void testRandomizedConcurrent() throws IOException {
    ExecutorService executor =
        Executors.newFixedThreadPool(
            TestUtil.nextInt(random(), 2, 5),
            new NamedThreadFactory(TestDoubleValuesRetriever.class.getSimpleName()));
    try {
      for (int iter = 0; iter < 50; iter++) {
        int numSegments = random().nextInt(6) + 1;
        int totalDocs = 0;
        try (Directory dir = newDirectory();
            IndexWriter writer =
                new IndexWriter(
                    dir, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
          for (int seg = 0; seg < numSegments; seg++) {
            int docsInSeg = random().nextInt(10) + 1;
            for (int i = 0; i < docsInSeg; i++) {
              writer.addDocument(docWith(totalDocs++));
            }
            writer.commit();
          }

          try (DirectoryReader reader = DirectoryReader.open(writer)) {
            // Random subset of global doc ids, in random order.
            int numHits = random().nextInt(totalDocs + 1);
            Set<Integer> hitSet = new HashSet<>();
            while (hitSet.size() < numHits) {
              hitSet.add(random().nextInt(totalDocs));
            }
            int[] docIds = hitSet.stream().mapToInt(Integer::intValue).toArray();
            // Shuffle so input is not docId-sorted.
            for (int i = docIds.length - 1; i > 0; i--) {
              int j = random().nextInt(i + 1);
              int tmp = docIds[i];
              docIds[i] = docIds[j];
              docIds[j] = tmp;
            }
            int[] inputCopy = docIds.clone();

            DoubleValuesSource[] sources = {
              DoubleValuesSource.fromLongField("a"), DoubleValuesSource.fromLongField("b")
            };
            double[][] values =
                DoubleValuesRetriever.retrieve(reader, docIds, sources, executor);

            assertArrayEquals("input array must not be mutated", inputCopy, docIds);
            assertEquals(docIds.length, values.length);
            for (int i = 0; i < docIds.length; i++) {
              int globalId = docIds[i];
              assertEquals(2, values[i].length);
              assertEquals((double) valueA(globalId), values[i][0], 0.0);
              assertEquals((double) valueB(globalId), values[i][1], 0.0);
            }
          }
        }
      }
    } finally {
      executor.shutdown();
    }
  }

  private static Document docWith(int globalId) {
    Document doc = new Document();
    doc.add(new NumericDocValuesField("a", valueA(globalId)));
    doc.add(new NumericDocValuesField("b", valueB(globalId)));
    return doc;
  }
}
