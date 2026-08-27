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
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.NamedThreadFactory;

public class TestReturnFieldsRetriever extends LuceneTestCase {

  private static final Executor DIRECT_EXECUTOR = Runnable::run;

  /** Stored "title" for the doc whose global id is {@code globalId}. */
  private static String title(int globalId) {
    return "doc-" + globalId;
  }

  /** A String-payload factory: reads the stored "title" field of each doc. */
  private static ReturnFieldsRetriever.LeafVisitorFactory<String> titleFactory() {
    return leaf -> {
      StoredFields storedFields = leaf.reader().storedFields();
      return localDoc -> storedFields.document(localDoc).get("title");
    };
  }

  public void testEmptyDocIds() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
      for (int i = 0; i < 5; i++) {
        writer.addDocument(docWith(i));
      }
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        String[] result =
            ReturnFieldsRetriever.retrieve(
                reader, new int[0], titleFactory(), String[]::new, DIRECT_EXECUTOR);
        assertEquals(0, result.length);
      }
    }
  }

  /** A generic (String) payload, unsorted input across segments, verifying input-order output. */
  public void testStringPayloadPreservesInputOrder() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter writer =
            new IndexWriter(
                dir, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
      // 3 segments: docs 0-9, 10-19, 20-29.
      for (int seg = 0; seg < 3; seg++) {
        for (int i = 0; i < 10; i++) {
          writer.addDocument(docWith(seg * 10 + i));
        }
        writer.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        assertEquals(3, reader.leaves().size());

        int[] docIds = {25, 3, 17, 9};
        String[] result =
            ReturnFieldsRetriever.retrieve(
                reader, docIds, titleFactory(), String[]::new, DIRECT_EXECUTOR);

        assertEquals(docIds.length, result.length);
        for (int i = 0; i < docIds.length; i++) {
          assertEquals(title(docIds[i]), result[i]);
        }
      }
    }
  }

  /**
   * The engine is payload-agnostic: an Integer payload computed from a doc-value goes through the
   * same machinery with no double-specific coupling.
   */
  public void testIntegerPayload() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
      for (int i = 0; i < 20; i++) {
        writer.addDocument(docWith(i));
      }
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        int[] docIds = {7, 2, 15};
        ReturnFieldsRetriever.LeafVisitorFactory<Integer> factory =
            leaf -> {
              NumericDocValues dv = leaf.reader().getNumericDocValues("id");
              return localDoc -> {
                boolean present = dv.advanceExact(localDoc);
                assertTrue(present);
                return (int) dv.longValue();
              };
            };
        Integer[] result =
            ReturnFieldsRetriever.retrieve(
                reader, docIds, factory, Integer[]::new, DIRECT_EXECUTOR);

        assertEquals(docIds.length, result.length);
        for (int i = 0; i < docIds.length; i++) {
          assertEquals(Integer.valueOf(docIds[i]), result[i]);
        }
      }
    }
  }

  public void testNullArgumentsRejected() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
      writer.addDocument(docWith(0));
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        ReturnFieldsRetriever.LeafVisitorFactory<String> factory = titleFactory();
        int[] docIds = {0};
        expectThrows(
            NullPointerException.class,
            () ->
                ReturnFieldsRetriever.retrieve(
                    null, docIds, factory, String[]::new, DIRECT_EXECUTOR));
        expectThrows(
            NullPointerException.class,
            () ->
                ReturnFieldsRetriever.retrieve(
                    reader, null, factory, String[]::new, DIRECT_EXECUTOR));
        expectThrows(
            NullPointerException.class,
            () ->
                ReturnFieldsRetriever.retrieve(
                    reader, docIds, null, String[]::new, DIRECT_EXECUTOR));
        expectThrows(
            NullPointerException.class,
            () ->
                ReturnFieldsRetriever.retrieve(reader, docIds, factory, null, DIRECT_EXECUTOR));
        expectThrows(
            NullPointerException.class,
            () ->
                ReturnFieldsRetriever.retrieve(reader, docIds, factory, String[]::new, null));
      }
    }
  }

  /** Randomized round-trip on a real thread pool, verifying input order and non-mutation. */
  public void testRandomizedConcurrent() throws IOException {
    ExecutorService executor =
        Executors.newFixedThreadPool(
            TestUtil.nextInt(random(), 2, 5),
            new NamedThreadFactory(TestReturnFieldsRetriever.class.getSimpleName()));
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
            int numHits = random().nextInt(totalDocs + 1);
            Set<Integer> hitSet = new HashSet<>();
            while (hitSet.size() < numHits) {
              hitSet.add(random().nextInt(totalDocs));
            }
            int[] docIds = hitSet.stream().mapToInt(Integer::intValue).toArray();
            for (int i = docIds.length - 1; i > 0; i--) {
              int j = random().nextInt(i + 1);
              int tmp = docIds[i];
              docIds[i] = docIds[j];
              docIds[j] = tmp;
            }
            int[] inputCopy = docIds.clone();

            String[] result =
                ReturnFieldsRetriever.retrieve(
                    reader, docIds, titleFactory(), String[]::new, executor);

            assertArrayEquals("input must not be mutated", inputCopy, docIds);
            assertEquals(docIds.length, result.length);
            for (int i = 0; i < docIds.length; i++) {
              assertEquals(title(docIds[i]), result[i]);
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
    doc.add(new StoredField("title", title(globalId)));
    doc.add(new NumericDocValuesField("id", globalId));
    return doc;
  }
}
