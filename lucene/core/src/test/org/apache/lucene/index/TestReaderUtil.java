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
package org.apache.lucene.index;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestReaderUtil extends LuceneTestCase {

  public void testPartitionByLeafEmptyDocIds() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
      writer.addDocument(new Document());
      writer.addDocument(new Document());
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        List<LeafReaderContext> leaves = reader.leaves();
        int[][] result = ReaderUtil.partitionByLeaf(new ScoreDoc[0], leaves);
        assertEquals(leaves.size(), result.length);
        for (int[] leaf : result) {
          assertEquals(0, leaf.length);
        }
      }
    }
  }

  public void testPartitionByLeafSingleSegment() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
      for (int i = 0; i < 10; i++) {
        writer.addDocument(new Document());
      }
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        List<LeafReaderContext> leaves = reader.leaves();
        assertEquals(1, leaves.size());

        ScoreDoc[] hits = {
          new ScoreDoc(0, 1f), new ScoreDoc(3, 1f), new ScoreDoc(5, 1f), new ScoreDoc(9, 1f)
        };
        int[][] result = ReaderUtil.partitionByLeaf(hits, leaves);

        assertEquals(1, result.length);
        assertArrayEquals(new int[] {0, 3, 5, 9}, result[0]);
      }
    }
  }

  public void testPartitionByLeafMultipleSegments() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter writer =
            new IndexWriter(dir, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
      for (int i = 0; i < 10; i++) {
        writer.addDocument(new Document());
      }
      writer.commit();

      // Create second segment
      for (int i = 0; i < 10; i++) {
        writer.addDocument(new Document());
      }
      writer.commit();

      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        List<LeafReaderContext> leaves = reader.leaves();
        assertEquals(2, leaves.size());

        // Hits in both segments
        ScoreDoc[] hits = {
          new ScoreDoc(2, 1f), new ScoreDoc(9, 1f), new ScoreDoc(10, 1f), new ScoreDoc(18, 1f)
        };
        int[][] result = ReaderUtil.partitionByLeaf(hits, leaves);

        assertEquals(2, result.length);
        // First segment: docs 0-9
        assertArrayEquals(new int[] {2, 9}, result[0]);
        // Second segment: docs 10-19
        assertArrayEquals(new int[] {10, 18}, result[1]);
      }
    }
  }

  public void testPartitionByLeafSkipsSegmentsWithNoHits() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter writer =
            new IndexWriter(dir, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
      // Create 3 segments
      for (int seg = 0; seg < 3; seg++) {
        for (int i = 0; i < 10; i++) {
          writer.addDocument(new Document());
        }
        writer.commit();
      }

      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        List<LeafReaderContext> leaves = reader.leaves();
        assertEquals(3, leaves.size());

        // Hits only in first and third segment (skip middle)
        ScoreDoc[] hits = {new ScoreDoc(3, 1f), new ScoreDoc(25, 1f)};
        int[][] result = ReaderUtil.partitionByLeaf(hits, leaves);

        assertEquals(3, result.length);
        assertArrayEquals(new int[] {3}, result[0]);
        assertEquals(0, result[1].length); // middle segment has no hits
        assertArrayEquals(new int[] {25}, result[2]);
      }
    }
  }

  public void testPartitionByLeafRandomized() throws IOException {
    for (int iter = 0; iter < 100; iter++) {
      int numSegments = random().nextInt(10) + 1;
      int totalDocs = 0;
      int[] docsPerSegment = new int[numSegments];
      for (int i = 0; i < numSegments; i++) {
        docsPerSegment[i] = random().nextInt(100) + 1;
        totalDocs += docsPerSegment[i];
      }

      try (Directory dir = newDirectory();
          IndexWriter writer =
              new IndexWriter(
                  dir, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
        for (int seg = 0; seg < numSegments; seg++) {
          for (int i = 0; i < docsPerSegment[seg]; i++) {
            writer.addDocument(new Document());
          }
          writer.commit();
        }

        try (DirectoryReader reader = DirectoryReader.open(writer)) {
          List<LeafReaderContext> leaves = reader.leaves();
          assertEquals(numSegments, leaves.size());

          // Generate random hits (0 to totalDocs inclusive - covers empty and all-match)
          int numHits = random().nextInt(totalDocs + 1);
          Set<Integer> hitSet = new HashSet<>();
          while (hitSet.size() < numHits) {
            hitSet.add(random().nextInt(totalDocs));
          }
          int[] docIds = hitSet.stream().mapToInt(Integer::intValue).toArray();
          ScoreDoc[] hits = new ScoreDoc[docIds.length];
          for (int i = 0; i < docIds.length; i++) {
            hits[i] = new ScoreDoc(docIds[i], 1f);
          }

          int[][] result = ReaderUtil.partitionByLeaf(hits, leaves);

          // Verify: result length matches leaves
          assertEquals(numSegments, result.length);

          // Verify: total hits preserved
          int totalResultDocs = Arrays.stream(result).mapToInt(a -> a.length).sum();
          assertEquals(docIds.length, totalResultDocs);

          // Verify: each doc in correct leaf and sorted
          for (int leafIdx = 0; leafIdx < result.length; leafIdx++) {
            int[] leafDocs = result[leafIdx];
            LeafReaderContext leaf = leaves.get(leafIdx);
            int docBase = leaf.docBase;
            int maxDoc = leaf.reader().maxDoc();
            for (int i = 0; i < leafDocs.length; i++) {
              int docId = leafDocs[i];
              assertTrue(docId >= docBase && docId < docBase + maxDoc);
              if (i > 0) {
                assertTrue(leafDocs[i] > leafDocs[i - 1]);
              }
            }
          }
        }
      }
    }
  }
}
