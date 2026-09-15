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
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestReaderUtil extends LuceneTestCase {

  public void testEmpty() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
      writer.addDocument(new Document());
      writer.addDocument(new Document());
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        List<LeafReaderContext> leaves = reader.leaves();
        ReaderUtil.PartitionedHits partitioned = ReaderUtil.partitionByLeaf(new int[0], leaves);
        assertEquals(leaves.size(), partitioned.docIdsByLeaf().length);
        assertEquals(leaves.size(), partitioned.ordinalsByLeaf().length);
        for (int i = 0; i < leaves.size(); i++) {
          assertEquals(0, partitioned.docIdsByLeaf()[i].length);
          assertEquals(0, partitioned.ordinalsByLeaf()[i].length);
        }
      }
    }
  }

  /**
   * Worked two-segment example with deliberately unsorted input: verifies per-leaf doc IDs are
   * sorted, ordinals point back into the original input array, and the round-trip invariant holds.
   */
  public void testTracksOriginalPositions() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter writer =
            new IndexWriter(dir, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
      for (int i = 0; i < 10; i++) {
        writer.addDocument(new Document());
      }
      writer.commit();
      for (int i = 0; i < 10; i++) {
        writer.addDocument(new Document());
      }
      writer.commit();

      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        List<LeafReaderContext> leaves = reader.leaves();
        assertEquals(2, leaves.size());

        // Input order is intentionally not docId-sorted; we want to verify ordinals point back
        // into this array (and that the per-leaf doc IDs come out sorted).
        int[] docIds = {18, 2, 10, 9};
        ReaderUtil.PartitionedHits partitioned = ReaderUtil.partitionByLeaf(docIds, leaves);

        // Leaf 0 holds docs 0-9: {2 (ord 1), 9 (ord 3)}
        assertArrayEquals(new int[] {2, 9}, partitioned.docIdsByLeaf()[0]);
        assertArrayEquals(new int[] {1, 3}, partitioned.ordinalsByLeaf()[0]);

        // Leaf 1 holds docs 10-19: {10 (ord 2), 18 (ord 0)}
        assertArrayEquals(new int[] {10, 18}, partitioned.docIdsByLeaf()[1]);
        assertArrayEquals(new int[] {2, 0}, partitioned.ordinalsByLeaf()[1]);

        // Round-trip: docIdsByLeaf[k][i] == globalDocIds[ordinalsByLeaf[k][i]]
        for (int k = 0; k < leaves.size(); k++) {
          int[] leafDocs = partitioned.docIdsByLeaf()[k];
          int[] leafOrds = partitioned.ordinalsByLeaf()[k];
          assertEquals(leafDocs.length, leafOrds.length);
          for (int i = 0; i < leafDocs.length; i++) {
            assertEquals(docIds[leafOrds[i]], leafDocs[i]);
          }
        }
      }
    }
  }

  /**
   * Leaves with no hits (including an empty middle leaf and trailing empty leaves) come back empty.
   */
  public void testSkipsSegmentsWithNoHits() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter writer =
            new IndexWriter(dir, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
      // Create 4 segments (docs 0-9, 10-19, 20-29, 30-39).
      for (int seg = 0; seg < 4; seg++) {
        for (int i = 0; i < 10; i++) {
          writer.addDocument(new Document());
        }
        writer.commit();
      }

      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        List<LeafReaderContext> leaves = reader.leaves();
        assertEquals(4, leaves.size());

        // Hits only in the first and third segments: empty middle leaf (1) and trailing empty
        // leaf (3). Input unsorted to also exercise sorting/ordinals.
        int[] docIds = {25, 3};
        ReaderUtil.PartitionedHits partitioned = ReaderUtil.partitionByLeaf(docIds, leaves);

        assertEquals(4, partitioned.docIdsByLeaf().length);
        assertEquals(4, partitioned.ordinalsByLeaf().length);

        assertArrayEquals(new int[] {3}, partitioned.docIdsByLeaf()[0]);
        assertArrayEquals(new int[] {1}, partitioned.ordinalsByLeaf()[0]); // 3 was input index 1
        assertEquals(0, partitioned.docIdsByLeaf()[1].length); // empty middle leaf
        assertEquals(0, partitioned.ordinalsByLeaf()[1].length);
        assertArrayEquals(new int[] {25}, partitioned.docIdsByLeaf()[2]);
        assertArrayEquals(new int[] {0}, partitioned.ordinalsByLeaf()[2]); // 25 was input index 0
        assertEquals(0, partitioned.docIdsByLeaf()[3].length); // trailing empty leaf
        assertEquals(0, partitioned.ordinalsByLeaf()[3].length);
      }
    }
  }

  public void testDoesNotMutateInput() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
      for (int i = 0; i < 10; i++) {
        writer.addDocument(new Document());
      }
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        List<LeafReaderContext> leaves = reader.leaves();

        int[] docIds = {9, 3, 0, 5};
        int[] copy = docIds.clone();
        ReaderUtil.partitionByLeaf(docIds, leaves);
        assertArrayEquals(copy, docIds);
      }
    }
  }

  public void testRandomized() throws IOException {
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

          // Random hits (0 to totalDocs inclusive - covers empty and all-match).
          int numHits = random().nextInt(totalDocs + 1);
          Set<Integer> hitSet = new HashSet<>();
          while (hitSet.size() < numHits) {
            hitSet.add(random().nextInt(totalDocs));
          }
          int[] docIds = hitSet.stream().mapToInt(Integer::intValue).toArray();

          ReaderUtil.PartitionedHits partitioned = ReaderUtil.partitionByLeaf(docIds, leaves);
          int[][] docIdsByLeaf = partitioned.docIdsByLeaf();
          int[][] ordinalsByLeaf = partitioned.ordinalsByLeaf();

          assertEquals(numSegments, docIdsByLeaf.length);
          assertEquals(numSegments, ordinalsByLeaf.length);

          // Total hits preserved.
          int totalResultDocs = Arrays.stream(docIdsByLeaf).mapToInt(a -> a.length).sum();
          assertEquals(docIds.length, totalResultDocs);

          for (int leafIdx = 0; leafIdx < docIdsByLeaf.length; leafIdx++) {
            int[] leafDocs = docIdsByLeaf[leafIdx];
            int[] leafOrds = ordinalsByLeaf[leafIdx];
            assertEquals(leafDocs.length, leafOrds.length);
            LeafReaderContext leaf = leaves.get(leafIdx);
            int docBase = leaf.docBase;
            int maxDoc = leaf.reader().maxDoc();

            for (int i = 0; i < leafDocs.length; i++) {
              int docId = leafDocs[i];
              // docId belongs to this leaf
              assertTrue(docId >= docBase && docId < docBase + maxDoc);
              // sorted ascending within leaf
              if (i > 0) {
                assertTrue(leafDocs[i] > leafDocs[i - 1]);
              }
              // ordinal points back to the original input
              assertEquals(docIds[leafOrds[i]], docId);
            }
          }
        }
      }
    }
  }
}
