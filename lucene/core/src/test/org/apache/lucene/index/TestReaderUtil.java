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
import java.util.List;
import org.apache.lucene.document.Document;
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
        int[][] result = ReaderUtil.partitionByLeaf(new int[0], leaves);
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

        int[] docIds = {0, 3, 5, 9};
        int[][] result = ReaderUtil.partitionByLeaf(docIds, leaves);

        assertEquals(1, result.length);
        assertArrayEquals(docIds, result[0]);
      }
    }
  }

  public void testPartitionByLeafMultipleSegments() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
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
        int[] docIds = {2, 9, 10, 18};
        int[][] result = ReaderUtil.partitionByLeaf(docIds, leaves);

        assertEquals(2, result.length);
        // First segment: docs 0-9
        assertArrayEquals(new int[] {2, 9}, result[0]);
        // Second segment: docs 10-19
        assertArrayEquals(new int[] {10, 18}, result[1]);
      }
    }
  }

  public void testPartitionByLeafSkipsEmptySegments() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
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
        int[] docIds = {3, 25};
        int[][] result = ReaderUtil.partitionByLeaf(docIds, leaves);

        assertEquals(3, result.length);
        assertArrayEquals(new int[] {3}, result[0]);
        assertEquals(0, result[1].length); // middle segment has no hits
        assertArrayEquals(new int[] {25}, result[2]);
      }
    }
  }
}
