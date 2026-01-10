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
 *
 */
package org.apache.lucene.facet.taxonomy.directory;

import java.io.IOException;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestTaxonomyIndexArrays extends LuceneTestCase {

  private void checkIntArraysEquals(
      TaxonomyIndexArrays.ChunkedIntArray expected, TaxonomyIndexArrays.ChunkedIntArray actual) {
    for (int i = 0; i < expected.values.length - 1; i++) {
      assertSame(expected.values[i], actual.values[i]);
    }
    int lastOldChunk = expected.values.length - 1;
    for (int i = 0; i < expected.values[lastOldChunk].length; i++) {
      assertEquals(expected.values[lastOldChunk][i], actual.values[lastOldChunk][i]);
    }
  }

  private void checkInvariants(TaxonomyIndexArrays oldArray, TaxonomyIndexArrays newArray) {
    TaxonomyIndexArrays.ChunkedIntArray oldParents = oldArray.parents();
    TaxonomyIndexArrays.ChunkedIntArray newParents = newArray.parents();
    checkIntArraysEquals(oldParents, newParents);
    TaxonomyIndexArrays.ChunkedIntArray oldSiblings = oldArray.siblings();
    TaxonomyIndexArrays.ChunkedIntArray newSiblings = newArray.siblings();
    checkIntArraysEquals(oldSiblings, newSiblings);
  }

  public void testRandom() {
    TaxonomyIndexArrays oldArray =
        new TaxonomyIndexArrays(new int[][] {new int[] {TaxonomyReader.INVALID_ORDINAL}});
    int numIterations = 100;
    int ordinal = 1;
    for (int i = 0; i < numIterations; i++) {
      int newOrdinal = ordinal + random().nextInt(TaxonomyIndexArrays.CHUNK_SIZE);
      TaxonomyIndexArrays newArray = oldArray.add(newOrdinal, ordinal);
      checkInvariants(oldArray, newArray);
      ordinal = newOrdinal;
    }
  }

  public void testMultiplesOfChunkSize() {
    TaxonomyIndexArrays oldArray =
        new TaxonomyIndexArrays(new int[][] {new int[] {TaxonomyReader.INVALID_ORDINAL}});
    int numIterations = 20;
    int ordinal = TaxonomyIndexArrays.CHUNK_SIZE;
    for (int i = 0; i < numIterations; i++) {
      int newOrdinal = ordinal + TaxonomyIndexArrays.CHUNK_SIZE;
      TaxonomyIndexArrays newArray = oldArray.add(newOrdinal, ordinal);
      checkInvariants(oldArray, newArray);
      ordinal = newOrdinal;
    }
  }

  public void testConstructFromEmptyIndex() throws IOException {
    Directory dir = newDirectory();

    // Produce empty index
    new IndexWriter(dir, newIndexWriterConfig(null)).close();

    IndexReader reader = DirectoryReader.open(dir);

    TaxonomyIndexArrays tia = new TaxonomyIndexArrays(reader);
    assertEquals(0, tia.parents().length());

    tia = new TaxonomyIndexArrays(reader, tia);
    assertEquals(0, tia.parents().length());

    reader.close();
    dir.close();
  }

  public void testConstructFromIndex() throws IOException {
    Directory dir = newDirectory();
    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(dir);
    TaxonomyIndexArrays oldTia, newTia;
    TaxonomyIndexArrays.ChunkedIntArray oldParents, newParents;

    // Test 1
    // Add one category. The first TIA will have a non-zero length incomplete chunk.
    taxoWriter.addCategory(new FacetLabel("a"));
    taxoWriter.commit();
    try (IndexReader reader = DirectoryReader.open(dir)) {
      oldTia = new TaxonomyIndexArrays(reader);
    }

    oldParents = oldTia.parents();
    assertEquals(2, oldParents.length());
    assertEquals(TaxonomyReader.INVALID_ORDINAL, oldParents.get(0));
    assertEquals(TaxonomyReader.ROOT_ORDINAL, oldParents.get(1));

    // Test 2
    // Add enough categories to fill the first chunk.
    for (int i = 2; i < TaxonomyIndexArrays.CHUNK_SIZE; i++) {
      taxoWriter.addCategory(new FacetLabel("a", Integer.toString(i)));
    }
    taxoWriter.commit();
    try (IndexReader reader = DirectoryReader.open(dir)) {
      oldTia = new TaxonomyIndexArrays(reader);
    }

    oldParents = oldTia.parents();
    assertEquals(TaxonomyIndexArrays.CHUNK_SIZE, oldParents.length());
    assertEquals(TaxonomyReader.INVALID_ORDINAL, oldParents.get(0));
    assertEquals(TaxonomyReader.ROOT_ORDINAL, oldParents.get(1));
    for (int i = 2; i < oldParents.length(); i++) {
      assertEquals(1, oldParents.get(i));
    }

    // Test 3
    // Both TIAs have the same parents and siblings arrays.
    oldTia.children(); // Initializes children
    try (IndexReader reader = DirectoryReader.open(dir)) {
      newTia = new TaxonomyIndexArrays(reader, oldTia);
    }
    checkInvariants(oldTia, newTia);

    // Test 4
    // Add one more category, which will start a new chunk on the new TIA.
    taxoWriter.addCategory(new FacetLabel("a", Integer.toString(TaxonomyIndexArrays.CHUNK_SIZE)));
    taxoWriter.commit();
    try (IndexReader reader = DirectoryReader.open(dir)) {
      newTia = new TaxonomyIndexArrays(reader, oldTia);
    }

    newParents = newTia.parents();
    assertEquals(1 + TaxonomyIndexArrays.CHUNK_SIZE, newParents.length());
    assertEquals(TaxonomyReader.INVALID_ORDINAL, newParents.get(0));
    assertEquals(TaxonomyReader.ROOT_ORDINAL, newParents.get(1));
    for (int i = 2; i < newParents.length(); i++) {
      assertEquals(1, newParents.get(i));
    }

    // Test 5
    // Fill the second chunk of the new TIA.
    for (int i = 1; i < TaxonomyIndexArrays.CHUNK_SIZE; i++) {
      taxoWriter.addCategory(
          new FacetLabel("a", Integer.toString(i + TaxonomyIndexArrays.CHUNK_SIZE)));
    }
    taxoWriter.commit();
    try (IndexReader reader = DirectoryReader.open(dir)) {
      newTia = new TaxonomyIndexArrays(reader, oldTia);
    }

    newParents = newTia.parents();
    assertEquals(2 * TaxonomyIndexArrays.CHUNK_SIZE, newParents.length());
    assertEquals(TaxonomyReader.INVALID_ORDINAL, newParents.get(0));
    assertEquals(TaxonomyReader.ROOT_ORDINAL, newParents.get(1));
    for (int i = 2; i < newParents.length(); i++) {
      assertEquals(1, newParents.get(i));
    }

    taxoWriter.close();
    dir.close();
  }

  public void testRefresh() throws IOException {
    Directory dir = newDirectory();

    // Write two chunks worth of ordinals whose parents are ordinals 1 or 2
    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(dir);
    taxoWriter.addCategory(new FacetLabel("a")); // ordinal 1
    taxoWriter.addCategory(new FacetLabel("b")); // ordinal 2
    for (int i = 0; i < 2 * TaxonomyIndexArrays.CHUNK_SIZE; i++) {
      if (i % 2 == 0) {
        taxoWriter.addCategory(new FacetLabel("a", Integer.toString(i)));
      } else {
        taxoWriter.addCategory(new FacetLabel("b", Integer.toString(i)));
      }
    }
    taxoWriter.commit();

    // Initialize old taxonomy arrays
    IndexReader reader = DirectoryReader.open(dir);
    TaxonomyIndexArrays oldTia = new TaxonomyIndexArrays(reader);
    reader.close();
    oldTia.children(); // Init the children

    // Write one more batch of ordinals whose parents are ordinals 1 or 2 again.
    for (int i = 2 * TaxonomyIndexArrays.CHUNK_SIZE; i < 3 * TaxonomyIndexArrays.CHUNK_SIZE; i++) {
      if (i % 2 == 0) {
        taxoWriter.addCategory(new FacetLabel("a", Integer.toString(i)));
      } else {
        taxoWriter.addCategory(new FacetLabel("b", Integer.toString(i)));
      }
    }
    taxoWriter.close();

    // Initialize new taxonomy arrays
    reader = DirectoryReader.open(dir);
    TaxonomyIndexArrays newTia = new TaxonomyIndexArrays(reader, oldTia);
    reader.close();

    // Parents and siblings are unchanged in the old range, children will have changed
    checkInvariants(oldTia, newTia);

    TaxonomyIndexArrays.ChunkedIntArray oldChildren = oldTia.children();
    TaxonomyIndexArrays.ChunkedIntArray newChildren = newTia.children();

    // The first chunk had to be reallocated to rewrite the value for ordinals 1 and 2
    assertNotSame(oldChildren.values[0], newChildren.values[0]);
    // The second chunk could stay the same, since none of these ordinals have children
    assertSame(oldChildren.values[1], newChildren.values[1]);
    // The third chunk had to be reallocated to grow
    assertNotSame(oldChildren.values[2], newChildren.values[2]);

    // Check contents of the first chunk
    for (int i = 0; i < TaxonomyIndexArrays.CHUNK_SIZE; i++) {
      if (i == 1 || i == 2) {
        assertNotEquals(oldChildren.values[0][i], newChildren.values[0][i]);
      } else {
        assertEquals(oldChildren.values[0][i], newChildren.values[0][i]);
      }
    }
    // Check contents of the third chunk
    for (int i = 0; i < oldChildren.values[2].length; i++) {
      assertEquals(oldChildren.values[2][i], newChildren.values[2][i]);
    }

    dir.close();
  }
}
