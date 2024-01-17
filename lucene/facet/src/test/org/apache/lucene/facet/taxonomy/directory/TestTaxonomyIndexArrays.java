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

import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestTaxonomyIndexArrays extends LuceneTestCase {

  private void checkInvariants(TaxonomyIndexArrays oldArray, TaxonomyIndexArrays newArray) {
    TaxonomyIndexArrays.ChunkedIntArray oldParents = oldArray.parents();
    TaxonomyIndexArrays.ChunkedIntArray newParents = newArray.parents();
    for (int i = 0; i < oldParents.values.length - 1; i++) {
      assertSame(oldParents.values[i], newParents.values[i]);
    }
    int lastOldChunk = oldParents.values.length - 1;
    for (int i = 0; i < oldParents.values[lastOldChunk].length; i++) {
      assertEquals(oldParents.values[lastOldChunk][i], newParents.values[lastOldChunk][i]);
    }
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
}
