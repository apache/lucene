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
package org.apache.lucene.search.suggest.fst;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.generators.RandomBytes;
import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import java.io.IOException;
import java.util.Comparator;
import java.util.Random;
import org.apache.lucene.search.suggest.InMemorySorter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.OfflineSorter;
import org.junit.Test;

public class TestBytesRefSorters extends LuceneTestCase {
  @Test
  public void testExternalRefSorter() throws Exception {
    Directory tempDir = newDirectory();
    ExternalRefSorter s = new ExternalRefSorter(new OfflineSorter(tempDir, "temp"));
    check(s);
    IOUtils.close(s, tempDir);
  }

  @Test
  public void testExternalRefSortersIteratorIsCloseable() throws Exception {
    try (Directory tempDir = newDirectory();
        ExternalRefSorter s = new ExternalRefSorter(new OfflineSorter(tempDir, "temp"))) {
      appendRandomSequences(s);

      // Sometimes iterate over a subset of the entries in the sequence iterator, then
      // close it before exhausting the iterator.
      try (ExternalRefSorter.ByteSequenceIterator it = s.iterator()) {
        for (int i = 0; i < 5 && it.next() != null; i++) {
          // Empty.
        }
      }
    }
  }

  @Test
  public void testInMemorySorter() throws Exception {
    check(new InMemorySorter(Comparator.naturalOrder()));
  }

  private void check(BytesRefSorter sorter) throws Exception {
    appendRandomSequences(sorter);

    // Create two iterators and check that they're aligned with each other.
    BytesRefIterator i1 = sorter.iterator();
    BytesRefIterator i2 = sorter.iterator();

    // Verify sorter contract.
    expectThrows(
        IllegalStateException.class,
        () -> {
          sorter.add(new BytesRef(new byte[1]));
        });

    while (true) {
      BytesRef spare1 = i1.next();
      BytesRef spare2 = i2.next();
      assertEquals(spare1, spare2);
      if (spare1 == null) {
        break;
      }
    }
  }

  private void appendRandomSequences(BytesRefSorter sorter) throws IOException {
    Random rnd = new Random(RandomizedContext.current().getRandom().nextLong());
    for (int i = 0; i < RandomNumbers.randomIntBetween(rnd, 10, 100); i++) {
      sorter.add(new BytesRef(RandomBytes.randomBytesOfLengthBetween(rnd, 1, 256)));
    }
  }
}
