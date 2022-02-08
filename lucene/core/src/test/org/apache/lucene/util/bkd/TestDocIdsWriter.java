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
package org.apache.lucene.util.bkd;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestDocIdsWriter extends LuceneTestCase {

  public void testRandom() throws Exception {
    innerTestRandom(false);
  }

  public void testLegacyRandom() throws Exception {
    innerTestRandom(true);
  }

  private void innerTestRandom(boolean legacy) throws Exception {
    int numIters = atLeast(100);
    try (Directory dir = newDirectory()) {
      for (int iter = 0; iter < numIters; ++iter) {
        int[] docIDs = new int[1 + random().nextInt(5000)];
        final int bpv = TestUtil.nextInt(random(), 1, 32);
        for (int i = 0; i < docIDs.length; ++i) {
          docIDs[i] = TestUtil.nextInt(random(), 0, (1 << bpv) - 1);
        }
        test(dir, docIDs, legacy);
      }
    }
  }

  public void testSorted() throws Exception {
    innerTestSorted(false);
  }

  public void testLegacySorted() throws Exception {
    innerTestSorted(true);
  }

  private void innerTestSorted(boolean legacy) throws Exception {
    int numIters = atLeast(100);
    try (Directory dir = newDirectory()) {
      for (int iter = 0; iter < numIters; ++iter) {
        int[] docIDs = new int[1 + random().nextInt(5000)];
        final int bpv = TestUtil.nextInt(random(), 1, 32);
        for (int i = 0; i < docIDs.length; ++i) {
          docIDs[i] = TestUtil.nextInt(random(), 0, (1 << bpv) - 1);
        }
        Arrays.sort(docIDs);
        test(dir, docIDs, legacy);
      }
    }
  }

  public void testCluster() throws Exception {
    int numIters = atLeast(100);
    try (Directory dir = newDirectory()) {
      for (int iter = 0; iter < numIters; ++iter) {
        int[] docIDs = new int[1 + random().nextInt(5000)];
        int min = random().nextInt(1000);
        final int bpv = TestUtil.nextInt(random(), 1, 16);
        for (int i = 0; i < docIDs.length; ++i) {
          docIDs[i] = min + TestUtil.nextInt(random(), 0, (1 << bpv) - 1);
        }
        test(dir, docIDs, false);
      }
    }
  }

  public void testBitSet() throws Exception {
    int numIters = atLeast(100);
    try (Directory dir = newDirectory()) {
      for (int iter = 0; iter < numIters; ++iter) {
        int size = 1 + random().nextInt(5000);
        Set<Integer> set = new HashSet<>(size);
        int small = random().nextInt(1000);
        while (set.size() < size) {
          set.add(small + random().nextInt(size * 16));
        }
        int[] docIDs = set.stream().mapToInt(t -> t).sorted().toArray();
        test(dir, docIDs, false);
      }
    }
  }

  public void testContinuousIds() throws Exception {
    int numIters = atLeast(100);
    try (Directory dir = newDirectory()) {
      for (int iter = 0; iter < numIters; ++iter) {
        int size = 1 + random().nextInt(5000);
        int[] docIDs = new int[size];
        int start = random().nextInt(1000000);
        for (int i = 0; i < docIDs.length; i++) {
          docIDs[i] = start + i;
        }
        test(dir, docIDs, false);
      }
    }
  }

  private void test(Directory dir, int[] ints, boolean legacy) throws Exception {
    final long len;
    DocIdsWriter docIdsWriter = new DocIdsWriter(ints.length);
    try (IndexOutput out = dir.createOutput("tmp", IOContext.DEFAULT)) {
      if (legacy) {
        legacyWriteDocIds(ints, 0, ints.length, out);
      } else {
        docIdsWriter.writeDocIds(ints, 0, ints.length, out);
      }
      len = out.getFilePointer();
      if (random().nextBoolean()) {
        out.writeLong(0); // garbage
      }
    }
    try (IndexInput in = dir.openInput("tmp", IOContext.READONCE)) {
      int[] read = new int[ints.length];
      docIdsWriter.readInts(in, ints.length, read);
      assertArrayEquals(ints, read);
      assertEquals(len, in.getFilePointer());
    }
    try (IndexInput in = dir.openInput("tmp", IOContext.READONCE)) {
      int[] read = new int[ints.length];
      docIdsWriter.readInts(
          in,
          ints.length,
          new IntersectVisitor() {
            int i = 0;

            @Override
            public void visit(int docID) throws IOException {
              read[i++] = docID;
            }

            @Override
            public void visit(int docID, byte[] packedValue) throws IOException {
              throw new UnsupportedOperationException();
            }

            @Override
            public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
              throw new UnsupportedOperationException();
            }
          });
      assertArrayEquals(ints, read);
      assertEquals(len, in.getFilePointer());
    }
    dir.deleteFile("tmp");
  }

  // This is a fork of legacy DocIdsWriter to test backward compatibility.
  private static void legacyWriteDocIds(int[] docIds, int start, int count, DataOutput out)
      throws IOException {
    boolean sorted = true;
    for (int i = 1; i < count; ++i) {
      if (docIds[start + i - 1] > docIds[start + i]) {
        sorted = false;
        break;
      }
    }
    if (sorted) {
      out.writeByte((byte) 0);
      int previous = 0;
      for (int i = 0; i < count; ++i) {
        int doc = docIds[start + i];
        out.writeVInt(doc - previous);
        previous = doc;
      }
    } else {
      long max = 0;
      for (int i = 0; i < count; ++i) {
        max |= Integer.toUnsignedLong(docIds[start + i]);
      }
      if (max <= 0xffffff) {
        out.writeByte((byte) 24);
        // write them the same way we are reading them.
        int i;
        for (i = 0; i < count - 7; i += 8) {
          int doc1 = docIds[start + i];
          int doc2 = docIds[start + i + 1];
          int doc3 = docIds[start + i + 2];
          int doc4 = docIds[start + i + 3];
          int doc5 = docIds[start + i + 4];
          int doc6 = docIds[start + i + 5];
          int doc7 = docIds[start + i + 6];
          int doc8 = docIds[start + i + 7];
          long l1 = (doc1 & 0xffffffL) << 40 | (doc2 & 0xffffffL) << 16 | ((doc3 >>> 8) & 0xffffL);
          long l2 =
              (doc3 & 0xffL) << 56
                  | (doc4 & 0xffffffL) << 32
                  | (doc5 & 0xffffffL) << 8
                  | ((doc6 >> 16) & 0xffL);
          long l3 = (doc6 & 0xffffL) << 48 | (doc7 & 0xffffffL) << 24 | (doc8 & 0xffffffL);
          out.writeLong(l1);
          out.writeLong(l2);
          out.writeLong(l3);
        }
        for (; i < count; ++i) {
          out.writeShort((short) (docIds[start + i] >>> 8));
          out.writeByte((byte) docIds[start + i]);
        }
      } else {
        out.writeByte((byte) 32);
        for (int i = 0; i < count; ++i) {
          out.writeInt(docIds[start + i]);
        }
      }
    }
  }
}
