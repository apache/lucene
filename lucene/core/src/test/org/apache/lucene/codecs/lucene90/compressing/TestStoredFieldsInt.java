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
package org.apache.lucene.codecs.lucene90.compressing;

import java.util.Arrays;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestStoredFieldsInt extends LuceneTestCase {

  public void testRandom() throws Exception {
    int numIters = atLeast(100);
    try (Directory dir = newDirectory()) {
      for (int iter = 0; iter < numIters; ++iter) {
        int[] values = new int[random().nextInt(5000) + 1];
        final int bpv = TestUtil.nextInt(random(), 1, 31);
        for (int i = 0; i < values.length; ++i) {
          values[i] = TestUtil.nextInt(random(), 0, (1 << bpv) - 1);
        }
        test(dir, values);
      }
    }
  }

  public void testAllEquals() throws Exception {
    try (Directory dir = newDirectory()) {
      int[] docIDs = new int[random().nextInt(5000) + 1];
      final int bpv = TestUtil.nextInt(random(), 1, 31);
      Arrays.fill(docIDs, TestUtil.nextInt(random(), 0, (1 << bpv) - 1));
      test(dir, docIDs);
    }
  }

  private void test(Directory dir, int[] ints) throws Exception {
    final long len;
    try (IndexOutput out = dir.createOutput("tmp", IOContext.DEFAULT)) {
      StoredFieldsInts.writeInts(ints, 0, ints.length, out);
      len = out.getFilePointer();
      if (random().nextBoolean()) {
        out.writeLong(0); // garbage
      }
    }

    try (IndexInput in = dir.openInput("tmp", IOContext.READONCE)) {
      final int offset = random().nextInt(5);
      long[] read = new long[ints.length + offset];
      StoredFieldsInts.readInts(in, ints.length, read, offset);
      int[] readInts = new int[ints.length];
      for (int i = 0; i < ints.length; i++) {
        readInts[i] = (int) read[offset + i];
      }
      assertArrayEquals(ints, readInts);
      assertEquals(len, in.getFilePointer());
    }
    dir.deleteFile("tmp");
  }
}
