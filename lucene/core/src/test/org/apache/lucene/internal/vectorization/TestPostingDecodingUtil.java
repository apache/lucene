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
package org.apache.lucene.internal.vectorization;

import org.apache.lucene.codecs.lucene912.ForUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.ArrayUtil;

public class TestPostingDecodingUtil extends LuceneTestCase {

  public void testDuel() throws Exception {
    final int iterations = atLeast(100);

    try (Directory dir = new MMapDirectory(createTempDir())) {
      try (IndexOutput out = dir.createOutput("tests.bin", IOContext.DEFAULT)) {
        out.writeInt(random().nextInt());
        for (int i = 0; i < ForUtil.BLOCK_SIZE; ++i) {
          out.writeLong(random().nextInt());
        }
      }
      try (IndexInput in = dir.openInput("tests.bin", IOContext.DEFAULT)) {
        long[] expectedB = new long[ForUtil.BLOCK_SIZE];
        long[] expectedC = new long[ForUtil.BLOCK_SIZE];
        long[] actualB = new long[ForUtil.BLOCK_SIZE];
        long[] actualC = new long[ForUtil.BLOCK_SIZE];
        for (int iter = 0; iter < iterations; ++iter) {
          // Initialize arrays with random content.
          for (int i = 0; i < expectedB.length; ++i) {
            expectedB[i] = random().nextLong();
            actualB[i] = expectedB[i];
            expectedC[i] = random().nextLong();
            actualC[i] = expectedC[i];
          }
          int count = TestUtil.nextInt(random(), 1, 64);
          int bShift = TestUtil.nextInt(random(), 1, 31);
          long bMask = random().nextLong();
          long cMask = random().nextLong();
          long startFP = random().nextInt(4);

          // Work on a slice that has just the right number of padding bytes to make the test fail
          // with an index-out-of-bounds in case the implementation reads more than the allowed
          // number of padding bytes.
          IndexInput slice =
              in.slice(
                  "test", 0, startFP + (count + PostingDecodingUtil.PADDING_LONGS) * Long.BYTES);

          PostingDecodingUtil defaultUtil = new DefaultPostingDecodingUtil(slice);
          PostingDecodingUtil optimizedUtil =
              VectorizationProvider.lookup(true).getPostingDecodingUtil(slice);
          assertNotSame(defaultUtil.getClass(), optimizedUtil.getClass());

          slice.seek(startFP);
          defaultUtil.splitLongs(count, expectedB, bShift, bMask, expectedC, cMask);
          long expectedEndFP = slice.getFilePointer();
          slice.seek(startFP);
          optimizedUtil.splitLongs(count, actualB, bShift, bMask, actualC, cMask);
          assertEquals(expectedEndFP, slice.getFilePointer());
          assertArrayEquals(
              ArrayUtil.copyOfSubArray(expectedB, 0, count),
              ArrayUtil.copyOfSubArray(actualB, 0, count));
          assertArrayEquals(
              ArrayUtil.copyOfSubArray(expectedC, 0, count),
              ArrayUtil.copyOfSubArray(actualC, 0, count));
          assertArrayEquals(
              ArrayUtil.copyOfSubArray(
                  expectedB, count + PostingDecodingUtil.PADDING_LONGS, expectedB.length),
              ArrayUtil.copyOfSubArray(
                  actualB, count + PostingDecodingUtil.PADDING_LONGS, actualB.length));
          assertArrayEquals(
              ArrayUtil.copyOfSubArray(
                  expectedC, count + PostingDecodingUtil.PADDING_LONGS, expectedC.length),
              ArrayUtil.copyOfSubArray(
                  actualC, count + PostingDecodingUtil.PADDING_LONGS, actualC.length));
        }
      }
    }
  }
}
