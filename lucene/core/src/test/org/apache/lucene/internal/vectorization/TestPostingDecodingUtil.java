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

import org.apache.lucene.codecs.lucene104.ForUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestPostingDecodingUtil extends LuceneTestCase {

  public void testDuelSplitInts() throws Exception {
    final int iterations = atLeast(100);

    try (Directory dir = new MMapDirectory(createTempDir())) {
      try (IndexOutput out = dir.createOutput("tests.bin", IOContext.DEFAULT)) {
        out.writeInt(random().nextInt());
        for (int i = 0; i < ForUtil.BLOCK_SIZE; ++i) {
          out.writeLong(random().nextInt());
        }
      }
      VectorizationProvider vectorizationProvider = VectorizationProvider.lookup(true);
      try (IndexInput in = dir.openInput("tests.bin", IOContext.DEFAULT)) {
        int[] expectedB = new int[ForUtil.BLOCK_SIZE];
        int[] expectedC = new int[ForUtil.BLOCK_SIZE];
        int[] actualB = new int[ForUtil.BLOCK_SIZE];
        int[] actualC = new int[ForUtil.BLOCK_SIZE];
        for (int iter = 0; iter < iterations; ++iter) {
          // Initialize arrays with random content.
          for (int i = 0; i < expectedB.length; ++i) {
            expectedB[i] = random().nextInt();
            actualB[i] = expectedB[i];
            expectedC[i] = random().nextInt();
            actualC[i] = expectedC[i];
          }
          int bShift = TestUtil.nextInt(random(), 1, 31);
          int dec = TestUtil.nextInt(random(), 1, bShift);
          int numIters = (bShift + dec - 1) / dec;
          int count = TestUtil.nextInt(random(), 1, 64 / numIters);
          int bMask = random().nextInt();
          int cIndex = random().nextInt(64);
          int cMask = random().nextInt();
          long startFP = random().nextInt(4);

          // Work on a slice that has just the right number of bytes to make the test fail with an
          // index-out-of-bounds in case the implementation reads more than the allowed number of
          // padding bytes.
          IndexInput slice = in.slice("test", 0, startFP + count * Long.BYTES);

          PostingDecodingUtil defaultUtil = new PostingDecodingUtil(slice);
          PostingDecodingUtil optimizedUtil = vectorizationProvider.newPostingDecodingUtil(slice);

          slice.seek(startFP);
          defaultUtil.splitInts(count, expectedB, bShift, dec, bMask, expectedC, cIndex, cMask);
          long expectedEndFP = slice.getFilePointer();
          slice.seek(startFP);
          optimizedUtil.splitInts(count, actualB, bShift, dec, bMask, actualC, cIndex, cMask);
          assertEquals(expectedEndFP, slice.getFilePointer());
          assertArrayEquals(expectedB, actualB);
          assertArrayEquals(expectedC, actualC);
        }
      }
    }
  }
}
