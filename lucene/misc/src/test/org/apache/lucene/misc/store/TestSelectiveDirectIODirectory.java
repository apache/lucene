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
package org.apache.lucene.misc.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.ParallelVectorReadable;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.NamedThreadFactory;
import org.junit.BeforeClass;

public class TestSelectiveDirectIODirectory extends LuceneTestCase {

  @BeforeClass
  public static void checkSupported() throws IOException {
    assumeTrue(
        "test requires a JDK with ExtendedOpenOption.DIRECT",
        DirectIODirectory.ExtendedOpenOption_DIRECT != null);
    Path path = createTempDir("selectiveDirectIOProbe");
    try (Directory dir = new SelectiveDirectIODirectory(FSDirectory.open(path));
        IndexOutput out = dir.createOutput("probe.vec", IOContext.DEFAULT)) {
      out.writeString("test");
    } catch (IOException e) {
      assumeNoException("test requires a filesystem that supports Direct IO", e);
    }
  }

  public void testParallelBatchReads() throws Exception {
    doTest(atLeast(200), 1 + random().nextInt(16), true, 4);
  }

  public void testSerialBatchReads() throws Exception {
    doTest(atLeast(50), 8, false, 0);
  }

  private void doTest(int count, int dim, boolean shuffle, int threads) throws Exception {
    ExecutorService es =
        threads > 0
            ? Executors.newFixedThreadPool(threads, new NamedThreadFactory("test-rerank"))
            : null;
    Path path = createTempDir("selectiveDirectIO");
    float[][] expected = new float[count][dim];
    long[] offsets = new long[count];
    try (Directory dir = new SelectiveDirectIODirectory(FSDirectory.open(path), es)) {
      try (IndexOutput out = dir.createOutput("data.vec", IOContext.DEFAULT)) {
        for (int i = 0; i < count; i++) {
          offsets[i] = out.getFilePointer();
          ByteBuffer bb = ByteBuffer.allocate(dim * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
          for (int d = 0; d < dim; d++) {
            float v = random().nextFloat();
            expected[i][d] = v;
            bb.putFloat(v);
          }
          out.writeBytes(bb.array(), 0, bb.capacity());
        }
      }

      if (shuffle) {
        for (int i = count - 1; i > 0; i--) {
          int j = random().nextInt(i + 1);
          long to = offsets[i];
          offsets[i] = offsets[j];
          offsets[j] = to;
          float[] te = expected[i];
          expected[i] = expected[j];
          expected[j] = te;
        }
      }

      try (IndexInput in = dir.openInput("data.vec", IOContext.DEFAULT)) {
        assertTrue(
            "the .vec input must expose the parallel batch read capability",
            in instanceof ParallelVectorReadable);
        float[] actual = new float[count * dim];
        ((ParallelVectorReadable) in).readVectors(offsets, dim, count, actual);
        for (int i = 0; i < count; i++) {
          float[] got = ArrayUtil.copyOfSubArray(actual, i * dim, i * dim + dim);
          assertArrayEquals("vector " + i + " mismatch", expected[i], got, 0f);
        }
      }
    } finally {
      if (es != null) {
        es.shutdown();
      }
    }
  }
}
