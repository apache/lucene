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
package org.apache.lucene.store;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import org.apache.lucene.tests.store.BaseDirectoryTestCase;
import org.apache.lucene.util.Constants;

/** Tests MMapDirectory */
// See: https://issues.apache.org/jira/browse/SOLR-12028 Tests cannot remove files on Windows
// machines occasionally
public class TestMMapDirectory extends BaseDirectoryTestCase {

  @Override
  protected Directory getDirectory(Path path) throws IOException {
    MMapDirectory m = new MMapDirectory(path);
    m.setPreload((file, context) -> random().nextBoolean());
    return m;
  }

  public void testAceWithThreads() throws Exception {
    final int nInts = 8 * 1024 * 1024;

    try (Directory dir = getDirectory(createTempDir("testAceWithThreads"))) {
      try (IndexOutput out = dir.createOutput("test", IOContext.DEFAULT)) {
        final Random random = random();
        for (int i = 0; i < nInts; i++) {
          out.writeInt(random.nextInt());
        }
      }

      final int iters = RANDOM_MULTIPLIER * (TEST_NIGHTLY ? 50 : 10);
      for (int iter = 0; iter < iters; iter++) {
        final IndexInput in = dir.openInput("test", IOContext.DEFAULT);
        final IndexInput clone = in.clone();
        final byte[] accum = new byte[nInts * Integer.BYTES];
        final CountDownLatch shotgun = new CountDownLatch(1);
        final Thread t1 =
            new Thread(
                () -> {
                  try {
                    shotgun.await();
                    for (int i = 0; i < 10; i++) {
                      clone.seek(0);
                      clone.readBytes(accum, 0, accum.length);
                    }
                  } catch (
                      @SuppressWarnings("unused")
                      AlreadyClosedException ok) {
                    // OK
                  } catch (InterruptedException | IOException e) {
                    throw new RuntimeException(e);
                  }
                });
        t1.start();
        shotgun.countDown();
        // this triggers "bad behaviour": closing input while other threads are running
        in.close();
        t1.join();
      }
    }
  }

  public void testNullParamsIndexInput() throws Exception {
    try (Directory mmapDir = getDirectory(createTempDir("testNullParamsIndexInput"))) {
      try (IndexOutput out = mmapDir.createOutput("bytes", newIOContext(random()))) {
        out.alignFilePointer(16);
      }
      try (IndexInput in = mmapDir.openInput("bytes", IOContext.DEFAULT)) {
        assertThrows(NullPointerException.class, () -> in.readBytes(null, 0, 1));
        assertThrows(NullPointerException.class, () -> in.readFloats(null, 0, 1));
        assertThrows(NullPointerException.class, () -> in.readLongs(null, 0, 1));
      }
    }
  }

  public void testMadviseAvail() throws Exception {
    assertEquals(
        "madvise should be supported on Linux and Macos",
        Constants.LINUX || Constants.MAC_OS_X,
        MMapDirectory.supportsMadvise());
  }

  // Opens the input with IOContext.RANDOM to ensure basic code path coverage for POSIX_MADV_RANDOM.
  public void testWithRandom() throws Exception {
    final int size = 8 * 1024;
    byte[] bytes = new byte[size];
    random().nextBytes(bytes);

    try (Directory dir = new MMapDirectory(createTempDir("testWithRandom"))) {
      try (IndexOutput out = dir.createOutput("test", IOContext.DEFAULT)) {
        out.writeBytes(bytes, 0, bytes.length);
      }

      try (final IndexInput in = dir.openInput("test", IOContext.RANDOM)) {
        final byte[] readBytes = new byte[size];
        in.readBytes(readBytes, 0, readBytes.length);
        assertArrayEquals(bytes, readBytes);
      }
    }
  }
}
