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
package org.apache.lucene.tests.store;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.ReadAdvice;

public class TestSerializedIOCountingDirectory extends BaseDirectoryTestCase {

  @Override
  protected Directory getDirectory(Path path) throws IOException {
    return new SerialIOCountingDirectory(FSDirectory.open(path));
  }

  public void testSequentialReads() throws IOException {
    try (SerialIOCountingDirectory dir = new SerialIOCountingDirectory(newDirectory())) {
      try (IndexOutput out = dir.createOutput("test", IOContext.DEFAULT)) {
        for (int i = 0; i < 10; ++i) {
          out.writeBytes(new byte[4096], 4096);
        }
      }
      try (IndexInput in =
          dir.openInput("test", IOContext.DEFAULT.withReadAdvice(ReadAdvice.NORMAL))) {
        in.readByte();
        long count = dir.count();
        while (in.getFilePointer() < in.length()) {
          in.readByte();
        }
        // Sequential reads are free with the normal advice
        assertThat(dir.count(), equalTo(count));
      }
      try (IndexInput in =
          dir.openInput("test", IOContext.DEFAULT.withReadAdvice(ReadAdvice.RANDOM))) {
        in.readByte();
        long count = dir.count();
        while (in.getFilePointer() < in.length()) {
          in.readByte();
        }
        // But not with the random advice
        assertThat(dir.count(), not(equalTo(count)));
      }
    }
  }

  public void testParallelReads() throws IOException {
    try (SerialIOCountingDirectory dir = new SerialIOCountingDirectory(newDirectory())) {
      try (IndexOutput out = dir.createOutput("test", IOContext.DEFAULT)) {
        for (int i = 0; i < 10; ++i) {
          out.writeBytes(new byte[4096], 4096);
        }
      }
      try (IndexInput in =
          dir.openInput("test", IOContext.DEFAULT.withReadAdvice(ReadAdvice.RANDOM))) {
        long count = dir.count();

        // count is incremented on the first prefetch
        in.prefetch(5_000, 1);
        assertEquals(count + 1, dir.count());
        count = dir.count();

        // but not on the second one since it can be performed in parallel
        in.prefetch(10_000, 1);
        assertEquals(count, dir.count());

        // and reading from a prefetched page doesn't increment the counter
        in.seek(5_000);
        in.readByte();
        assertEquals(count, dir.count());

        in.seek(10_000);
        in.readByte();
        assertEquals(count, dir.count());

        // reading data on a page that was not prefetched increments the counter
        in.seek(15_000);
        in.readByte();
        assertEquals(count + 1, dir.count());
      }
    }
  }
}
