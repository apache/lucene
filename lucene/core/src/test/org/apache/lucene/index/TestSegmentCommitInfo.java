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
package org.apache.lucene.index;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;

public class TestSegmentCommitInfo extends LuceneTestCase {

  public void testSizeInBytesIsCached() throws IOException {
    try (Directory dir = newDirectory()) {
      HookDirectory hookDir = new HookDirectory(dir);
      SegmentCommitInfo sci = newSegmentCommitInfo(hookDir);

      assertEquals(30, sci.sizeInBytes());
      assertEquals(2, hookDir.fileLengthCalls.get());
      assertEquals(30, sci.sizeInBytes());
      assertEquals(2, hookDir.fileLengthCalls.get());
    }
  }

  public void testSizeInBytesIsRecomputedAfterGenerationAdvances() throws IOException {
    try (Directory dir = newDirectory()) {
      HookDirectory hookDir = new HookDirectory(dir);
      SegmentCommitInfo sci = newSegmentCommitInfo(hookDir);

      assertEquals(30, sci.sizeInBytes());
      sci.advanceFieldInfosGen();
      assertEquals(30, sci.sizeInBytes());
      assertEquals(4, hookDir.fileLengthCalls.get());
    }
  }

  public void testGenerationAdvancedDuringSizeInBytesIsNotCached() throws IOException {
    try (Directory dir = newDirectory()) {
      HookDirectory hookDir = new HookDirectory(dir);
      SegmentCommitInfo sci = newSegmentCommitInfo(hookDir);
      hookDir.onFileLength = sci::advanceFieldInfosGen;

      // the computation that raced with the invalidation still returns a size, not the sentinel
      assertEquals(30, sci.sizeInBytes());
      assertEquals(2, hookDir.fileLengthCalls.get());

      // but its result was not cached, so the next call computes the size again
      assertEquals(30, sci.sizeInBytes());
      assertEquals(4, hookDir.fileLengthCalls.get());
      assertEquals(30, sci.sizeInBytes());
      assertEquals(4, hookDir.fileLengthCalls.get());
    }
  }

  // Simulates a caller outside the IndexWriter lock, such as a MergeScheduler, computing the size
  // while ReadersAndUpdates#writeFieldUpdates changes the files after it has already advanced the
  // generation
  public void testFilesChangedDuringSizeInBytesIsNotCached() throws IOException {
    try (Directory dir = newDirectory()) {
      writeFile(dir, "_0.c", 30);
      HookDirectory hookDir = new HookDirectory(dir);
      SegmentCommitInfo sci = newSegmentCommitInfo(hookDir);
      hookDir.onFileLength = () -> sci.setFieldInfosFiles(Set.of("_0.c"));

      // computed from the files before the change
      assertEquals(30, sci.sizeInBytes());

      // the stale size was not cached, so the new file is counted
      assertEquals(60, sci.sizeInBytes());
    }
  }

  // The setters invalidate on their own rather than relying on a preceding generation advance,
  // which is what closes the window exercised by testFilesChangedDuringSizeInBytesIsNotCached
  public void testSizeInBytesIsRecomputedAfterFilesChange() throws IOException {
    try (Directory dir = newDirectory()) {
      writeFile(dir, "_0.c", 30);
      writeFile(dir, "_0_1.d", 40);
      HookDirectory hookDir = new HookDirectory(dir);
      SegmentCommitInfo sci = newSegmentCommitInfo(hookDir);

      assertEquals(30, sci.sizeInBytes());
      sci.setFieldInfosFiles(Set.of("_0.c"));
      assertEquals(60, sci.sizeInBytes());
      sci.setDocValuesUpdatesFiles(Collections.singletonMap(0, Set.of("_0_1.d")));
      assertEquals(100, sci.sizeInBytes());
    }
  }

  /** Creates a segment with two files totalling 30 bytes. */
  private static SegmentCommitInfo newSegmentCommitInfo(HookDirectory dir) throws IOException {
    writeFile(dir, "_0.a", 10);
    writeFile(dir, "_0.b", 20);
    SegmentInfo info =
        new SegmentInfo(
            dir,
            Version.LATEST,
            Version.LATEST,
            "_0",
            1,
            false,
            false,
            Codec.getDefault(),
            Collections.emptyMap(),
            StringHelper.randomId(),
            Collections.emptyMap(),
            null);
    info.setFiles(Set.of("_0.a", "_0.b"));
    return new SegmentCommitInfo(info, 0, 0, -1, -1, -1, StringHelper.randomId());
  }

  private static void writeFile(Directory dir, String name, int length) throws IOException {
    try (IndexOutput out = dir.createOutput(name, IOContext.DEFAULT)) {
      out.writeBytes(new byte[length], length);
    }
  }

  /** Counts {@link #fileLength} calls and runs a hook, once, during the first one. */
  private static class HookDirectory extends FilterDirectory {
    final AtomicInteger fileLengthCalls = new AtomicInteger();
    Runnable onFileLength;

    HookDirectory(Directory in) {
      super(in);
    }

    @Override
    public long fileLength(String name) throws IOException {
      fileLengthCalls.incrementAndGet();
      Runnable hook = onFileLength;
      if (hook != null) {
        onFileLength = null;
        hook.run();
      }
      return super.fileLength(name);
    }
  }
}
