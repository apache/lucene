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
import java.lang.foreign.MemorySegment;
import java.lang.foreign.MemorySession;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.logging.Logger;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.Unwrappable;

@SuppressWarnings("preview")
final class MemorySegmentIndexInputProvider implements MMapDirectory.MMapIndexInputProvider {

  public MemorySegmentIndexInputProvider() {
    var log = Logger.getLogger(getClass().getName());
    log.info(
        "Using MemorySegmentIndexInput with Java 19; to disable start with -D"
            + MMapDirectory.ENABLE_MEMORY_SEGMENTS_SYSPROP
            + "=false");
  }

  @Override
  public IndexInput openInput(Path path, IOContext context, int chunkSizePower, boolean preload)
      throws IOException {
    final String resourceDescription = "MemorySegmentIndexInput(path=\"" + path.toString() + "\")";

    // Work around for JDK-8259028: we need to unwrap our test-only file system layers
    path = Unwrappable.unwrapAll(path);

    boolean success = false;
    final MemorySession session = MemorySession.openShared();
    try (var fc = FileChannel.open(path, StandardOpenOption.READ)) {
      final long fileSize = fc.size();
      final IndexInput in =
          MemorySegmentIndexInput.newInstance(
              resourceDescription,
              session,
              map(session, resourceDescription, fc, chunkSizePower, preload, fileSize),
              fileSize,
              chunkSizePower);
      success = true;
      return in;
    } finally {
      if (success == false) {
        session.close();
      }
    }
  }

  @Override
  public long getDefaultMaxChunkSize() {
    return Constants.JRE_IS_64BIT ? (1L << 34) : (1L << 28);
  }

  @Override
  public boolean isUnmapSupported() {
    return true;
  }

  @Override
  public String getUnmapNotSupportedReason() {
    return null;
  }

  private final MemorySegment[] map(
      MemorySession session,
      String resourceDescription,
      FileChannel fc,
      int chunkSizePower,
      boolean preload,
      long length)
      throws IOException {
    if ((length >>> chunkSizePower) >= Integer.MAX_VALUE)
      throw new IllegalArgumentException("File too big for chunk size: " + resourceDescription);

    final long chunkSize = 1L << chunkSizePower;

    // we always allocate one more segments, the last one may be a 0 byte one
    final int nrSegments = (int) (length >>> chunkSizePower) + 1;

    final MemorySegment[] segments = new MemorySegment[nrSegments];

    long startOffset = 0L;
    for (int segNr = 0; segNr < nrSegments; segNr++) {
      final long segSize =
          (length > (startOffset + chunkSize)) ? chunkSize : (length - startOffset);
      final MemorySegment segment;
      try {
        segment = fc.map(MapMode.READ_ONLY, startOffset, segSize, session);
      } catch (IOException ioe) {
        throw convertMapFailedIOException(ioe, resourceDescription, segSize);
      }
      if (preload) {
        segment.load();
      }
      segments[segNr] = segment;
      startOffset += segSize;
    }
    return segments;
  }
}
