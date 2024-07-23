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
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.Unwrappable;

@SuppressWarnings("preview")
final class MemorySegmentIndexInputProvider
    implements MMapDirectory.MMapIndexInputProvider<
        ConcurrentHashMap<String, RefCountedSharedArena>> {

  private final Optional<NativeAccess> nativeAccess;

  MemorySegmentIndexInputProvider() {
    this.nativeAccess = NativeAccess.getImplementation();
  }

  @Override
  public IndexInput openInput(
      Path path,
      IOContext context,
      int chunkSizePower,
      boolean preload,
      Optional<String> group,
      ConcurrentHashMap<String, RefCountedSharedArena> arenas)
      throws IOException {
    final String resourceDescription = "MemorySegmentIndexInput(path=\"" + path.toString() + "\")";

    // Work around for JDK-8259028: we need to unwrap our test-only file system layers
    path = Unwrappable.unwrapAll(path);

    boolean success = false;
    final boolean confined = context == IOContext.READONCE;
    final Arena arena = confined ? Arena.ofConfined() : getSharedArena(group, arenas);
    try (var fc = FileChannel.open(path, StandardOpenOption.READ)) {
      final long fileSize = fc.size();
      final IndexInput in =
          MemorySegmentIndexInput.newInstance(
              resourceDescription,
              arena,
              map(
                  arena,
                  resourceDescription,
                  fc,
                  context.readAdvice(),
                  chunkSizePower,
                  preload,
                  fileSize),
              fileSize,
              chunkSizePower,
              confined);
      success = true;
      return in;
    } finally {
      if (success == false) {
        arena.close();
      }
    }
  }

  @Override
  public long getDefaultMaxChunkSize() {
    return Constants.JRE_IS_64BIT ? (1L << 34) : (1L << 28);
  }

  @Override
  public boolean supportsMadvise() {
    return nativeAccess.isPresent();
  }

  private final MemorySegment[] map(
      Arena arena,
      String resourceDescription,
      FileChannel fc,
      ReadAdvice readAdvice,
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
        segment = fc.map(MapMode.READ_ONLY, startOffset, segSize, arena);
      } catch (IOException ioe) {
        throw convertMapFailedIOException(ioe, resourceDescription, segSize);
      }
      // if preload apply it without madvise.
      // skip madvise if the address of our segment is not page-aligned (small segments due to
      // internal FileChannel logic)
      if (preload) {
        segment.load();
      } else if (nativeAccess.filter(na -> segment.address() % na.getPageSize() == 0).isPresent()) {
        nativeAccess.get().madvise(segment, readAdvice);
      }
      segments[segNr] = segment;
      startOffset += segSize;
    }
    return segments;
  }

  @Override
  public ConcurrentHashMap<String, RefCountedSharedArena> attachment() {
    return new ConcurrentHashMap<>();
  }

  /**
   * This sysprop allows to control the max number of permits that a RefCountedSharedArena will
   * support for its lifetime. For example, to set the max number of permits to 256, pass the
   * following on the command line pass {@code
   * -Dorg.apache.lucene.store.MemorySegmentIndexInputProvider.sharedArenaMaxPermits=256}.
   */
  static final String SHARED_ARENA_MAX_PERMITS_SYSPROP =
      "org.apache.lucene.store.MemorySegmentIndexInputProvider.sharedArenaMaxPermits";

  private static int getSharedArenaMaxPermitsSysprop() {
    try {
      int v =
          Optional.ofNullable(System.getProperty(SHARED_ARENA_MAX_PERMITS_SYSPROP))
              .map(Integer::valueOf)
              .orElse(RefCountedSharedArena.DEFAULT_MAX_PERMITS);
      if (RefCountedSharedArena.validMaxPermits(v) == false) {
        v = RefCountedSharedArena.DEFAULT_MAX_PERMITS;
        Logger.getLogger(MemorySegmentIndexInputProvider.class.getName())
            .warning(
                "Invalid value for sysprop "
                    + SHARED_ARENA_MAX_PERMITS_SYSPROP
                    + ", must be positive and <= 0x07FF. The default value will be used.");
      }
      return v;
    } catch (@SuppressWarnings("unused") NumberFormatException | SecurityException ignored) {
      Logger.getLogger(MemorySegmentIndexInputProvider.class.getName())
          .warning(
              "Cannot read sysprop "
                  + SHARED_ARENA_MAX_PERMITS_SYSPROP
                  + ", so the default value will be used.");
      return RefCountedSharedArena.DEFAULT_MAX_PERMITS;
    }
  }

  private static final int sharedArenaMaxPermits = getSharedArenaMaxPermitsSysprop();

  /**
   * Gets an arena for the given group, potentially aggregating files from the same segment into a
   * single ref counted shared arena. A ref counted shared arena, if created will be added to the
   * given arenas map.
   */
  static Arena getSharedArena(
      Optional<String> group, ConcurrentHashMap<String, RefCountedSharedArena> arenas) {
    if (group.isEmpty()) {
      return Arena.ofShared();
    }

    String key = group.get();
    var refCountedArena =
        arenas.computeIfAbsent(
            key, s -> new RefCountedSharedArena(s, () -> arenas.remove(s), sharedArenaMaxPermits));
    if (refCountedArena.acquire()) {
      return refCountedArena;
    } else {
      return arenas.compute(
          key,
          (s, v) -> {
            if (v != null && v.acquire()) {
              return v;
            } else {
              v = new RefCountedSharedArena(s, () -> arenas.remove(s), sharedArenaMaxPermits);
              v.acquire(); // guaranteed to succeed
              return v;
            }
          });
    }
  }
}
