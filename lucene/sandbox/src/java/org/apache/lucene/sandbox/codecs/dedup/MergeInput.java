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
package org.apache.lucene.sandbox.codecs.dedup;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.stream.Stream;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.NoReuseHint;

/**
 * The raw vectors as merges read them: at random, since a duplicate reads its group's vector back,
 * and not reused. Read advice applies to a whole mapping, so when searches read the file
 * differently a merge maps it again instead of reading through the mapping searches use.
 *
 * <p>The mapping is opened when a merge first asks for it, shared by the merges that follow, and
 * closed when the last one is done, or with the reader.
 */
final class MergeInput implements Closeable {
  private final Directory directory;
  private final String fileName;
  private final IOContext context;
  private final IndexInput searchInput;
  private IndexInput mergeInput;
  // merges holding the mapping and not yet finished
  private int merges;

  /**
   * @param context the context {@code searchInput} was opened with
   */
  MergeInput(Directory directory, String fileName, IOContext context, IndexInput searchInput) {
    this.directory = directory;
    this.fileName = fileName;
    this.context = context;
    this.searchInput = searchInput;
  }

  /**
   * Whether a merge needs a mapping of its own: only when searches read the file differently. A
   * file a merge opened is already read the way a merge reads it.
   */
  boolean needed() {
    return context.context() != IOContext.Context.MERGE
        && context.hints().equals(mergeContext().hints()) == false;
  }

  /** The mapping for one more merge, to give back with {@link #release()}. */
  synchronized IndexInput acquire() throws IOException {
    assert needed();
    if (mergeInput == null) {
      try {
        mergeInput = directory.openInput(fileName, mergeContext());
      } catch (FileNotFoundException | NoSuchFileException _) {
        // an open reader outlives its files, so fall back to the mapping it already holds
        mergeInput = searchInput;
      }
    }
    merges++;
    return mergeInput;
  }

  /** Gives back the mapping of a merge that is done, closing it after the last one. */
  synchronized void release() throws IOException {
    assert merges > 0;
    if (--merges > 0) {
      return;
    }
    if (mergeInput != null && mergeInput != searchInput) {
      mergeInput.close();
    }
    mergeInput = null;
  }

  /** A merge context with what the caller said about the file, read at random and not reused. */
  private IOContext mergeContext() {
    Stream<IOContext.FileOpenHint> merge = Stream.of(DataAccessHint.RANDOM, NoReuseHint.INSTANCE);
    return IOContext.merge()
        .withHints(
            Stream.concat(
                    context.hints().stream()
                        .filter(
                            hint ->
                                hint instanceof DataAccessHint == false
                                    && hint != NoReuseHint.INSTANCE),
                    merge)
                .toArray(IOContext.FileOpenHint[]::new));
  }

  @Override
  public void close() throws IOException {
    IndexInput toClose = takeMapping();
    if (toClose != null) {
      toClose.close();
    }
  }

  /**
   * The mapping a merge opened, taken under the lock that guards it so that a merge finishing later
   * does not close it again, and closed outside it.
   */
  private synchronized IndexInput takeMapping() {
    if (mergeInput == searchInput) {
      return null;
    }
    IndexInput toClose = mergeInput;
    mergeInput = null;
    return toClose;
  }
}
