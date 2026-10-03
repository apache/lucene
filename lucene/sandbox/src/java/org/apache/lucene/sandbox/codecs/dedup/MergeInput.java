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
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

/**
 * A data file as merges read it. Read advice applies to a whole mapping, so when searches read the
 * file at random a merge maps it again without that advice, instead of reading through the mapping
 * searches use. The merge mapping does not claim another access pattern: a merge in this format
 * comes back to vectors it has already read, to compare the ones whose hashes collide and to
 * quantize the distinct ones once all are known.
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
   * Whether a merge needs a mapping of its own: only when searches read the file at random. A file
   * a merge opened is already read the way a merge reads it.
   */
  boolean needed() {
    return context.context() != IOContext.Context.MERGE
        && context.hints().contains(DataAccessHint.RANDOM);
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
    if (--merges > 0) {
      return;
    }
    if (mergeInput != null && mergeInput != searchInput) {
      mergeInput.close();
    }
    mergeInput = null;
  }

  /** The context searches opened the file with, as a merge, without their access pattern. */
  private IOContext mergeContext() {
    return IOContext.merge()
        .withHints(
            context.hints().stream()
                .filter(hint -> hint instanceof DataAccessHint == false)
                .toArray(IOContext.FileOpenHint[]::new));
  }

  @Override
  public void close() throws IOException {
    IndexInput toClose = mappingToClose();
    if (toClose != null) {
      toClose.close();
    }
  }

  /** The mapping a merge opened, read under the lock that guards it, closed outside it. */
  private synchronized IndexInput mappingToClose() {
    return mergeInput != searchInput ? mergeInput : null;
  }
}
