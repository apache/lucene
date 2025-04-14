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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;

/**
 * Utility class to safely share {@link MultiReader} instances across multiple threads, while
 * periodically reopening. This class ensures each multi-reader is closed only once all threads have
 * finished using each of its sub-readers.
 *
 * <p>Sub-readers for the reference managed multi-reader must be instances of {@link
 * DirectoryReader}.
 *
 * @see SearcherManager
 * @lucene.experimental
 */
public final class MultiReaderManager extends ReferenceManager<MultiReader> {

  /**
   * Creates and returns a new MultiReaderManager from the given {@link Directory} list.
   *
   * @param dir directories to open DirectoryReader(s) on.
   * @throws IOException If there is a low-level I/O error
   */
  public MultiReaderManager(Directory... dir) throws IOException {
    IndexReader[] subReaders = new IndexReader[dir.length];
    for (int i = 0; i < dir.length; i++) {
      subReaders[i] = DirectoryReader.open(dir[i]);
    }
    current = new MultiReader(subReaders, null, false);
  }

  /**
   * Creates and returns a new MultiReaderManager from the given already-opened {@link
   * DirectoryReader}s, stealing the incoming reference.
   *
   * @param subReaders the directoryReader(s) to use for future reopens
   * @throws IOException If there is a low-level I/O error
   */
  public MultiReaderManager(DirectoryReader... subReaders) throws IOException {
    current = new MultiReader(subReaders, null, false);
  }

  public MultiReaderManager(DirectoryReader[] subReaders, Comparator<IndexReader> subReadersSorter)
      throws IOException {
    current = new MultiReader(subReaders, subReadersSorter, false);
  }

  /** Decrements reference count for each sub-reader in the provided reference. */
  @Override
  protected void decRef(MultiReader reference) throws IOException {
    for (IndexReader reader : reference.getSequentialSubReaders()) {
      reader.decRef();
    }
  }

  /**
   * Refreshes sub-readers if needed. Returns a new MultiReader that references the refreshed
   * sub-readers. If none of the sub-readers have changes, returns null.
   */
  @Override
  protected MultiReader refreshIfNeeded(MultiReader referenceToRefresh) throws IOException {
    IndexReader[] newSubReaders =
        new IndexReader[referenceToRefresh.getSequentialSubReaders().size()];
    boolean refreshed = false;
    for (int i = 0; i < referenceToRefresh.getSequentialSubReaders().size(); i++) {
      DirectoryReader old = (DirectoryReader) referenceToRefresh.getSequentialSubReaders().get(i);
      newSubReaders[i] = DirectoryReader.openIfChanged(old);
      if (newSubReaders[i] == null) {
        newSubReaders[i] = old;
        newSubReaders[i].incRef();
      } else {
        refreshed = true;
      }
    }
    if (refreshed == false) {
      return null;
    }
    return new MultiReader(newSubReaders);
  }

  /**
   * Tries to increment references on each of the sub-readers. Returns true if *all* sub-reader
   * references can be incremented, false otherwise.
   */
  @Override
  protected boolean tryIncRef(MultiReader reference) throws IOException {
    List<IndexReader> refIncReaders = new ArrayList<>();
    boolean success = false;
    try {
      for (IndexReader reader : reference.getSequentialSubReaders()) {
        if (reader.tryIncRef()) {
          refIncReaders.add(reader);
        } else {
          break;
        }
      }
      if (refIncReaders.size() == reference.getSequentialSubReaders().size()) {
        success = true;
      }
    } finally {
      if (success == false) {
        for (IndexReader reader : refIncReaders) {
          reader.decRef();
        }
      }
    }
    return success;
  }

  /**
   * Returns the lowest reference count across all sub-readers.
   *
   * <p>The multi-reader is closed if reference count for any of its sub-readers drops to 0. The
   * minimum refCount returned here is hence, the number of references pending release before the
   * MultiReader is closed.
   */
  @Override
  protected int getRefCount(MultiReader reference) {
    int minRefCount = 0;
    for (IndexReader reader : reference.getSequentialSubReaders()) {
      if (minRefCount > reader.getRefCount()) {
        minRefCount = reader.getRefCount();
      }
    }
    return minRefCount;
  }
}
