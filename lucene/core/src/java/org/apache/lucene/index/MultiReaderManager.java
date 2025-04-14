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

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class to safely share {@link MultiReader} instances across multiple threads, while
 * periodically reopening. This class ensures each multi-reader is closed only once all threads have
 * finished using it. Sub-readers for the reference managed multi-reader are instances of {@link DirectoryReader}
 *
 * @see SearcherManager
 * @lucene.experimental
 */
public final class MultiReaderManager extends ReferenceManager<MultiReader> {

  /**
   * Creates and returns a new ReaderManager from the given {@link Directory}.
   *
   * @param dir directories to open the DirectoryReader(s) on.
   * @throws IOException If there is a low-level I/O error
   */
  public MultiReaderManager(Directory... dir) throws IOException {
    IndexReader[] subReaders = new IndexReader[dir.length];
    for (int i = 0; i < dir.length; i++) {
      subReaders[i] = DirectoryReader.open(dir[i]);
    }
    current = new MultiReader(subReaders);
  }

  /**
   * Creates and returns a new ReaderManager from the given already-opened {@link DirectoryReader},
   * stealing the incoming reference.
   *
   * @param subReaders the directoryReader(s) to use for future reopens
   * @throws IOException If there is a low-level I/O error
   */
  public MultiReaderManager(DirectoryReader... subReaders) throws IOException {
    current = new MultiReader(subReaders);
  }
//
//  @Override
//  protected void decRef(DirectoryReader reference) throws IOException {
//    reference.decRef();
//  }
//
//  @Override
//  protected DirectoryReader refreshIfNeeded(DirectoryReader referenceToRefresh) throws IOException {
//    return DirectoryReader.openIfChanged(referenceToRefresh);
//  }
//
//  @Override
//  protected boolean tryIncRef(DirectoryReader reference) {
//    return reference.tryIncRef();
//  }
//
//  @Override
//  protected int getRefCount(DirectoryReader reference) {
//    return reference.getRefCount();
//  }

  @Override
  protected void decRef(MultiReader reference) throws IOException {
    for (IndexReader reader: current.getSequentialSubReaders()) {
      reader.decRef();
    }
  }

  @Override
  protected MultiReader refreshIfNeeded(MultiReader referenceToRefresh) throws IOException {
    return null;
  }

  /**
   * Tries to increment references on each of the sub-readers.
   * Returns true if *all* sub-reader references can be incremented, false otherwise.
   */
  @Override
  protected boolean tryIncRef(MultiReader reference) throws IOException {
    List<IndexReader> refIncReaders = new ArrayList<>();
    boolean success = false;
    try {
      for (IndexReader reader : current.getSequentialSubReaders()) {
        if (reader.tryIncRef()) {
          refIncReaders.add(reader);
        } else {
          break;
        }
      }
      if (refIncReaders.size() == current.getSequentialSubReaders().size()) {
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
   * The multi-reader is closed if reference count for any of its sub-readers drops
   * to 0. The minimum refCount returned here is hence, the number of references
   * pending release before the MultiReader is closed.
   */
  @Override
  protected int getRefCount(MultiReader reference) {
    int minRefCount = Integer.MAX_VALUE;
    for (IndexReader reader : current.getSequentialSubReaders()) {
      if (minRefCount > reader.getRefCount()) {
        minRefCount = reader.getRefCount();
      }
    }
    return minRefCount;
  }
}
