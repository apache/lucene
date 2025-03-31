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
package org.apache.lucene.search;

import java.io.IOException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;

/**
 * Expert: Interface to supply commit for searcher refresh.
 *
 * @lucene.experimental
 */
public interface RefreshCommitSupplier {

  /**
   * Expert: Returns the index commit that searcher should refresh on. A null return value (default)
   * indicates reader should refresh on the latest commit.
   *
   * @param reader DirectoryReader to refresh
   */
  default IndexCommit getSearcherRefreshCommit(DirectoryReader reader) throws IOException {
    return null;
  }

  /**
   * Determines if taxonomy reader is ready for refresh on the latest commit
   *
   * <p>Expert: Taxonomy readers are always refreshed on the latest commit. Use this function to
   * skip taxonomy refresh if the searcher commit returned by {@link
   * RefreshCommitSupplier#getSearcherRefreshCommit(DirectoryReader)} is not yet ready for the
   * latest taxonomy changes. Otherwise, you might get a new reader that references ords not yet
   * known to the taxonomy reader
   *
   * <p>For example, a possible implementation could be to return false as long as
   * getSearcherRefreshCommit selects a non-latest directory commit, and return true when it finally
   * selects the latest commit.
   *
   * @return true if taxonomy reader should be refreshed to latest commit, false otherwise.
   */
  default boolean refreshTaxonomy() {
    return true;
  }
}
