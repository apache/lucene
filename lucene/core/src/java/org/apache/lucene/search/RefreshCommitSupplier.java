package org.apache.lucene.search;

import java.util.List;
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
   * @param commits List of commits from searcher directory.
   * @param currentCommit Current searcher commit.
   */
  default IndexCommit getSearcherRefreshCommit(
      List<IndexCommit> commits, IndexCommit currentCommit) {
    return null;
  }

  /**
   * Determines if taxonomy reader is ready for refresh on the latest commit
   *
   * <p>Expert: Taxonomy readers are always refreshed on the latest commit. Use this function to
   * skip taxonomy refresh if the searcher commit returned by {@link
   * RefreshCommitSupplier#getSearcherRefreshCommit(List, IndexCommit)} is not yet ready for the
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
