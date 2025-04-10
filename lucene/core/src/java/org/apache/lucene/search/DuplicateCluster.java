package org.apache.lucene.search;

import java.util.Collections;
import java.util.Set;

/** Represents a group of near-duplicate documents. */
public final class DuplicateCluster {

  private final Set<Integer> docIDs;

  public DuplicateCluster(Set<Integer> docIDs) {
    this.docIDs = Collections.unmodifiableSet(docIDs);
  }

  /** Returns the set of global document IDs in this cluster. */
  public Set<Integer> docIds() {
    return docIDs;
  }

  @Override
  public String toString() {
    return "DuplicateCluster" + docIDs;
  }
}
