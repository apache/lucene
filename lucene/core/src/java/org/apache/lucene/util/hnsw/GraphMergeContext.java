package org.apache.lucene.util.hnsw;

import org.apache.lucene.util.BitSet;

/**
 * A helper class to hold the context of merging graphs.
 *
 * @param initGraphs graphs that will be participated in initialization. For now, it's all graphs that does not have
 *     any deletion. If there are no such graphs, it will be null.
 * @param oldToNewOrdinalMaps for each graph in {@code initGraphs}, it's the old to new ordinal mapping.
 * @param maxOrd max ordinal of the new (to be created) graph
 * @param initializedNodes all new ordinals that are included in the {@code initGraphs}, they might have already
 *     been initialized, as part of the very first graph, or will be initialized in a later process, e.g. see
 *     {@link UpdateGraphsUtils#joinSetGraphMerge(HnswGraph, HnswGraph, int[], HnswBuilder)} Note: in case of
 *     {@code initGraphs} is non-null but this field is null, it means all ordinals are/will be initialized.
 */
record GraphMergeContext(HnswGraph[] initGraphs, int[][] oldToNewOrdinalMaps, int maxOrd, BitSet initializedNodes) {

  public boolean allInitialized() {
    return initGraphs != null && initializedNodes == null;
  }
}
