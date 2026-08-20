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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;

/**
 * Utility for retrieving {@link DoubleValuesSource} values for a set of global doc IDs (the "return
 * fields" of a set of hits), concurrently across leaves.
 *
 * <p>Given an {@link IndexReader}, an {@code int[]} of global doc IDs (typically the doc IDs of a
 * page of hits, in ranking order), and one or more {@link DoubleValuesSource}s, this returns a
 * {@code double[hit][source]} grid. The result preserves input order: row {@code i} holds the
 * values for {@code globalDocIds[i]}, and column {@code s} holds the value produced by {@code
 * sources[s]}.
 *
 * <p>Internally the doc IDs are scattered to their leaves and sorted ascending within each leaf
 * (via {@link ReaderUtil#partitionByLeaf}), each leaf is processed as an independent task on the
 * supplied {@link Executor}, and the per-leaf results are gathered back into input order using the
 * ordinals tracked by {@code partitionByLeaf}. The input array is not mutated.
 *
 * <p>Documents that have no value for a given source (i.e. {@link DoubleValues#advanceExact} returns
 * {@code false}) are reported as {@link Double#NaN}.
 *
 * <p>This concrete retriever does not supply document scores, so every source must return {@code
 * false} from {@link DoubleValuesSource#needsScores()}; otherwise an {@link
 * IllegalArgumentException} is thrown.
 *
 * @lucene.experimental
 */
public final class DoubleValuesRetriever {

  private DoubleValuesRetriever() {}

  /**
   * Retrieves the values of each source for each global doc ID.
   *
   * @param reader an open index reader (the caller is responsible for its reference count)
   * @param globalDocIds global doc IDs in any order (e.g. ranking order); not mutated
   * @param sources the value sources to evaluate for each doc; none may need scores
   * @param executor executor used to process leaves concurrently
   * @return a {@code double[globalDocIds.length][sources.length]} grid, in input order; missing
   *     values are {@link Double#NaN}
   */
  public static double[][] retrieve(
      IndexReader reader, int[] globalDocIds, DoubleValuesSource[] sources, Executor executor)
      throws IOException {
    Objects.requireNonNull(reader, "reader");
    Objects.requireNonNull(globalDocIds, "globalDocIds");
    Objects.requireNonNull(sources, "sources");
    Objects.requireNonNull(executor, "executor");

    // Rewrite the sources once against the top-level reader, as required by DoubleValuesSource.
    final IndexSearcher searcher = new IndexSearcher(reader);
    final DoubleValuesSource[] rewritten = new DoubleValuesSource[sources.length];
    for (int s = 0; s < sources.length; s++) {
      rewritten[s] = sources[s].rewrite(searcher);
      if (rewritten[s].needsScores()) {
        throw new IllegalArgumentException(
            "DoubleValuesRetriever does not supply scores, but sources["
                + s
                + "] requires them: "
                + sources[s]);
      }
    }

    final double[][] values = new double[globalDocIds.length][];
    if (globalDocIds.length == 0) {
      return values;
    }

    final List<LeafReaderContext> leaves = reader.leaves();
    final ReaderUtil.PartitionedHits partitioned =
        ReaderUtil.partitionByLeaf(globalDocIds, leaves);
    final int[][] docIdsByLeaf = partitioned.docIdsByLeaf();
    final int[][] ordinalsByLeaf = partitioned.ordinalsByLeaf();

    // One task per non-empty leaf. Each task writes disjoint rows of `values` (each ordinal is
    // unique across leaves), so no synchronization is needed.
    final List<Callable<Void>> tasks = new ArrayList<>();
    for (int leafIdx = 0; leafIdx < leaves.size(); leafIdx++) {
      final int[] leafDocs = docIdsByLeaf[leafIdx];
      if (leafDocs.length == 0) {
        continue;
      }
      final int[] leafOrds = ordinalsByLeaf[leafIdx];
      final LeafReaderContext leaf = leaves.get(leafIdx);
      tasks.add(
          () -> {
            retrieveLeaf(leaf, leafDocs, leafOrds, rewritten, values);
            return null;
          });
    }

    new TaskExecutor(executor).invokeAll(tasks);
    return values;
  }

  /**
   * Retrieves values for a single leaf: builds one {@link DoubleValues} cursor per source, then
   * walks the leaf's (ascending) doc IDs, writing each hit's row into {@code values} at its
   * original input ordinal.
   */
  private static void retrieveLeaf(
      LeafReaderContext leaf,
      int[] leafDocs,
      int[] leafOrds,
      DoubleValuesSource[] sources,
      double[][] values)
      throws IOException {
    final int docBase = leaf.docBase;
    final DoubleValues[] cursors = new DoubleValues[sources.length];
    for (int s = 0; s < sources.length; s++) {
      cursors[s] = sources[s].getValues(leaf, null);
    }
    for (int i = 0; i < leafDocs.length; i++) {
      final int localDoc = leafDocs[i] - docBase;
      final double[] row = new double[sources.length];
      for (int s = 0; s < sources.length; s++) {
        // TODO(discuss): missing-value policy. We report NaN when a doc has no value for a source.
        // Revisit for the abstract/visitor version.
        row[s] = cursors[s].advanceExact(localDoc) ? cursors[s].doubleValue() : Double.NaN;
      }
      values[leafOrds[i]] = row;
    }
  }
}
