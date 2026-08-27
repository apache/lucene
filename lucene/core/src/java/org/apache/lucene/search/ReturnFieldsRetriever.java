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
import java.util.function.IntFunction;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;

/**
 * Generic engine for computing per-hit "return fields": given an {@link IndexReader} and an {@code
 * int[]} of global doc IDs (in any order), it produces one result per hit and returns them in input
 * order ({@code result[i]} is the value for {@code globalDocIds[i]}). The input array is not mutated.
 *
 * <p>It owns the scatter/gather machinery (partition via {@link ReaderUtil#partitionByLeaf}, run
 * leaves on the {@link Executor}, gather back to input order) but is agnostic about <em>what</em> is
 * produced per hit: the caller supplies a {@link LeafVisitorFactory} that turns a document into a
 * value of type {@code T}.
 *
 * <p><b>Concurrency contract.</b> Leaves run concurrently, so {@link
 * LeafVisitorFactory#newLeafVisitor} must return a <em>fresh</em>, single-threaded {@link
 * LeafVisitor} per leaf (like {@link CollectorManager#newCollector()}).
 *
 * @lucene.experimental
 */
public final class ReturnFieldsRetriever {

  private ReturnFieldsRetriever() {}

  /**
   * Creates a {@link LeafVisitor} bound to a single leaf. Called once per leaf, on the thread that
   * will process that leaf.
   *
   * @param <T> the per-hit result type
   */
  @FunctionalInterface
  public interface LeafVisitorFactory<T> {
    /**
     * Returns a fresh visitor for the given leaf. Must not return a shared/reused instance: leaves
     * are processed concurrently.
     */
    LeafVisitor<T> newLeafVisitor(LeafReaderContext leaf) throws IOException;
  }

  /**
   * Produces the result for a single hit within a leaf. Instances are single-threaded and bound to
   * one leaf.
   *
   * @param <T> the per-hit result type
   */
  @FunctionalInterface
  public interface LeafVisitor<T> {
    /**
     * Produces the result for one document. The engine calls this with leaf-local doc IDs in
     * ascending order.
     *
     * @param localDoc the doc ID relative to the leaf (i.e. {@code globalDocId - leaf.docBase})
     */
    T visit(int localDoc) throws IOException;
  }

  /**
   * Retrieves one result per global doc ID, in input order.
   *
   * @param reader an open index reader (the caller is responsible for its reference count)
   * @param globalDocIds global doc IDs in any order (e.g. ranking order); not mutated
   * @param factory creates a fresh per-leaf {@link LeafVisitor}; see the concurrency contract
   * @param arrayFactory allocates the result array of the required length (e.g. {@code
   *     String[]::new})
   * @param executor executor used to process leaves concurrently
   * @param <T> the per-hit result type
   * @return an array of length {@code globalDocIds.length}, where element {@code i} is the result
   *     for {@code globalDocIds[i]}
   */
  public static <T> T[] retrieve(
      IndexReader reader,
      int[] globalDocIds,
      LeafVisitorFactory<T> factory,
      IntFunction<T[]> arrayFactory,
      Executor executor)
      throws IOException {
    Objects.requireNonNull(reader, "reader");
    Objects.requireNonNull(globalDocIds, "globalDocIds");
    Objects.requireNonNull(factory, "factory");
    Objects.requireNonNull(arrayFactory, "arrayFactory");
    Objects.requireNonNull(executor, "executor");

    final T[] results = arrayFactory.apply(globalDocIds.length);
    if (globalDocIds.length == 0) {
      return results;
    }

    final List<LeafReaderContext> leaves = reader.leaves();
    final ReaderUtil.PartitionedHits partitioned =
        ReaderUtil.partitionByLeaf(globalDocIds, leaves);
    final int[][] docIdsByLeaf = partitioned.docIdsByLeaf();
    final int[][] ordinalsByLeaf = partitioned.ordinalsByLeaf();

    // One task per non-empty leaf. Each task writes disjoint slots of `results` (each ordinal is
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
            final LeafVisitor<T> visitor = factory.newLeafVisitor(leaf);
            final int docBase = leaf.docBase;
            for (int i = 0; i < leafDocs.length; i++) {
              results[leafOrds[i]] = visitor.visit(leafDocs[i] - docBase);
            }
            return null;
          });
    }

    new TaskExecutor(executor).invokeAll(tasks);
    return results;
  }
}
