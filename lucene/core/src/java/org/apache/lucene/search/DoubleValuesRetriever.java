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
import java.util.Objects;
import java.util.concurrent.Executor;
import org.apache.lucene.index.IndexReader;

/**
 * Retrieves {@link DoubleValuesSource} values for an {@code int[]} of global doc IDs, returning a
 * {@code double[hit][source]} grid in input order: {@code result[i][s]} is {@code sources[s]}'s
 * value for {@code globalDocIds[i]}. The input array is not mutated.
 *
 * <p>A {@code DoubleValuesSource}-specific specialization of {@link ReturnFieldsRetriever}. A doc
 * with no value for a source yields {@link Double#NaN}.
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
   * @throws IllegalArgumentException if any source needs scores (this retriever supplies none)
   */
  public static double[][] retrieve(
      IndexReader reader, int[] globalDocIds, DoubleValuesSource[] sources, Executor executor)
      throws IOException {
    Objects.requireNonNull(reader, "reader");
    Objects.requireNonNull(sources, "sources");

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

    return ReturnFieldsRetriever.retrieve(
        reader,
        globalDocIds,
        leaf -> {
          final DoubleValues[] cursors = new DoubleValues[rewritten.length];
          for (int s = 0; s < rewritten.length; s++) {
            cursors[s] = rewritten[s].getValues(leaf, null);
          }
          return localDoc -> {
            final double[] row = new double[cursors.length];
            for (int s = 0; s < cursors.length; s++) {
              row[s] = cursors[s].advanceExact(localDoc) ? cursors[s].doubleValue() : Double.NaN;
            }
            return row;
          };
        },
        double[][]::new,
        executor);
  }
}
