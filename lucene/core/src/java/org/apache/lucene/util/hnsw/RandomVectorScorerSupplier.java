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

package org.apache.lucene.util.hnsw;

import java.io.IOException;

/**
 * A supplier that creates {@link RandomVectorScorer} from an ordinal.
 *
 * <p>Not thread-safe, use {@link #copy()} to return an instance suitable for sharing with another
 * thread. Each thread should have its own copy.
 *
 * @lucene.experimental
 */
public interface RandomVectorScorerSupplier {
  /**
   * This creates a {@link RandomVectorScorer} for scoring random nodes in batches against the given
   * ordinal.
   *
   * @param ord the ordinal of the node to compare
   * @return a new {@link RandomVectorScorer}
   */
  RandomVectorScorer scorer(int ord) throws IOException;

  /**
   * Returns the score between the given nodes.
   *
   * @param firstOrd the ordinal of the first node to compare
   * @param secondOrd the ordinal of the second node to compare
   * @return the score between the first and second nodes
   */
  float score(int firstOrd, int secondOrd) throws IOException;

  /**
   * Make a copy of the supplier, which will copy the underlying vectorValues so the copy is safe to
   * be used in another thread.
   */
  RandomVectorScorerSupplier copy() throws IOException;
}
