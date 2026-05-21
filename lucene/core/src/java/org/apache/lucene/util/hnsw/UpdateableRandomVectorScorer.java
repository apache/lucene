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
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.util.Bits;

/**
 * Just like a {@link RandomVectorScorer} but allows the scoring ordinal to be changed. Useful
 * during indexing operations
 *
 * @lucene.internal
 */
public interface UpdateableRandomVectorScorer extends RandomVectorScorer {
  /**
   * Changes the scoring ordinal to the given node. If the same scorer object is being used
   * continually, this can be used to avoid creating a new scorer for each node.
   *
   * @param node the node to score against
   * @throws IOException if an exception occurs initializing the scorer for the given node
   */
  void setScoringOrdinal(int node) throws IOException;

  /** Creates a default scorer for random access vectors. */
  abstract class AbstractUpdateableRandomVectorScorer
      implements UpdateableRandomVectorScorer, HasKnnVectorValues {
    private final KnnVectorValues values;

    /**
     * Creates a new scorer for the given vector values.
     *
     * @param values the vector values
     */
    public AbstractUpdateableRandomVectorScorer(KnnVectorValues values) {
      this.values = values;
    }

    @Override
    public int maxOrd() {
      return values.size();
    }

    @Override
    public int ordToDoc(int ord) {
      return values.ordToDoc(ord);
    }

    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
      return values.getAcceptOrds(acceptDocs);
    }

    @Override
    public KnnVectorValues values() {
      return values;
    }
  }
}
