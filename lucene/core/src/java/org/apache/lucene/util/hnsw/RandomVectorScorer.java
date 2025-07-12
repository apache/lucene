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
 * A {@link RandomVectorScorer} for scoring random nodes in batches against an abstract query. This
 * class isn't thread-safe and should be used by a single thread.
 */
public interface RandomVectorScorer {
  /**
   * Returns the score between the query and the provided node.
   *
   * @param node a random node in the graph
   * @return the computed score
   */
  float score(int node) throws IOException;

  // TODO:
  default boolean supportsBulk() {
    return false;
  }

  /**
   * Bulk scores the given 4 nodes.
   *
   * @param scores the scores - must be of length >= 4
   * @param node1 a random node in the graph
   * @param node2 a random node in the graph
   * @param node3 a random node in the graph
   * @param node4 a random node in the graph
   */
  // TODO: consider replacing with supportsBulkScorer or Optional<BulkScorer>
  // so that can avoid potentially slower path?
  default void scoreBulk(float[] scores, int node1, int node2, int node3, int node4)
      throws IOException {
    throw new UnsupportedOperationException("bulk scoring is not supported");
    //    scores[0] = score(node1);
    //    scores[1] = score(node2);
    //    scores[2] = score(node3);
    //    scores[3] = score(node4);
  }

  /**
   * @return the maximum possible ordinal for this scorer
   */
  int maxOrd();

  /**
   * Translates vector ordinal to the correct document ID. By default, this is an identity function.
   *
   * @param ord the vector ordinal
   * @return the document Id for that vector ordinal
   */
  default int ordToDoc(int ord) {
    return ord;
  }

  /**
   * Returns the {@link Bits} representing live documents. By default, this is an identity function.
   *
   * @param acceptDocs the accept docs
   * @return the accept docs
   */
  default Bits getAcceptOrds(Bits acceptDocs) {
    return acceptDocs;
  }

  /** Creates a default scorer for random access vectors. */
  abstract class AbstractRandomVectorScorer implements RandomVectorScorer {
    private final KnnVectorValues values;

    /**
     * Creates a new scorer for the given vector values.
     *
     * @param values the vector values
     */
    public AbstractRandomVectorScorer(KnnVectorValues values) {
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
  }
}
