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

import java.io.Closeable;
import org.apache.lucene.index.Sorter;

/**
 * A supplier that creates {@link UpdateableRandomVectorScorer} from an ordinal. Caller should be
 * sure to close after use
 *
 * <p>NOTE: the {@link #copy()} returned {@link RandomVectorScorerSupplier} is not necessarily
 * closeable
 */
public interface CloseableRandomVectorScorerSupplier extends Closeable, RandomVectorScorerSupplier {
  int totalVectorCount();

  /**
   * If the vectors were reordered, this encodes the mapping from old (before reordering) node
   * ordinals to new (reordered) ones. If no reordering was done, this will be null. Note that the
   * RandomVectorScorer(s) returned from this will be based on the old ordinals; the reordering is
   * only indicative.
   */
  default Sorter.DocMap sortMap() {
    return null;
  }
}
