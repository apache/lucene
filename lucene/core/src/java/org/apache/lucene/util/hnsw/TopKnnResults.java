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

/**
 * TopKnnResults is a specific KnnResults, enforcing a minHeap is utilized for results. There is no
 * special logic other than keeping track of the topK results for kNN
 */
public class TopKnnResults extends KnnResults {
  /** A provider used to construct a new {@link TopKnnResults} */
  public static class Provider implements KnnResultsProvider {
    private final int k;

    public Provider(int k) {
      this.k = k;
    }

    @Override
    public int k() {
      return k;
    }

    @Override
    public KnnResults getKnnResults(IntToIntFunction vectorToOrd) {
      return new TopKnnResults(k, vectorToOrd);
    }
  }

  public TopKnnResults(int k, IntToIntFunction vectorToOrd) {
    super(k, vectorToOrd);
  }

  @Override
  protected void doClear() {}

  @Override
  public String toString() {
    return "TopKnnResults[" + size() + "]";
  }
}
