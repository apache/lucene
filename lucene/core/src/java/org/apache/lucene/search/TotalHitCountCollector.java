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
import org.apache.lucene.index.LeafReaderContext;

/**
 * Just counts the total number of hits. This is the collector behind {@link IndexSearcher#count}.
 * When the {@link Weight} implements {@link Weight#count}, this collector will skip collecting
 * segments.
 */
public class TotalHitCountCollector implements Collector {
  private Weight weight;
  private int totalHits;

  /** Returns how many hits matched the search. */
  public int getTotalHits() {
    return totalHits;
  }

  @Override
  public ScoreMode scoreMode() {
    return ScoreMode.COMPLETE_NO_SCORES;
  }

  @Override
  public void setWeight(Weight weight) {
    this.weight = weight;
  }

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
    int leafCount = weight == null ? -1 : weight.count(context);
    if (leafCount != -1) {
      totalHits += leafCount;
      throw new CollectionTerminatedException();
    }
    return new LeafCollector() {

      @Override
      public void setScorer(Scorable scorer) throws IOException {}

      @Override
      public void collect(int doc) throws IOException {
        totalHits++;
      }
    };
  }
}
