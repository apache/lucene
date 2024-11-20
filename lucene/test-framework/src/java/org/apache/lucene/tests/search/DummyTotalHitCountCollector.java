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
package org.apache.lucene.tests.search;

import java.io.IOException;
import java.util.Collection;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.Weight;

/**
 * A dummy version of {@link TotalHitCountCollector} that doesn't shortcut using {@link
 * Weight#count}.
 */
public class DummyTotalHitCountCollector implements Collector {
  private int totalHits;

  /** Constructor */
  public DummyTotalHitCountCollector() {}

  /** Get the number of hits. */
  public int getTotalHits() {
    return totalHits;
  }

  @Override
  public ScoreMode scoreMode() {
    return ScoreMode.COMPLETE_NO_SCORES;
  }

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
    return new LeafCollector() {

      @Override
      public void setScorer(Scorable scorer) throws IOException {}

      @Override
      public void collect(int doc) throws IOException {
        totalHits++;
      }
    };
  }

  /** Create a collector manager. */
  public static CollectorManager<DummyTotalHitCountCollector, Integer> createManager() {
    return new CollectorManager<DummyTotalHitCountCollector, Integer>() {

      @Override
      public DummyTotalHitCountCollector newCollector() throws IOException {
        return new DummyTotalHitCountCollector();
      }

      @Override
      public Integer reduce(Collection<DummyTotalHitCountCollector> collectors) throws IOException {
        int sum = 0;
        for (DummyTotalHitCountCollector coll : collectors) {
          sum += coll.totalHits;
        }
        return sum;
      }
    };
  }
}
