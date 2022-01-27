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

/**
 * Just counts the total number of hits. For cases when this is the only collector used, {@link
 * IndexSearcher#count(Query)} should be called instead of {@link IndexSearcher#search(Query,
 * Collector)} as the former is faster whenever the count can be returned directly from the index
 * statistics.
 */
public class TotalHitCountCollector extends SimpleCollector {
  private int totalHits;

  /** Returns how many hits matched the search. */
  public int getTotalHits() {
    return totalHits;
  }

  @Override
  public void collect(int doc) {
    totalHits++;
  }

  @Override
  public ScoreMode scoreMode() {
    return ScoreMode.COMPLETE_NO_SCORES;
  }
}
