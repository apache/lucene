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

import static org.apache.lucene.search.TotalHits.Relation.EQUAL_TO;
import static org.apache.lucene.search.TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;

import java.util.Collection;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;
import org.apache.lucene.util.PriorityQueue;

/** Util class for Scorer related methods */
class ScorerUtil {
  static long costWithMinShouldMatch(LongStream costs, int numScorers, int minShouldMatch) {
    // the idea here is the following: a boolean query c1,c2,...cn with minShouldMatch=m
    // could be rewritten to:
    // (c1 AND (c2..cn|msm=m-1)) OR (!c1 AND (c2..cn|msm=m))
    // if we assume that clauses come in ascending cost, then
    // the cost of the first part is the cost of c1 (because the cost of a conjunction is
    // the cost of the least costly clause)
    // the cost of the second part is the cost of finding m matches among the c2...cn
    // remaining clauses
    // since it is a disjunction overall, the total cost is the sum of the costs of these
    // two parts

    // If we recurse infinitely, we find out that the cost of a msm query is the sum of the
    // costs of the num_scorers - minShouldMatch + 1 least costly scorers
    final PriorityQueue<Long> pq =
        new PriorityQueue<Long>(numScorers - minShouldMatch + 1) {
          @Override
          protected boolean lessThan(Long a, Long b) {
            return a > b;
          }
        };
    costs.forEach(pq::insertWithOverflow);
    return StreamSupport.stream(pq.spliterator(), false).mapToLong(Number::longValue).sum();
  }

  static TotalHits costWithMinShouldMatch(
      Collection<ScorerSupplier> collection, int numScorers, int minShouldMatch, long upperBound) {
    int queueSize = Math.min(numScorers - minShouldMatch + 1, collection.size());
    final PriorityQueue<Long> pq =
        new PriorityQueue<Long>(queueSize) {
          @Override
          protected boolean lessThan(Long a, Long b) {
            return a > b;
          }
        };
    // Keep track of the last eliminated value that was added to the priority queue.
    long leastTopNScoreBound = upperBound;
    for (ScorerSupplier supplier : collection) {
      TotalHits totalHits = supplier.isEstimatedPointCountGreaterThanOrEqualTo(leastTopNScoreBound);
      if (totalHits.relation() == EQUAL_TO) {
        Long oldCost = pq.insertWithOverflow(totalHits.value());
        if (oldCost != null && leastTopNScoreBound > oldCost) {
          leastTopNScoreBound = oldCost;
        }
      }
    }
    long cost = StreamSupport.stream(pq.spliterator(), false).mapToLong(Number::longValue).sum();
    if (pq.size() < queueSize || cost > upperBound) {
      return new TotalHits(Math.max(cost, upperBound), GREATER_THAN_OR_EQUAL_TO);
    } else {
      return new TotalHits(cost, EQUAL_TO);
    }
  }
}
