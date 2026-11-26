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

package org.apache.lucene.queries.intervals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.lucene.search.IndexSearcher;

final class Disjunctions {

  // Given a list of sources that contain disjunctions, and a combiner function,
  // pulls the disjunctions to the top of the source tree

  // eg FUNC(a, b, OR(c, "d e")) => [FUNC(a, b, c), FUNC(a, b, "d e")]

  public static List<IntervalsSource> pullUp(
      List<IntervalsSource> sources, Function<List<IntervalsSource>, IntervalsSource> function) {

    List<List<IntervalsSource>> rewritten = new ArrayList<>();
    rewritten.add(new ArrayList<>());
    for (IntervalsSource source : sources) {
      List<IntervalsSource> disjuncts = splitDisjunctions(source);
      if (disjuncts.size() == 1) {
        rewritten.forEach(l -> l.add(disjuncts.get(0)));
      } else {
        if (rewritten.size() * disjuncts.size() > IndexSearcher.getMaxClauseCount()) {
          throw new IllegalArgumentException("Too many disjunctions to expand");
        }
        List<List<IntervalsSource>> toAdd = new ArrayList<>();
        for (IntervalsSource disj : disjuncts) {
          // clone the rewritten list, then append the disjunct
          for (List<IntervalsSource> subList : rewritten) {
            List<IntervalsSource> l = new ArrayList<>(subList);
            l.add(disj);
            toAdd.add(l);
          }
        }
        rewritten = toAdd;
      }
    }
    if (rewritten.size() == 1) {
      return Collections.singletonList(function.apply(rewritten.get(0)));
    }
    return rewritten.stream().map(function).toList();
  }

  // Given a source containing disjunctions, and a mapping function,
  // pulls the disjunctions to the top of the source tree
  public static List<IntervalsSource> pullUp(
      IntervalsSource source, Function<IntervalsSource, IntervalsSource> function) {
    List<IntervalsSource> disjuncts = splitDisjunctions(source);
    if (disjuncts.size() == 1) {
      return Collections.singletonList(function.apply(disjuncts.get(0)));
    }
    return disjuncts.stream().map(function).toList();
  }

  // Separate out disjunctions into individual sources
  // Clauses that always match intervals of the same fixed width are grouped together by width
  // and treated as a single source, as intervals of equal width can never strictly contain one
  // another, so minimization cannot cause one to hide another. This covers single terms (width 1)
  // and, for example, phrases of the same length. Variable-width clauses are kept separate.
  private static List<IntervalsSource> splitDisjunctions(IntervalsSource source) {
    Map<Integer, List<IntervalsSource>> fixedByWidth = new LinkedHashMap<>();
    List<IntervalsSource> variableWidth = new ArrayList<>();
    for (IntervalsSource disj : source.pullUpDisjunctions()) {
      int width = disj.minExtent();
      if (width == disj.maxExtent()) {
        fixedByWidth.computeIfAbsent(width, _ -> new ArrayList<>()).add(disj);
      } else {
        variableWidth.add(disj);
      }
    }
    List<IntervalsSource> split = new ArrayList<>();
    for (List<IntervalsSource> group : fixedByWidth.values()) {
      split.add(group.size() == 1 ? group.get(0) : Intervals.or(group));
    }
    split.addAll(variableWidth);
    return split;
  }
}
