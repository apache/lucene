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
package org.apache.lucene.queryparser.flexible.standard.nodes.intervalfn;

import java.util.Locale;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.search.FuzzyQuery;

/**
 * An interval function equivalent to {@link FuzzyQuery}. A fuzzy term expands to a disjunction of
 * intervals of terms that are within the specified {@code maxEdits} from the provided term. A limit
 * of {@code maxExpansions} prevents the internal implementation from blowing up on too many
 * potential candidate terms.
 */
public class FuzzyTerm extends IntervalFunction {
  private final String term;
  private final int maxEdits;
  private final Integer maxExpansions;

  public FuzzyTerm(String term, Integer maxEdits, Integer maxExpansions) {
    this.term = term;
    this.maxEdits = maxEdits == null ? FuzzyQuery.defaultMaxEdits : maxEdits;
    this.maxExpansions = maxExpansions == null ? Intervals.DEFAULT_MAX_EXPANSIONS : maxExpansions;
  }

  @Override
  public IntervalsSource toIntervalSource(String field, Analyzer analyzer) {
    return Intervals.fuzzyTerm(
        term,
        maxEdits,
        FuzzyQuery.defaultPrefixLength,
        FuzzyQuery.defaultTranspositions,
        maxExpansions);
  }

  @Override
  public String toString() {
    return String.format(
        Locale.ROOT,
        "fn:fuzzyTerm(%s %d%s)",
        AnalyzedText.requiresQuotes(term) ? '"' + term + '"' : term,
        maxEdits,
        maxExpansions);
  }
}
