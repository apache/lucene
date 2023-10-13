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
import org.apache.lucene.util.BytesRef;

/** Node that represents {@link Intervals#wildcard(BytesRef)}. */
public class Wildcard extends IntervalFunction {
  private final String wildcard;
  private final int maxExpansions;

  public Wildcard(String wildcard, int maxExpansions) {
    this.wildcard = wildcard;
    this.maxExpansions = maxExpansions;
  }

  @Override
  public IntervalsSource toIntervalSource(String field, Analyzer analyzer) {
    if (maxExpansions == 0) {
      return Intervals.wildcard(new BytesRef(wildcard));
    } else {
      return Intervals.wildcard(new BytesRef(wildcard), maxExpansions);
    }
  }

  @Override
  public String toString() {
    return String.format(
        Locale.ROOT,
        "fn:wildcard(%s%s)",
        wildcard,
        maxExpansions == 0 ? "" : " maxExpansions:" + maxExpansions);
  }
}
