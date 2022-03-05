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
import java.util.Objects;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;

/** Node that represents {@link Intervals#nonOverlapping(IntervalsSource, IntervalsSource)} . */
public class NonOverlapping extends IntervalFunction {
  private final IntervalFunction minuend;
  private final IntervalFunction subtrahend;

  public NonOverlapping(IntervalFunction minuend, IntervalFunction subtrahend) {
    this.minuend = Objects.requireNonNull(minuend);
    this.subtrahend = Objects.requireNonNull(subtrahend);
  }

  @Override
  public IntervalsSource toIntervalSource(String field, Analyzer analyzer) {
    return Intervals.nonOverlapping(
        minuend.toIntervalSource(field, analyzer), subtrahend.toIntervalSource(field, analyzer));
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT, "fn:nonOverlapping(%s %s)", minuend, subtrahend);
  }
}
