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

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;

/** Node that represents {@link Intervals#analyzedText(String, Analyzer, String, int, boolean)}. */
public class AnalyzedText extends IntervalFunction {
  private final String term;

  public AnalyzedText(String term) {
    this.term = term;
  }

  @Override
  public IntervalsSource toIntervalSource(String field, Analyzer analyzer) {
    int gaps = 0;
    boolean ordered = true;
    try {
      return Intervals.analyzedText(term, analyzer, field, gaps, ordered);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String toString() {
    if (requiresQuotes(term)) {
      return '"' + term + '"';
    } else {
      return term;
    }
  }

  static boolean requiresQuotes(String term) {
    return Pattern.compile("[\\s]").matcher(term).find();
  }
}
