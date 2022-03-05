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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;

abstract class ConjunctionIntervalsSource extends IntervalsSource {

  protected final List<IntervalsSource> subSources;

  protected ConjunctionIntervalsSource(List<IntervalsSource> subSources) {
    assert subSources.size() > 1;
    this.subSources = subSources;
  }

  @Override
  public void visit(String field, QueryVisitor visitor) {
    Query parent = new IntervalQuery(field, this);
    QueryVisitor v = visitor.getSubVisitor(BooleanClause.Occur.MUST, parent);
    for (IntervalsSource source : subSources) {
      source.visit(field, v);
    }
  }

  @Override
  public final IntervalIterator intervals(String field, LeafReaderContext ctx) throws IOException {
    List<IntervalIterator> subIntervals = new ArrayList<>();
    for (IntervalsSource source : subSources) {
      IntervalIterator it = source.intervals(field, ctx);
      if (it == null) {
        return null;
      }
      subIntervals.add(it);
    }
    return combine(subIntervals);
  }

  protected abstract IntervalIterator combine(List<IntervalIterator> iterators);

  /**
   * Create matches iterator from an advanced and validated interval iterator and a list of matches
   * iterator of all the sub-sources
   */
  protected IntervalMatchesIterator createMatchesIterator(
      IntervalIterator it, List<IntervalMatchesIterator> subs) {
    return new ConjunctionMatchesIterator(it, subs);
  }

  @Override
  public final IntervalMatchesIterator matches(String field, LeafReaderContext ctx, int doc)
      throws IOException {
    List<IntervalMatchesIterator> subs = new ArrayList<>();
    for (IntervalsSource source : subSources) {
      IntervalMatchesIterator mi = source.matches(field, ctx, doc);
      if (mi == null) {
        return null;
      }
      subs.add(mi);
    }
    IntervalIterator it =
        combine(
            subs.stream()
                .map(m -> IntervalMatches.wrapMatches(m, doc))
                .collect(Collectors.toList()));
    if (it.advance(doc) != doc) {
      return null;
    }
    if (it.nextInterval() == IntervalIterator.NO_MORE_INTERVALS) {
      return null;
    }
    return createMatchesIterator(it, subs);
  }
}
