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
import org.apache.lucene.search.FilterMatchesIterator;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.MatchesUtils;
import org.apache.lucene.search.Query;

class ConjunctionMatchesIterator implements IntervalMatchesIterator {

  final IntervalIterator iterator;
  final List<? extends IntervalMatchesIterator> subs;
  boolean cached = true;

  ConjunctionMatchesIterator(
      IntervalIterator iterator, List<? extends IntervalMatchesIterator> subs) {
    this.iterator = iterator;
    this.subs = subs;
  }

  @Override
  public boolean next() throws IOException {
    if (cached) {
      cached = false;
      return true;
    }
    return iterator.nextInterval() != IntervalIterator.NO_MORE_INTERVALS;
  }

  @Override
  public int startPosition() {
    return iterator.start();
  }

  @Override
  public int endPosition() {
    return iterator.end();
  }

  @Override
  public int startOffset() throws IOException {
    int start = Integer.MAX_VALUE;
    for (MatchesIterator s : subs) {
      int v = s.startOffset();
      if (v == -1) {
        return -1;
      }
      start = Math.min(start, v);
    }
    return start;
  }

  @Override
  public int endOffset() throws IOException {
    int end = -1;
    for (MatchesIterator s : subs) {
      int v = s.endOffset();
      if (v == -1) {
        return -1;
      }
      end = Math.max(end, v);
    }
    return end;
  }

  @Override
  public MatchesIterator getSubMatches() throws IOException {
    List<MatchesIterator> subMatches = new ArrayList<>();
    for (MatchesIterator mi : subs) {
      MatchesIterator sub = mi.getSubMatches();
      if (sub == null) {
        sub = new SingletonMatchesIterator(mi);
      }
      subMatches.add(sub);
    }
    return MatchesUtils.disjunction(subMatches);
  }

  @Override
  public Query getQuery() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int gaps() {
    return iterator.gaps();
  }

  @Override
  public int width() {
    return iterator.width();
  }

  static class SingletonMatchesIterator extends FilterMatchesIterator {

    boolean exhausted = false;

    SingletonMatchesIterator(MatchesIterator in) {
      super(in);
    }

    @Override
    public boolean next() {
      if (exhausted) {
        return false;
      }
      return exhausted = true;
    }
  }
}
