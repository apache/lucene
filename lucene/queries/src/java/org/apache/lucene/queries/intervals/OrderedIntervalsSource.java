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
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

class OrderedIntervalsSource extends MinimizingConjunctionIntervalsSource {

  static IntervalsSource build(List<IntervalsSource> sources) {
    if (sources.size() == 1) {
      return sources.get(0);
    }
    List<IntervalsSource> rewritten = deduplicate(sources);
    if (rewritten.size() == 1) {
      return rewritten.get(0);
    }
    return new OrderedIntervalsSource(rewritten);
  }

  private static List<IntervalsSource> deduplicate(List<IntervalsSource> sources) {
    List<IntervalsSource> deduplicated = new ArrayList<>();
    List<IntervalsSource> current = new ArrayList<>();
    for (IntervalsSource source : sources) {
      if (current.size() == 0 || current.get(0).equals(source)) {
        current.add(source);
      } else {
        deduplicated.add(RepeatingIntervalsSource.build(current.get(0), current.size()));
        current.clear();
        current.add(source);
      }
    }
    deduplicated.add(RepeatingIntervalsSource.build(current.get(0), current.size()));
    if (deduplicated.size() == 1 && deduplicated.get(0) instanceof RepeatingIntervalsSource) {
      ((RepeatingIntervalsSource) deduplicated.get(0)).setName("ORDERED");
    }
    return deduplicated;
  }

  private OrderedIntervalsSource(List<IntervalsSource> sources) {
    super(sources);
  }

  @Override
  protected IntervalIterator combine(List<IntervalIterator> iterators, MatchCallback onMatch) {
    return new OrderedIntervalIterator(iterators, onMatch);
  }

  @Override
  public int minExtent() {
    int minExtent = 0;
    for (IntervalsSource subSource : subSources) {
      minExtent += subSource.minExtent();
    }
    return minExtent;
  }

  @Override
  public Collection<IntervalsSource> pullUpDisjunctions() {
    return Disjunctions.pullUp(subSources, OrderedIntervalsSource::new);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(subSources);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof OrderedIntervalsSource == false) return false;
    OrderedIntervalsSource s = (OrderedIntervalsSource) other;
    return Objects.equals(subSources, s.subSources);
  }

  @Override
  public String toString() {
    return "ORDERED("
        + subSources.stream().map(IntervalsSource::toString).collect(Collectors.joining(","))
        + ")";
  }

  private static class OrderedIntervalIterator extends ConjunctionIntervalIterator {

    int start = -1, end = -1, i = 1;
    int slop;
    final MatchCallback onMatch;

    private OrderedIntervalIterator(List<IntervalIterator> subIntervals, MatchCallback onMatch) {
      super(subIntervals);
      this.onMatch = onMatch;
    }

    @Override
    public int start() {
      return start;
    }

    @Override
    public int end() {
      return end;
    }

    @Override
    public int nextInterval() throws IOException {
      start = end = slop = IntervalIterator.NO_MORE_INTERVALS;
      int lastStart = Integer.MAX_VALUE;
      boolean minimizing = false;
      final var subIterators = this.subIterators;
      int currentIndex = i;
      while (true) {
        int prevEnd = subIterators.get(currentIndex - 1).end();
        while (true) {
          if (prevEnd >= lastStart) {
            i = currentIndex;
            return start;
          }
          if (currentIndex == subIterators.size()) {
            break;
          }
          final IntervalIterator current = subIterators.get(currentIndex);
          if (minimizing && (current.start() > prevEnd)) {
            break;
          }
          int currentStart;
          do {
            if (current.end() >= lastStart
                || (currentStart = current.nextInterval()) == IntervalIterator.NO_MORE_INTERVALS) {
              i = currentIndex;
              return start;
            }
          } while (currentStart <= prevEnd);
          currentIndex++;
          prevEnd = current.end();
        }
        var first = subIterators.get(0);
        final int start = first.start();
        this.start = start;
        if (start == NO_MORE_INTERVALS) {
          i = currentIndex;
          return end = NO_MORE_INTERVALS;
        }
        var last = subIterators.getLast();

        final int end = last.end();
        this.end = end;
        int slop = end - start + 1;
        // use indexed loop since this is always a random access capable list to avoid allocations
        // in a hot nested loop
        for (int j = 0, n = subIterators.size(); j < n; j++) {
          slop -= subIterators.get(j).width();
        }
        this.slop = slop;
        onMatch.onMatch();
        currentIndex = 1;
        if (first.nextInterval() == IntervalIterator.NO_MORE_INTERVALS) {
          i = currentIndex;
          return start;
        }
        lastStart = last.start();
        minimizing = true;
      }
    }

    @Override
    public int gaps() {
      return slop;
    }

    @Override
    protected void reset() throws IOException {
      subIterators.get(0).nextInterval();
      i = 1;
      start = end = slop = -1;
    }
  }
}
