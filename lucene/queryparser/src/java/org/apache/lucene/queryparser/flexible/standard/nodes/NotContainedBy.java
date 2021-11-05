package org.apache.lucene.queryparser.flexible.standard.nodes;

import java.util.Locale;
import java.util.Objects;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;

public class NotContainedBy extends IntervalFunction {
  private final IntervalFunction small;
  private final IntervalFunction big;

  public NotContainedBy(IntervalFunction small, IntervalFunction big) {
    this.small = Objects.requireNonNull(small);
    this.big = Objects.requireNonNull(big);
  }

  @Override
  public IntervalsSource toIntervalSource(String field, Analyzer analyzer) {
    return Intervals.notContainedBy(
        small.toIntervalSource(field, analyzer), big.toIntervalSource(field, analyzer));
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT, "fn:notContainedBy(%s %s)", small, big);
  }
}
