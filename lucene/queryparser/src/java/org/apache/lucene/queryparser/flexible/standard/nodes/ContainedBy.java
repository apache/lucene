package org.apache.lucene.queryparser.flexible.standard.nodes;

import java.util.Locale;
import java.util.Objects;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;

public class ContainedBy extends IntervalFunction {
  private final IntervalFunction big;
  private final IntervalFunction small;

  public ContainedBy(IntervalFunction small, IntervalFunction big) {
    this.small = Objects.requireNonNull(small);
    this.big = Objects.requireNonNull(big);
  }

  @Override
  public IntervalsSource toIntervalSource(String field, Analyzer analyzer) {
    return Intervals.containedBy(
        small.toIntervalSource(field, analyzer), big.toIntervalSource(field, analyzer));
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT, "fn:containedBy(%s %s)", small, big);
  }
}
