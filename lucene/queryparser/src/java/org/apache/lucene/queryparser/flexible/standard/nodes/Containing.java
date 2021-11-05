package org.apache.lucene.queryparser.flexible.standard.nodes;

import java.util.Locale;
import java.util.Objects;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;

public class Containing extends IntervalFunction {
  private final IntervalFunction big;
  private final IntervalFunction small;

  public Containing(IntervalFunction big, IntervalFunction small) {
    this.big = Objects.requireNonNull(big);
    this.small = Objects.requireNonNull(small);
  }

  @Override
  public IntervalsSource toIntervalSource(String field, Analyzer analyzer) {
    return Intervals.containing(
        big.toIntervalSource(field, analyzer), small.toIntervalSource(field, analyzer));
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT, "fn:containing(%s %s)", big, small);
  }
}
