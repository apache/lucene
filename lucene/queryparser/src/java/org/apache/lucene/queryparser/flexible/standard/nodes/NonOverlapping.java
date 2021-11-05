package org.apache.lucene.queryparser.flexible.standard.nodes;

import java.util.Locale;
import java.util.Objects;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;

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
