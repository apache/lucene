package org.apache.lucene.queryparser.flexible.standard.nodes;

import java.util.Locale;
import java.util.Objects;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;

public class NotWithin extends IntervalFunction {
  private final int positions;
  private final IntervalFunction minuend, subtrahend;

  public NotWithin(IntervalFunction minuend, int positions, IntervalFunction subtrahend) {
    this.positions = positions;
    this.minuend = Objects.requireNonNull(minuend);
    this.subtrahend = Objects.requireNonNull(subtrahend);
  }

  @Override
  public IntervalsSource toIntervalSource(String field, Analyzer analyzer) {
    return Intervals.notWithin(
        minuend.toIntervalSource(field, analyzer),
        positions,
        subtrahend.toIntervalSource(field, analyzer));
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT, "fn:notWithin(%s %d %s)", minuend, positions, subtrahend);
  }
}
