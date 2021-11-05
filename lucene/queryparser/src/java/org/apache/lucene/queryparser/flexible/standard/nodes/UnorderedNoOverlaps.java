package org.apache.lucene.queryparser.flexible.standard.nodes;

import java.util.Locale;
import java.util.Objects;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;

public class UnorderedNoOverlaps extends IntervalFunction {
  private final IntervalFunction a, b;

  public UnorderedNoOverlaps(IntervalFunction a, IntervalFunction b) {
    this.a = Objects.requireNonNull(a);
    this.b = Objects.requireNonNull(b);
  }

  @Override
  public IntervalsSource toIntervalSource(String field, Analyzer analyzer) {
    return Intervals.unorderedNoOverlaps(
        a.toIntervalSource(field, analyzer), b.toIntervalSource(field, analyzer));
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT, "fn:unorderedNoOverlaps(%s %s)", a, b);
  }
}
