package org.apache.lucene.queryparser.flexible.standard.nodes;

import java.util.Locale;
import java.util.Objects;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;

public class Within extends IntervalFunction {
  private final int positions;
  private final IntervalFunction source, reference;

  public Within(IntervalFunction source, int positions, IntervalFunction reference) {
    this.positions = positions;
    this.source = Objects.requireNonNull(source);
    this.reference = Objects.requireNonNull(reference);
  }

  @Override
  public IntervalsSource toIntervalSource(String field, Analyzer analyzer) {
    return Intervals.within(
        source.toIntervalSource(field, analyzer),
        positions,
        reference.toIntervalSource(field, analyzer));
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT, "fn:within(%s %d %s)", source, positions, reference);
  }
}
