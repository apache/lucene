package org.apache.lucene.queryparser.flexible.standard.nodes;

import java.util.Locale;
import java.util.Objects;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;

public class Before extends IntervalFunction {
  private final IntervalFunction source;
  private final IntervalFunction reference;

  public Before(IntervalFunction source, IntervalFunction reference) {
    this.source = Objects.requireNonNull(source);
    this.reference = Objects.requireNonNull(reference);
  }

  @Override
  public IntervalsSource toIntervalSource(String field, Analyzer analyzer) {
    return Intervals.before(
        source.toIntervalSource(field, analyzer), reference.toIntervalSource(field, analyzer));
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT, "fn:before(%s %s)", source, reference);
  }
}
