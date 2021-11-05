package org.apache.lucene.queryparser.flexible.standard.nodes;

import java.util.Locale;
import java.util.Objects;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;

public class MaxWidth extends IntervalFunction {
  private final int width;
  private final IntervalFunction source;

  public MaxWidth(int width, IntervalFunction source) {
    this.width = width;
    this.source = Objects.requireNonNull(source);
  }

  @Override
  public IntervalsSource toIntervalSource(String field, Analyzer analyzer) {
    return Intervals.maxwidth(width, source.toIntervalSource(field, analyzer));
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT, "fn:maxwidth(%s %s)", width, source);
  }
}
