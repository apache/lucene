package org.apache.lucene.queryparser.flexible.standard.nodes;

import java.util.Locale;
import java.util.Objects;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;

public class Extend extends IntervalFunction {
  private final int before, after;
  private final IntervalFunction source;

  public Extend(IntervalFunction source, int before, int after) {
    this.source = Objects.requireNonNull(source);
    this.before = before;
    this.after = after;
  }

  @Override
  public IntervalsSource toIntervalSource(String field, Analyzer analyzer) {
    return Intervals.extend(source.toIntervalSource(field, analyzer), before, after);
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT, "fn:extend(%s %d %d)", source, before, after);
  }
}
