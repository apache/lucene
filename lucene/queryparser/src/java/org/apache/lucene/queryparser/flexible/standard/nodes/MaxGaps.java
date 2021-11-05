package org.apache.lucene.queryparser.flexible.standard.nodes;

import java.util.Locale;
import java.util.Objects;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;

public class MaxGaps extends IntervalFunction {
  private final int maxGaps;
  private final IntervalFunction source;

  public MaxGaps(int maxGaps, IntervalFunction source) {
    this.maxGaps = maxGaps;
    this.source = Objects.requireNonNull(source);
  }

  @Override
  public IntervalsSource toIntervalSource(String field, Analyzer analyzer) {
    return Intervals.maxgaps(maxGaps, source.toIntervalSource(field, analyzer));
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT, "fn:maxgaps(%s %s)", maxGaps, source);
  }
}
