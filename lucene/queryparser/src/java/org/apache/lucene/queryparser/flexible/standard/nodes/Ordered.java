package org.apache.lucene.queryparser.flexible.standard.nodes;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;

public class Ordered extends IntervalFunction {
  private final List<IntervalFunction> sources;

  public Ordered(List<IntervalFunction> sources) {
    this.sources = Objects.requireNonNull(sources);
  }

  @Override
  public IntervalsSource toIntervalSource(String field, Analyzer analyzer) {
    return Intervals.ordered(
        sources.stream()
            .map(intervalFunction -> intervalFunction.toIntervalSource(field, analyzer))
            .toArray(IntervalsSource[]::new));
  }

  @Override
  public String toString() {
    return String.format(
        Locale.ROOT,
        "fn:ordered(%s)",
        sources.stream().map(IntervalFunction::toString).collect(Collectors.joining(" ")));
  }
}
