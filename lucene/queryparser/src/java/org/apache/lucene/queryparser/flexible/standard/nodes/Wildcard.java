package org.apache.lucene.queryparser.flexible.standard.nodes;

import java.util.Locale;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.util.BytesRef;

public class Wildcard extends IntervalFunction {
  private final String wildcard;

  public Wildcard(String wildcard) {
    this.wildcard = wildcard;
  }

  @Override
  public IntervalsSource toIntervalSource(String field, Analyzer analyzer) {
    return Intervals.wildcard(new BytesRef(wildcard));
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT, "fn:wildcard(%s)", wildcard);
  }
}
