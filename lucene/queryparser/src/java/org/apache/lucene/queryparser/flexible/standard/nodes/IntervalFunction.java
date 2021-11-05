package org.apache.lucene.queryparser.flexible.standard.nodes;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queries.intervals.IntervalsSource;

public abstract class IntervalFunction {
  public abstract IntervalsSource toIntervalSource(String field, Analyzer analyzer);

  @Override
  public abstract String toString();
}
