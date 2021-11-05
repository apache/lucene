package org.apache.lucene.queryparser.flexible.standard.nodes;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;

public class AnalyzedText extends IntervalFunction {
  private final String term;

  public AnalyzedText(String term) {
    this.term = term;
  }

  @Override
  public IntervalsSource toIntervalSource(String field, Analyzer analyzer) {
    int gaps = 0;
    boolean ordered = true;
    try {
      return Intervals.analyzedText(term, analyzer, field, gaps, ordered);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String toString() {
    if (requiresQuotes(term)) {
      return '"' + term + '"';
    } else {
      return term;
    }
  }

  private boolean requiresQuotes(String term) {
    return Pattern.compile("[\\s]").matcher(term).find();
  }
}
