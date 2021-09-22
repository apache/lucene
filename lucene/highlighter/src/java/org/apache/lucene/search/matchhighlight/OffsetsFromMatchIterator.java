/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.search.matchhighlight;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.queries.intervals.ExtendedIntervalsSource;
import org.apache.lucene.queries.intervals.IntervalQuery;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.Query;

/** This strategy retrieves offsets directly from {@link MatchesIterator}. */
public final class OffsetsFromMatchIterator implements OffsetsRetrievalStrategy {
  private final String field;
  private final Analyzer analyzer;

  OffsetsFromMatchIterator(String field, Analyzer analyzer) {
    this.field = field;
    this.analyzer = analyzer;
  }

  @Override
  public List<OffsetRange> get(
      MatchesIterator matchesIterator, MatchRegionRetriever.FieldValueProvider doc)
      throws IOException {
    ArrayList<OffsetRange> ranges = new ArrayList<>();
    while (matchesIterator.next()) {
      int from = matchesIterator.startOffset();
      int to = matchesIterator.endOffset();
      if (from < 0 || to < 0) {
        throw new IOException("Matches API returned negative offsets for field: " + field);
      }
      ranges.add(new OffsetRange(from, to));
    }

    // nocommit seems too many type checking & casting?
    Query query = matchesIterator.getQuery();
    if (query != null && query instanceof IntervalQuery) {
      IntervalsSource intervalsSource = ((IntervalQuery) query).getIntervalsSource();
      if (intervalsSource instanceof ExtendedIntervalsSource) {
        ExtendedIntervalsSource extendedIntervalsSource = (ExtendedIntervalsSource) intervalsSource;
        // nocommit is there better way to get before / after values from ExtendedIntervalsSource ?
        int before = extendedIntervalsSource.getBefore();
        int after = extendedIntervalsSource.getAfter();

        if (before > 0 || after > 0) {
          return adjustOffsetRange(doc, ranges, before, after);
        }
      }
    }

    return ranges;
  }

  private List<OffsetRange> adjustOffsetRange(
      MatchRegionRetriever.FieldValueProvider doc,
      ArrayList<OffsetRange> ranges,
      int before,
      int after)
      throws IOException {
    int position = -1;
    int valueOffset = 0;

    Map<Integer, OffsetsPair> positionToOffsets = new HashMap<>();
    Map<Integer, Integer> startOffsetToPosition = new HashMap<>();
    Map<Integer, Integer> endOffsetToPosition = new HashMap<>();

    List<CharSequence> values = doc.getValues(field);
    for (CharSequence charSequence : values) {
      final String value = charSequence.toString();

      TokenStream ts = analyzer.tokenStream(field, value);
      OffsetAttribute offsetAttr = ts.getAttribute(OffsetAttribute.class);
      PositionIncrementAttribute posAttr = ts.getAttribute(PositionIncrementAttribute.class);
      ts.reset();

      // go through all tokens to collect position from / to offset mappings
      while (ts.incrementToken()) {
        position += posAttr.getPositionIncrement();
        int startOffset = valueOffset + offsetAttr.startOffset();
        int endOffset = valueOffset + offsetAttr.endOffset();

        positionToOffsets.put(position, new OffsetsPair(startOffset, endOffset));
        startOffsetToPosition.put(startOffset, position);
        endOffsetToPosition.put(endOffset, position);
      }
      ts.end();
      position += posAttr.getPositionIncrement() + analyzer.getPositionIncrementGap(field);
      valueOffset += offsetAttr.endOffset() + analyzer.getOffsetGap(field);
      ts.close();
    }

    int maxPosition = position - analyzer.getPositionIncrementGap(field);

    ArrayList<OffsetRange> extendedRanges = new ArrayList<>();
    for (OffsetRange original : ranges) {
      int originalStartOffset = original.from;
      int originalEndOffset = original.to;
      int originalStartPosition = startOffsetToPosition.get(originalStartOffset);
      int originalEndPosition = endOffsetToPosition.get(originalEndOffset);

      int extendedStartPosition = Math.max(0, originalStartPosition - before);
      // nocommit needs to handle overflow
      int extendedEndPosition = Math.min(maxPosition, originalEndPosition + after);

      assert extendedStartPosition >= 0;
      assert extendedEndPosition >= 0;

      // nocommit is the following correct handling of highlighting when there's also token
      // filtering such as stopword?
      // if extendedStartPosition is not available due to stopword filtered out etc, find the
      // next possible extendedStartPosition
      // it's very likely that the next available extendedStartPosition requires only once or
      // twice increment, hence "brute-force" scan is used here
      while (!positionToOffsets.containsKey(extendedStartPosition)) {
        extendedStartPosition++;
      }

      // nocommit is the following correct handling of highlighting when there's also token
      // filtering such as stopword?
      // if extendedEndPosition is not available due to stopword filtered out etc, find the
      // next possible extendedEndPosition
      // it's very likely that the next available extendedEndPosition requires only once or
      // twice decrement, hence "brute-force" scan is used here
      while (!positionToOffsets.containsKey(extendedEndPosition)) {
        extendedEndPosition--;
      }

      int extendedStartOffset = positionToOffsets.get(extendedStartPosition).startOffset;
      int extendedEndOffset = positionToOffsets.get(extendedEndPosition).endOffset;

      extendedRanges.add(new OffsetRange(extendedStartOffset, extendedEndOffset));
    }
    return extendedRanges;
  }

  @Override
  public boolean requiresDocument() {
    return true;
  }

  private class OffsetsPair {
    int startOffset;
    int endOffset;

    public OffsetsPair(int startOffset, int endOffset) {
      this.startOffset = startOffset;
      this.endOffset = endOffset;
    }
  }
}
