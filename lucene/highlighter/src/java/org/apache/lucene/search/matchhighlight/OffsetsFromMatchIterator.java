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
import java.util.List;
import java.util.Objects;
import org.apache.lucene.search.MatchesIterator;

/**
 * This strategy retrieves offsets directly from {@link MatchesIterator}, if they are available,
 * otherwise it falls back to using {@link OffsetsFromPositions}.
 */
public final class OffsetsFromMatchIterator implements OffsetsRetrievalStrategy {
  private final String field;
  private final OffsetsFromPositions noOffsetsFallback;

  OffsetsFromMatchIterator(String field, OffsetsFromPositions noOffsetsFallback) {
    this.field = field;
    this.noOffsetsFallback = Objects.requireNonNull(noOffsetsFallback);
  }

  @Override
  public List<OffsetRange> get(
      MatchesIterator matchesIterator, MatchRegionRetriever.FieldValueProvider doc)
      throws IOException {
    ArrayList<OffsetRange> positionRanges = new ArrayList<>();
    ArrayList<OffsetRange> offsetRanges = new ArrayList<>();
    while (matchesIterator.next()) {
      int fromPosition = matchesIterator.startPosition();
      int toPosition = matchesIterator.endPosition();
      if (fromPosition < 0 || toPosition < 0) {
        throw new IOException("Matches API returned negative positions for field: " + field);
      }
      positionRanges.add(new OffsetRange(fromPosition, toPosition));

      if (offsetRanges != null) {
        int from = matchesIterator.startOffset();
        int to = matchesIterator.endOffset();
        if (from < 0 || to < 0) {
          // At least one offset isn't available. Fallback to just positions.
          offsetRanges = null;
        } else {
          offsetRanges.add(new OffsetRange(from, to));
        }
      }
    }

    // Use the fallback conversion from positions if not all offsets were available.
    if (offsetRanges == null) {
      return noOffsetsFallback.convertPositionsToOffsets(positionRanges, doc.getValues(field));
    } else {
      return offsetRanges;
    }
  }
}
