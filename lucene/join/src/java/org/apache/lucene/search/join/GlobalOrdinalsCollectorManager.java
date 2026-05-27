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
package org.apache.lucene.search.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.LongValues;

/**
 * A {@link CollectorManager} that collects all ordinals from a specified field matching the query.
 * Each per-slice collector tracks only segment-local ordinals (sized to the segment's value count),
 * and {@link #reduce} remaps them to global ordinals via the {@link OrdinalMap}.
 */
final class GlobalOrdinalsCollectorManager
    implements CollectorManager<GlobalOrdinalsCollectorManager.SegmentLocalCollector, LongBitSet> {

  private final String field;
  private final OrdinalMap ordinalMap;
  private final long valueCount;

  GlobalOrdinalsCollectorManager(String field, OrdinalMap ordinalMap, long valueCount) {
    this.field = field;
    this.ordinalMap = ordinalMap;
    this.valueCount = valueCount;
  }

  @Override
  public SegmentLocalCollector newCollector() {
    return new SegmentLocalCollector();
  }

  @Override
  public LongBitSet reduce(Collection<SegmentLocalCollector> collectors) throws IOException {
    LongBitSet result = new LongBitSet(valueCount);
    for (SegmentLocalCollector collector : collectors) {
      for (int i = 0; i < collector.segmentBits.size(); i++) {
        LongBitSet segmentBits = collector.segmentBits.get(i);
        if (ordinalMap != null) {
          LongValues segToGlobal = ordinalMap.getGlobalOrds(collector.segmentOrds.get(i));
          for (long ord = segmentBits.nextSetBit(0); ord != -1; ) {
            result.set(segToGlobal.get(ord));
            long next = ord + 1;
            ord = next < segmentBits.length() ? segmentBits.nextSetBit(next) : -1;
          }
        } else {
          result.or(segmentBits);
        }
      }
    }
    return result;
  }

  final class SegmentLocalCollector implements Collector {

    final ArrayList<Integer> segmentOrds = new ArrayList<>();
    final ArrayList<LongBitSet> segmentBits = new ArrayList<>();

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      SortedDocValues docTermOrds = DocValues.getSorted(context.reader(), field);
      long segmentValueCount = docTermOrds.getValueCount();
      if (segmentValueCount == 0) {
        return new LeafCollector() {
          @Override
          public void setScorer(Scorable scorer) {}

          @Override
          public void collect(int doc) {}
        };
      }
      LongBitSet bits = new LongBitSet(segmentValueCount);
      segmentOrds.add(context.ord);
      segmentBits.add(bits);
      return new LeafCollector() {
        @Override
        public void setScorer(Scorable scorer) {}

        @Override
        public void collect(int doc) throws IOException {
          if (docTermOrds.advanceExact(doc)) {
            bits.set(docTermOrds.ordValue());
          }
        }
      };
    }

    @Override
    public ScoreMode scoreMode() {
      return ScoreMode.COMPLETE_NO_SCORES;
    }
  }
}
