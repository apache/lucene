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
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLongArray;
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
 * All per-slice collectors share a single {@link AtomicLongArray} sized to the global value count,
 * so memory is O(globalValueCount) regardless of the number of slices or segment cardinality.
 * Global ordinal remapping happens during collection, and {@link #reduce} assembles the final
 * {@link LongBitSet} from the shared array.
 */
final class GlobalOrdinalsCollectorManager implements CollectorManager<Collector, LongBitSet> {

  private final String field;
  private final OrdinalMap ordinalMap;
  private final long valueCount;
  // Single shared bitset written atomically by all collector slices.
  private final AtomicLongArray sharedBits;

  GlobalOrdinalsCollectorManager(String field, OrdinalMap ordinalMap, long valueCount) {
    this.field = field;
    this.ordinalMap = ordinalMap;
    this.valueCount = valueCount;
    this.sharedBits = new AtomicLongArray(LongBitSet.bits2words(valueCount));
  }

  @Override
  public Collector newCollector() {
    return new Collector() {
      @Override
      public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        SortedDocValues docTermOrds = DocValues.getSorted(context.reader(), field);
        LongValues globalOrds = ordinalMap == null ? null : ordinalMap.getGlobalOrds(context.ord);
        return new LeafCollector() {
          @Override
          public void setScorer(Scorable scorer) {}

          @Override
          public void collect(int doc) throws IOException {
            if (docTermOrds.advanceExact(doc)) {
              long segOrd = docTermOrds.ordValue();
              setGlobalOrdBit(globalOrds == null ? segOrd : globalOrds.get(segOrd));
            }
          }
        };
      }

      @Override
      public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
      }
    };
  }

  @Override
  public LongBitSet reduce(Collection<Collector> collectors) {
    int numWords = sharedBits.length();
    long[] words = new long[numWords];
    for (int i = 0; i < numWords; i++) {
      words[i] = sharedBits.get(i);
    }
    return new LongBitSet(words, valueCount);
  }

  private void setGlobalOrdBit(long globalOrd) {
    int wordIndex = (int) (globalOrd >> 6);
    long bit = 1L << globalOrd;
    long prev = sharedBits.get(wordIndex);
    while ((prev & bit) == 0) {
      if (sharedBits.compareAndSet(wordIndex, prev, prev | bit)) {
        break;
      }
      prev = sharedBits.get(wordIndex);
    }
  }
}
