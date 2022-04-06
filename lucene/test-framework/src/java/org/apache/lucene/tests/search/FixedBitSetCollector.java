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
package org.apache.lucene.tests.search;

import java.io.IOException;
import java.util.Collection;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.util.FixedBitSet;

/** Collector that accumulates matching docs in a {@link FixedBitSet} */
public class FixedBitSetCollector extends SimpleCollector {
  private final FixedBitSet bitSet;

  private int docBase;

  FixedBitSetCollector(int maxDoc) {
    this.bitSet = new FixedBitSet(maxDoc);
  }

  @Override
  protected void doSetNextReader(LeafReaderContext context) throws IOException {
    docBase = context.docBase;
  }

  @Override
  public void collect(int doc) throws IOException {
    bitSet.set(docBase + doc);
  }

  @Override
  public ScoreMode scoreMode() {
    return ScoreMode.COMPLETE_NO_SCORES;
  }

  /**
   * Creates a {@link CollectorManager} that can concurrently collect matching docs in a {@link
   * FixedBitSet}
   */
  public static CollectorManager<FixedBitSetCollector, FixedBitSet> createManager(int maxDoc) {
    return new CollectorManager<>() {
      @Override
      public FixedBitSetCollector newCollector() {
        return new FixedBitSetCollector(maxDoc);
      }

      @Override
      public FixedBitSet reduce(Collection<FixedBitSetCollector> collectors) {
        FixedBitSet reduced = new FixedBitSet(maxDoc);
        for (FixedBitSetCollector collector : collectors) {
          reduced.or(collector.bitSet);
        }
        return reduced;
      }
    };
  }
}
