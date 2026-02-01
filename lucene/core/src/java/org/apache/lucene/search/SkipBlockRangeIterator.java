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

package org.apache.lucene.search;

import java.io.IOException;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.util.FixedBitSet;

/**
 * A DocIdSetIterator that returns all documents within DocValuesSkipper blocks that have minimum
 * and maximum values that fall within a specified range.
 */
public class SkipBlockRangeIterator extends AbstractDocIdSetIterator {

  private final DocValuesSkipper skipper;
  private final long minValue;
  private final long maxValue;

  /**
   * Creates a new SkipBlockRangeIterator
   *
   * @param skipper the DocValuesSkipper to use to check block bounds
   * @param minValue only return documents that lie within a block with a maximum value greater than
   *     this
   * @param maxValue only return documents that lie within a block with a minimum value less than
   *     this
   */
  public SkipBlockRangeIterator(DocValuesSkipper skipper, long minValue, long maxValue) {
    this.skipper = skipper;
    this.minValue = minValue;
    this.maxValue = maxValue;
  }

  @Override
  public int nextDoc() throws IOException {
    return advance(doc + 1);
  }

  @Override
  public int advance(int target) throws IOException {
    if (target <= skipper.maxDocID(0)) {
      // within current block
      if (doc > -1) {
        // already positioned, so we've checked bounds and know that we're in a matching block
        return doc = target;
      }
    } else {
      // Advance to target
      skipper.advance(target);
    }

    // Find the next matching block (could be the current block)
    skipper.advance(minValue, maxValue);
    return doc = Math.max(target, skipper.minDocID(0));
  }

  @Override
  public long cost() {
    return DocIdSetIterator.NO_MORE_DOCS;
  }

  @Override
  public int docIDRunEnd() throws IOException {
    int maxDoc = skipper.maxDocID(0);
    int nextLevel = 1;
    while (nextLevel < skipper.numLevels()
        && skipper.minValue(nextLevel) < maxValue
        && skipper.maxValue(nextLevel) > minValue) {
      maxDoc = skipper.maxDocID(nextLevel);
      nextLevel++;
    }
    return maxDoc + 1;
  }

  @Override
  public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
    while (doc < upTo) {
      int end = Math.min(upTo, docIDRunEnd());
      bitSet.set(doc - offset, end - offset);
      advance(end);
    }
  }
}
