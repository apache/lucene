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
package org.apache.lucene.util;

import java.io.IOException;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

/** A RangeDocIdSet contains a range docIds from minDocId (inclusive) to maxDocId (exclusive). */
public class RangeDocIdSet extends DocIdSet {

  private final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(RangeDocIdSet.class);

  private final int minDoc;
  private final int maxDoc;

  public RangeDocIdSet(int minDoc, int maxDoc) {
    if (minDoc >= maxDoc) {
      throw new IllegalArgumentException(
          "minDoc must be < maxDoc but got minDoc=" + minDoc + " maxDoc=" + maxDoc);
    }
    if (minDoc < 0) {
      throw new IllegalArgumentException("minDoc must be >= 0 but got minDoc=" + minDoc);
    }
    this.minDoc = minDoc;
    this.maxDoc = maxDoc;
  }

  @Override
  public DocIdSetIterator iterator() throws IOException {
    return DocIdSetIterator.range(minDoc, maxDoc);
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED;
  }

  @Override
  public Bits bits() throws IOException {
    return new Bits() {
      @Override
      public boolean get(int index) {
        return index >= minDoc && index < maxDoc;
      }

      @Override
      public int length() {
        // according to the implementation in FixedBitSet, this method should return maxDoc,
        // not number of document like (maxDoc - minDoc)
        return maxDoc;
      }
    };
  }
}
