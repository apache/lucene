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
import org.apache.lucene.util.FixedBitSet;

final class BitSetDocIdStream extends DocIdStream {

  private final FixedBitSet bitSet;
  private final int offset, max;
  private int upTo;

  BitSetDocIdStream(FixedBitSet bitSet, int offset) {
    this.bitSet = bitSet;
    this.offset = offset;
    upTo = offset;
    max = (int) Math.min(Integer.MAX_VALUE, (long) offset + bitSet.length());
  }

  @Override
  public boolean mayHaveRemaining() {
    return upTo < max;
  }

  @Override
  public void forEach(int upTo, CheckedIntConsumer<IOException> consumer) throws IOException {
    if (upTo > this.upTo) {
      upTo = Math.min(upTo, max);
      bitSet.forEach(this.upTo - offset, upTo - offset, offset, consumer);
      this.upTo = upTo;
    }
  }

  @Override
  public int count(int upTo) throws IOException {
    if (upTo > this.upTo) {
      upTo = Math.min(upTo, max);
      int count = bitSet.cardinality(this.upTo - offset, upTo - offset);
      this.upTo = upTo;
      return count;
    } else {
      return 0;
    }
  }
}
