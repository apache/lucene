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
import org.apache.lucene.util.IOIntConsumer;
import org.apache.lucene.util.MathUtil;

final class RangeDocIdStream extends DocIdStream {

  private int upTo;
  private final int max;

  RangeDocIdStream(int min, int max) {
    if (min >= max) {
      throw new IllegalArgumentException("min = " + min + " >= max = " + max);
    }
    this.upTo = min;
    this.max = max;
  }

  @Override
  public boolean mayHaveRemaining() {
    return upTo < max;
  }

  @Override
  public void forEach(int upTo, IOIntConsumer consumer) throws IOException {
    if (upTo > this.upTo) {
      upTo = Math.min(upTo, max);
      for (int doc = this.upTo; doc < upTo; ++doc) {
        consumer.accept(doc);
      }
      this.upTo = upTo;
    }
  }

  @Override
  public int count(int upTo) throws IOException {
    if (upTo > this.upTo) {
      upTo = Math.min(upTo, max);
      int count = upTo - this.upTo;
      this.upTo = upTo;
      return count;
    } else {
      return 0;
    }
  }

  @Override
  public int intoArray(int upTo, int[] array) {
    int start = this.upTo;
    upTo = Math.min(upTo, max);
    upTo = MathUtil.unsignedMin(upTo, start + array.length);
    if (upTo > start) {
      for (int doc = start; doc < upTo; ++doc) {
        array[doc - start] = doc;
      }
      this.upTo = upTo;
      return upTo - start;
    }
    return 0;
  }
}
