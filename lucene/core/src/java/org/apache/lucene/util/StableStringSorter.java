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

import java.util.Comparator;

abstract class StableStringSorter extends StringSorter {

  StableStringSorter(Comparator<BytesRef> cmp) {
    super(cmp);
  }

  /** Save the i-th value into the j-th position in temporary storage. */
  protected abstract void save(int i, int j);

  /** Restore values between i-th and j-th(excluding) in temporary storage into original storage. */
  protected abstract void restore(int i, int j);

  @Override
  protected Sorter radixSorter(BytesRefComparator cmp) {
    return new StableMSBRadixSorter(cmp.comparedBytesCount) {

      @Override
      protected void save(int i, int j) {
        StableStringSorter.this.save(i, j);
      }

      @Override
      protected void restore(int i, int j) {
        StableStringSorter.this.restore(i, j);
      }

      @Override
      protected void swap(int i, int j) {
        StableStringSorter.this.swap(i, j);
      }

      @Override
      protected int byteAt(int i, int k) {
        get(scratch1, scratchBytes1, i);
        return cmp.byteAt(scratchBytes1, k);
      }

      @Override
      protected Sorter getFallbackSorter(int k) {
        return fallbackSorter((o1, o2) -> cmp.compare(o1, o2, k));
      }
    };
  }

  @Override
  protected Sorter fallbackSorter(Comparator<BytesRef> cmp) {
    // TODO: Maybe tim sort is better?
    return new InPlaceMergeSorter() {
      @Override
      protected int compare(int i, int j) {
        return StableStringSorter.this.compare(i, j);
      }

      @Override
      protected void swap(int i, int j) {
        StableStringSorter.this.swap(i, j);
      }
    };
  }
}
