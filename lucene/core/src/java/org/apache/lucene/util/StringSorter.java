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

/**
 * A {@link BytesRef} sorter tries to use a efficient radix sorter if {@link StringSorter#cmp} is a
 * {@link BytesRefComparator}, otherwise fallback to {@link StringSorter#fallbackSorter}
 *
 * @lucene.internal
 */
public abstract class StringSorter extends Sorter {

  private final Comparator<BytesRef> cmp;
  protected final BytesRefBuilder scratch1 = new BytesRefBuilder();
  protected final BytesRefBuilder scratch2 = new BytesRefBuilder();
  protected final BytesRefBuilder pivotBuilder = new BytesRefBuilder();
  protected final BytesRef scratchBytes1 = new BytesRef();
  protected final BytesRef scratchBytes2 = new BytesRef();
  protected final BytesRef pivot = new BytesRef();

  protected StringSorter(Comparator<BytesRef> cmp) {
    this.cmp = cmp;
  }

  protected abstract void get(BytesRefBuilder builder, BytesRef result, int i);

  @Override
  protected int compare(int i, int j) {
    get(scratch1, scratchBytes1, i);
    get(scratch2, scratchBytes2, j);
    return cmp.compare(scratchBytes1, scratchBytes2);
  }

  @Override
  public void sort(int from, int to) {
    if (cmp instanceof BytesRefComparator) {
      BytesRefComparator bCmp = (BytesRefComparator) cmp;
      radixSorter(bCmp).sort(from, to);
    } else {
      fallbackSorter(cmp).sort(from, to);
    }
  }

  /** A radix sorter for {@link BytesRef} */
  protected class MSBStringRadixSorter extends MSBRadixSorter {

    private final BytesRefComparator cmp;

    protected MSBStringRadixSorter(BytesRefComparator cmp) {
      super(cmp.comparedBytesCount);
      this.cmp = cmp;
    }

    @Override
    protected void swap(int i, int j) {
      StringSorter.this.swap(i, j);
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
  }

  protected Sorter radixSorter(BytesRefComparator cmp) {
    return new MSBStringRadixSorter(cmp);
  }

  protected Sorter fallbackSorter(Comparator<BytesRef> cmp) {
    return new IntroSorter() {
      @Override
      protected void swap(int i, int j) {
        StringSorter.this.swap(i, j);
      }

      @Override
      protected int compare(int i, int j) {
        get(scratch1, scratchBytes1, i);
        get(scratch2, scratchBytes2, j);
        return cmp.compare(scratchBytes1, scratchBytes2);
      }

      @Override
      protected void setPivot(int i) {
        get(pivotBuilder, pivot, i);
      }

      @Override
      protected int comparePivot(int j) {
        get(scratch1, scratchBytes1, j);
        return cmp.compare(pivot, scratchBytes1);
      }
    };
  }
}
