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

/**
 * Stable radix sorter for variable-length strings. It has to check data beforehand to see if stable
 * sort can be enabled or not. If not applicable, it works the same way as its parent class {@link
 * MSBRadixSorter}.
 *
 * @lucene.internal
 */
public abstract class StableMSBRadixSorter extends MSBRadixSorter {

  protected boolean useStableSort;

  public StableMSBRadixSorter(int maxLength) {
    super(maxLength);
  }

  /** Check if stable sort can be enabled or not. */
  protected boolean isEnableStableSort(int from, int to) {
    return false;
  }

  /**
   * If stable sort is applicable, do some closure logic, which might update some of the internal
   * variable.
   */
  protected void doClosureIfStableSortEnabled() {}

  /** Assign the from-th value to to-th position in another array which used temporarily. */
  protected void assign(int from, int to) {
    throw new UnsupportedOperationException("not implement");
  }

  /** Finalize assign operation, to switch array. */
  protected void finalizeAssign(int from, int to) {
    throw new UnsupportedOperationException("not implement");
  }

  @Override
  public void sort(int from, int to) {
    checkRange(from, to);
    useStableSort = isEnableStableSort(from, to);
    if (useStableSort) {
      doClosureIfStableSortEnabled();
    }
    sort(from, to, 0, 0);
  }

  @Override
  protected void reorder(int from, int to, int[] startOffsets, int[] endOffsets, int k) {
    if (useStableSort) {
      stableReorder(from, to, startOffsets, endOffsets, k);
    } else {
      super.reorder(from, to, startOffsets, endOffsets, k);
    }
  }

  /**
   * Reorder elements in stable way, since Dutch sort does not guarantee ordering for same values.
   *
   * <p>When this method returns, startOffsets and endOffsets are equal.
   */
  protected void stableReorder(int from, int to, int[] startOffsets, int[] endOffsets, int k) {
    int[] assignPos = ArrayUtil.copyOfSubArray(startOffsets, 0, startOffsets.length);
    for (int i = 0; i < HISTOGRAM_SIZE; ++i) {
      final int limit = endOffsets[i];
      for (int h1 = assignPos[i]; h1 < limit; h1++) {
        final int b = getBucket(from + h1, k);
        final int h2 = startOffsets[b]++;
        assign(from + h1, from + h2);
      }
    }
    finalizeAssign(from, to);
  }
}
