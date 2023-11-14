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
 * A LSB Radix sorter for unsigned int values.
 *
 * @lucene.internal
 */
public final class UnsignedIntLSBRadixSorter extends BaseLSBRadixSorter {

  private int[] array;
  private int[] src;
  private int[] dest;

  /**
   * For scenes that need to use one sorter across several rounds of sort in favor of {@link
   * #reset}.
   */
  public UnsignedIntLSBRadixSorter() {
    this(-1, IntsRef.EMPTY_INTS);
  }

  public UnsignedIntLSBRadixSorter(int bits, int[] array) {
    super(bits);
    this.array = array;
    this.src = array;
    this.dest = new int[array.length];
  }

  public UnsignedIntLSBRadixSorter reset(int bits, int[] array) {
    this.bits = bits;
    this.array = array;
    this.src = array;
    this.dest = ArrayUtil.growNoCopy(dest, array.length);
    return this;
  }

  @Override
  protected void switchBuffer() {
    int[] tmp = src;
    src = dest;
    dest = tmp;
  }

  @Override
  protected int bucket(int i, int shift) {
    return (src[i] >>> shift) & 0xFF;
  }

  @Override
  protected void save(int i, int j) {
    dest[j] = src[i];
  }

  @Override
  protected void restore(int from, int to) {
    if (src != array) {
      System.arraycopy(src, from, array, from, to - from);
      dest = src;
    }
  }

  @Override
  protected int compare(int i, int j) {
    return array[i] - array[j];
  }

  @Override
  protected void swap(int i, int j) {
    int tmp = src[i];
    src[i] = src[j];
    src[j] = tmp;
  }
}
