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

import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.IntStream;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestStringSorter extends LuceneTestCase {

  private void test(BytesRef[] refs, int len) {
    test(ArrayUtil.copyOfSubArray(refs, 0, len), len, BytesRefComparator.NATURAL);
    test(ArrayUtil.copyOfSubArray(refs, 0, len), len, Comparator.naturalOrder());
    testStable(ArrayUtil.copyOfSubArray(refs, 0, len), len, BytesRefComparator.NATURAL);
    testStable(ArrayUtil.copyOfSubArray(refs, 0, len), len, Comparator.naturalOrder());
  }

  private void test(BytesRef[] refs, int len, Comparator<BytesRef> comparator) {
    BytesRef[] expected = ArrayUtil.copyOfSubArray(refs, 0, len);
    Arrays.sort(expected);

    new StringSorter(comparator) {

      @Override
      protected void get(BytesRefBuilder builder, BytesRef result, int i) {
        BytesRef ref = refs[i];
        result.offset = ref.offset;
        result.length = ref.length;
        result.bytes = ref.bytes;
      }

      @Override
      protected void swap(int i, int j) {
        BytesRef tmp = refs[i];
        refs[i] = refs[j];
        refs[j] = tmp;
      }
    }.sort(0, len);
    BytesRef[] actual = ArrayUtil.copyOfSubArray(refs, 0, len);
    assertArrayEquals(expected, actual);
  }

  private void testStable(BytesRef[] refs, int len, Comparator<BytesRef> comparator) {
    BytesRef[] expected = ArrayUtil.copyOfSubArray(refs, 0, len);
    Arrays.sort(expected);

    int[] ord = new int[len];
    IntStream.range(0, len).forEach(i -> ord[i] = i);
    new StableStringSorter(comparator) {

      final int[] tmp = new int[len];

      @Override
      protected void save(int i, int j) {
        tmp[j] = ord[i];
      }

      @Override
      protected void restore(int i, int j) {
        System.arraycopy(tmp, i, ord, i, j - i);
      }

      @Override
      protected void get(BytesRefBuilder builder, BytesRef result, int i) {
        BytesRef ref = refs[ord[i]];
        result.offset = ref.offset;
        result.length = ref.length;
        result.bytes = ref.bytes;
      }

      @Override
      protected void swap(int i, int j) {
        int tmp = ord[i];
        ord[i] = ord[j];
        ord[j] = tmp;
      }
    }.sort(0, len);

    for (int i = 0; i < len; i++) {
      assertEquals(expected[i], refs[ord[i]]);
      if (i > 0 && expected[i].equals(expected[i - 1])) {
        assertTrue("not stable: " + ord[i] + " <= " + ord[i - 1], ord[i] > ord[i - 1]);
      }
    }
  }

  public void testEmpty() {
    test(new BytesRef[random().nextInt(5)], 0);
  }

  public void testOneValue() {
    BytesRef bytes = new BytesRef(TestUtil.randomSimpleString(random()));
    test(new BytesRef[] {bytes}, 1);
  }

  public void testTwoValues() {
    BytesRef bytes1 = new BytesRef(TestUtil.randomSimpleString(random()));
    BytesRef bytes2 = new BytesRef(TestUtil.randomSimpleString(random()));
    test(new BytesRef[] {bytes1, bytes2}, 2);
  }

  private void testRandom(int commonPrefixLen, int maxLen) {
    byte[] commonPrefix = new byte[commonPrefixLen];
    random().nextBytes(commonPrefix);
    final int len = random().nextInt(100000);
    BytesRef[] bytes = new BytesRef[len + random().nextInt(50)];
    for (int i = 0; i < len; ++i) {
      byte[] b = new byte[commonPrefixLen + random().nextInt(maxLen)];
      random().nextBytes(b);
      System.arraycopy(commonPrefix, 0, b, 0, commonPrefixLen);
      bytes[i] = new BytesRef(b);
    }
    test(bytes, len);
  }

  public void testRandom() {
    int numIters = atLeast(3);
    for (int iter = 0; iter < numIters; ++iter) {
      testRandom(0, 10);
    }
  }

  public void testRandomWithLotsOfDuplicates() {
    int numIters = atLeast(3);
    for (int iter = 0; iter < numIters; ++iter) {
      testRandom(0, 2);
    }
  }

  public void testRandomWithSharedPrefix() {
    int numIters = atLeast(3);
    for (int iter = 0; iter < numIters; ++iter) {
      testRandom(TestUtil.nextInt(random(), 1, 30), 10);
    }
  }

  public void testRandomWithSharedPrefixAndLotsOfDuplicates() {
    int numIters = atLeast(3);
    for (int iter = 0; iter < numIters; ++iter) {
      testRandom(TestUtil.nextInt(random(), 1, 30), 2);
    }
  }
}
