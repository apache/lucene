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
package org.apache.lucene.backward_codecs.packed;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import java.util.Random;
import org.apache.lucene.backward_codecs.store.EndiannessReverserUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.PackedInts;

public class TestLegacyDirectPacked extends LuceneTestCase {

  /** simple encode/decode */
  public void testSimple() throws Exception {
    Directory dir = newDirectory();
    int bitsPerValue = LegacyDirectWriter.bitsRequired(2);
    IndexOutput output = EndiannessReverserUtil.createOutput(dir, "foo", IOContext.DEFAULT);
    LegacyDirectWriter writer = LegacyDirectWriter.getInstance(output, 5, bitsPerValue);
    writer.add(1);
    writer.add(0);
    writer.add(2);
    writer.add(1);
    writer.add(2);
    writer.finish();
    output.close();
    IndexInput input = EndiannessReverserUtil.openInput(dir, "foo", IOContext.DEFAULT);
    LongValues reader =
        LegacyDirectReader.getInstance(input.randomAccessSlice(0, input.length()), bitsPerValue, 0);
    assertEquals(1, reader.get(0));
    assertEquals(0, reader.get(1));
    assertEquals(2, reader.get(2));
    assertEquals(1, reader.get(3));
    assertEquals(2, reader.get(4));
    input.close();
    dir.close();
  }

  /** test exception is delivered if you add the wrong number of values */
  public void testNotEnoughValues() throws Exception {
    Directory dir = newDirectory();
    int bitsPerValue = LegacyDirectWriter.bitsRequired(2);
    IndexOutput output = EndiannessReverserUtil.createOutput(dir, "foo", IOContext.DEFAULT);
    LegacyDirectWriter writer = LegacyDirectWriter.getInstance(output, 5, bitsPerValue);
    writer.add(1);
    writer.add(0);
    writer.add(2);
    writer.add(1);
    IllegalStateException expected =
        expectThrows(
            IllegalStateException.class,
            () -> {
              writer.finish();
            });
    assertTrue(expected.getMessage().startsWith("Wrong number of values added"));

    output.close();
    dir.close();
  }

  public void testRandom() throws Exception {
    Directory dir = newDirectory();
    for (int bpv = 1; bpv <= 64; bpv++) {
      doTestBpv(dir, bpv, 0);
    }
    dir.close();
  }

  public void testRandomWithOffset() throws Exception {
    Directory dir = newDirectory();
    final int offset = TestUtil.nextInt(random(), 1, 100);
    for (int bpv = 1; bpv <= 64; bpv++) {
      doTestBpv(dir, bpv, offset);
    }
    dir.close();
  }

  private void doTestBpv(Directory directory, int bpv, long offset) throws Exception {
    Random random = random();
    int numIters = TEST_NIGHTLY ? 100 : 10;
    for (int i = 0; i < numIters; i++) {
      long[] original = randomLongs(random, bpv);
      int bitsRequired = bpv == 64 ? 64 : LegacyDirectWriter.bitsRequired(1L << (bpv - 1));
      String name = "bpv" + bpv + "_" + i;
      IndexOutput output = EndiannessReverserUtil.createOutput(directory, name, IOContext.DEFAULT);
      for (long j = 0; j < offset; ++j) {
        output.writeByte((byte) random.nextInt());
      }
      LegacyDirectWriter writer =
          LegacyDirectWriter.getInstance(output, original.length, bitsRequired);
      for (int j = 0; j < original.length; j++) {
        writer.add(original[j]);
      }
      writer.finish();
      output.close();
      IndexInput input = EndiannessReverserUtil.openInput(directory, name, IOContext.DEFAULT);
      LongValues reader =
          LegacyDirectReader.getInstance(
              input.randomAccessSlice(0, input.length()), bitsRequired, offset);
      for (int j = 0; j < original.length; j++) {
        assertEquals("bpv=" + bpv, original[j], reader.get(j));
      }
      input.close();
    }
  }

  private long[] randomLongs(Random random, int bpv) {
    int amount = random.nextInt(5000);
    long[] longs = new long[amount];
    long max = PackedInts.maxValue(bpv);
    for (int i = 0; i < longs.length; i++) {
      longs[i] = RandomNumbers.randomLongBetween(random, 0, max);
    }
    return longs;
  }
}
