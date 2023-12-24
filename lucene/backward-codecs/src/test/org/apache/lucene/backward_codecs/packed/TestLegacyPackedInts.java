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
import java.io.IOException;
import org.apache.lucene.backward_codecs.store.EndiannessReverserUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.RamUsageTester;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedInts.Reader;

public class TestLegacyPackedInts extends LuceneTestCase {

  public void testPackedInts() throws IOException {
    int num = atLeast(3);
    for (int iter = 0; iter < num; iter++) {
      for (int nbits = 1; nbits <= 64; nbits++) {
        final long maxValue = PackedInts.maxValue(nbits);
        final int valueCount = TestUtil.nextInt(random(), 1, 600);
        final Directory d = newDirectory();

        IndexOutput out = EndiannessReverserUtil.createOutput(d, "out.bin", newIOContext(random()));
        final int mem = random().nextInt(2 * PackedInts.DEFAULT_BUFFER_SIZE);
        PackedInts.Writer w =
            PackedInts.getWriterNoHeader(out, PackedInts.Format.PACKED, valueCount, nbits, mem);
        final long startFp = out.getFilePointer();

        final int actualValueCount =
            random().nextBoolean() ? valueCount : TestUtil.nextInt(random(), 0, valueCount);
        final long[] values = new long[valueCount];
        for (int i = 0; i < actualValueCount; i++) {
          if (nbits == 64) {
            values[i] = random().nextLong();
          } else {
            values[i] = TestUtil.nextLong(random(), 0, maxValue);
          }
          w.add(values[i]);
        }
        w.finish();
        final long fp = out.getFilePointer();
        out.close();

        // ensure that finish() added the (valueCount-actualValueCount) missing values
        final long bytes =
            PackedInts.Format.PACKED.byteCount(
                PackedInts.VERSION_CURRENT, valueCount, w.bitsPerValue());
        assertEquals(bytes, fp - startFp);

        { // test reader
          IndexInput in = EndiannessReverserUtil.openInput(d, "out.bin", newIOContext(random()));
          PackedInts.Reader r =
              LegacyPackedInts.getReaderNoHeader(
                  in, PackedInts.Format.PACKED, PackedInts.VERSION_CURRENT, valueCount, nbits);
          assertEquals(fp, in.getFilePointer());
          for (int i = 0; i < valueCount; i++) {
            assertEquals(
                "index="
                    + i
                    + " valueCount="
                    + valueCount
                    + " nbits="
                    + nbits
                    + " for "
                    + r.getClass().getSimpleName(),
                values[i],
                r.get(i));
          }
          in.close();
          final long expectedBytesUsed = RamUsageTester.ramUsed(r);
          final long computedBytesUsed = r.ramBytesUsed();
          assertEquals(
              r.getClass() + " expected " + expectedBytesUsed + ", got: " + computedBytesUsed,
              expectedBytesUsed,
              computedBytesUsed);
        }
        d.close();
      }
    }
  }

  public void testEndPointer() throws IOException {
    final Directory dir = newDirectory();
    final int valueCount = RandomNumbers.randomIntBetween(random(), 1, 1000);
    final IndexOutput out =
        EndiannessReverserUtil.createOutput(dir, "tests.bin", newIOContext(random()));
    for (int i = 0; i < valueCount; ++i) {
      out.writeLong(0);
    }
    out.close();
    final IndexInput in =
        EndiannessReverserUtil.openInput(dir, "tests.bin", newIOContext(random()));
    for (int version = PackedInts.VERSION_START; version <= PackedInts.VERSION_CURRENT; ++version) {
      for (int bpv = 1; bpv <= 64; ++bpv) {
        for (PackedInts.Format format : PackedInts.Format.values()) {
          if (!format.isSupported(bpv)) {
            continue;
          }
          final long byteCount = format.byteCount(version, valueCount, bpv);
          String msg =
              "format="
                  + format
                  + ",version="
                  + version
                  + ",valueCount="
                  + valueCount
                  + ",bpv="
                  + bpv;
          // test reader
          in.seek(0L);
          LegacyPackedInts.getReaderNoHeader(in, format, version, valueCount, bpv);
          assertEquals(msg, byteCount, in.getFilePointer());
        }
      }
    }
    in.close();
    dir.close();
  }

  public void testSingleValue() throws Exception {
    for (int bitsPerValue = 1; bitsPerValue <= 64; ++bitsPerValue) {
      Directory dir = newDirectory();
      IndexOutput out = EndiannessReverserUtil.createOutput(dir, "out", newIOContext(random()));
      PackedInts.Writer w =
          PackedInts.getWriterNoHeader(
              out, PackedInts.Format.PACKED, 1, bitsPerValue, PackedInts.DEFAULT_BUFFER_SIZE);
      long value = 17L & PackedInts.maxValue(bitsPerValue);
      w.add(value);
      w.finish();
      final long end = out.getFilePointer();
      out.close();

      IndexInput in = EndiannessReverserUtil.openInput(dir, "out", newIOContext(random()));
      Reader reader =
          LegacyPackedInts.getReaderNoHeader(
              in, PackedInts.Format.PACKED, PackedInts.VERSION_CURRENT, 1, bitsPerValue);
      String msg = "Impl=" + w.getClass().getSimpleName() + ", bitsPerValue=" + bitsPerValue;
      assertEquals(msg, 1, reader.size());
      assertEquals(msg, value, reader.get(0));
      assertEquals(msg, end, in.getFilePointer());
      in.close();

      dir.close();
    }
  }
}
