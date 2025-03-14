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
package org.apache.lucene.codecs.lucene90;

import java.io.IOException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.LongValues;

/** Reads long values used in {@link SortedNumericDocValuesProducer} */
abstract class LongValuesProducer {

  public abstract LongValues getLongValues(IndexInput data, int maxDoc, boolean merging)
      throws IOException;

  public static LongValuesProducer create(Lucene90DocValuesProducer.NumericEntry entry) {
    if (entry.bitsPerValue == 0) {
      return new SingleLongValuesProducer(entry.minValue);
    } else {
      if (entry.blockShift >= 0) {
        return new VaryingVPLongValuesProducer(entry);
      } else {
        if (entry.table != null) {
          return new TableLongValuesProducer(entry);
        } else if (entry.gcd != 1) {
          return new GcdLongValuesProducer(entry);
        } else if (entry.minValue != 0) {
          return new MinValueLongValuesProducer(entry);
        } else {
          return new PlainLongValuesProducer(entry);
        }
      }
    }
  }

  private static class SingleLongValuesProducer extends LongValuesProducer {

    private final long value;

    private SingleLongValuesProducer(long value) {
      this.value = value;
    }

    @Override
    public LongValues getLongValues(IndexInput data, int maxDoc, boolean merging) {
      return new LongValues() {
        @Override
        public long get(long index) {
          return value;
        }
      };
    }
  }

  private abstract static class BaseLongValuesProducer extends LongValuesProducer {

    protected final long valuesOffset;
    private final long valuesLength;

    protected BaseLongValuesProducer(long valuesOffset, long valuesLength) {
      this.valuesOffset = valuesOffset;
      this.valuesLength = valuesLength;
    }

    protected RandomAccessInput slice(IndexInput data) throws IOException {
      final RandomAccessInput slice = data.randomAccessSlice(valuesOffset, valuesLength);
      // Prefetch the first page of data. Following pages are expected to get prefetched through
      // read-ahead.
      if (slice.length() > 0) {
        slice.prefetch(0, 1);
      }
      return slice;
    }
  }

  private static class VaryingVPLongValuesProducer extends BaseLongValuesProducer {

    private final long valueJumpTableOffset;
    private final int blockShift;
    private final long gcd;
    private final long numValues;

    private VaryingVPLongValuesProducer(Lucene90DocValuesProducer.NumericEntry entry) {
      super(entry.valuesOffset, entry.valuesLength);
      this.valueJumpTableOffset = entry.valueJumpTableOffset;
      this.blockShift = entry.blockShift;
      this.gcd = entry.gcd;
      this.numValues = entry.numValues;
    }

    @Override
    public LongValues getLongValues(IndexInput data, int maxDoc, boolean merging)
        throws IOException {
      final RandomAccessInput slice = slice(data);
      return new LongValues() {
        final NumericDocValuesProducer.VaryingBPVReader vBPVReader =
            new NumericDocValuesProducer.VaryingBPVReader(
                slice,
                data,
                merging,
                valueJumpTableOffset,
                blockShift,
                gcd,
                valuesOffset,
                numValues);

        @Override
        public long get(long index) {
          try {
            return vBPVReader.getLongValue(index);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      };
    }
  }

  private static class TableLongValuesProducer extends BaseLongValuesProducer {

    private final long[] table;
    private final byte bitsPerValue;
    private final long numValues;

    private TableLongValuesProducer(Lucene90DocValuesProducer.NumericEntry entry) {
      super(entry.valuesOffset, entry.valuesLength);
      this.table = entry.table;
      this.bitsPerValue = entry.bitsPerValue;
      this.numValues = entry.numValues;
    }

    @Override
    public LongValues getLongValues(IndexInput data, int maxDoc, boolean merging)
        throws IOException {
      final RandomAccessInput slice = slice(data);
      final LongValues values =
          NumericDocValuesProducer.getDirectReaderInstance(
              slice, bitsPerValue, 0L, numValues, merging);
      final long[] table = this.table;
      return new LongValues() {
        @Override
        public long get(long index) {
          return table[(int) values.get(index)];
        }
      };
    }
  }

  private static class GcdLongValuesProducer extends BaseLongValuesProducer {

    private final byte bitsPerValue;
    private final long numValues;
    private final long gcd;
    private final long value;

    private GcdLongValuesProducer(Lucene90DocValuesProducer.NumericEntry entry) {
      super(entry.valuesOffset, entry.valuesLength);
      this.bitsPerValue = entry.bitsPerValue;
      this.numValues = entry.numValues;
      this.gcd = entry.gcd;
      this.value = entry.minValue;
    }

    @Override
    public LongValues getLongValues(IndexInput data, int maxDoc, boolean merging)
        throws IOException {
      final RandomAccessInput slice = slice(data);
      final LongValues values =
          NumericDocValuesProducer.getDirectReaderInstance(
              slice, bitsPerValue, 0L, numValues, merging);
      final long gcd = this.gcd;
      final long value = this.value;
      return new LongValues() {
        @Override
        public long get(long index) {
          return values.get(index) * gcd + value;
        }
      };
    }
  }

  private static class MinValueLongValuesProducer extends BaseLongValuesProducer {

    private final byte bitsPerValue;
    private final long numValues;
    private final long minValue;

    private MinValueLongValuesProducer(Lucene90DocValuesProducer.NumericEntry entry) {
      super(entry.valuesOffset, entry.valuesLength);
      this.bitsPerValue = entry.bitsPerValue;
      this.numValues = entry.numValues;
      this.minValue = entry.minValue;
    }

    @Override
    public LongValues getLongValues(IndexInput data, int maxDoc, boolean merging)
        throws IOException {
      final RandomAccessInput slice = slice(data);
      final LongValues values =
          NumericDocValuesProducer.getDirectReaderInstance(
              slice, bitsPerValue, 0L, numValues, merging);
      final long minValue = this.minValue;
      return new LongValues() {
        @Override
        public long get(long index) {
          return values.get(index) + minValue;
        }
      };
    }
  }

  private static class PlainLongValuesProducer extends BaseLongValuesProducer {

    private final byte bitsPerValue;
    private final long numValues;

    private PlainLongValuesProducer(Lucene90DocValuesProducer.NumericEntry entry) {
      super(entry.valuesOffset, entry.valuesLength);
      this.bitsPerValue = entry.bitsPerValue;
      this.numValues = entry.numValues;
    }

    @Override
    public LongValues getLongValues(IndexInput data, int maxDoc, boolean merging)
        throws IOException {
      final RandomAccessInput slice = slice(data);
      return NumericDocValuesProducer.getDirectReaderInstance(
          slice, bitsPerValue, 0L, numValues, merging);
    }
  }
}
