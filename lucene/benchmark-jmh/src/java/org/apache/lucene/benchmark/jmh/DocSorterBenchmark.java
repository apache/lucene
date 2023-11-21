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
package org.apache.lucene.benchmark.jmh;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BaseLSBRadixSorter;
import org.apache.lucene.util.IntroSorter;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.LongsRef;
import org.apache.lucene.util.MSBRadixSorter;
import org.apache.lucene.util.Sorter;
import org.apache.lucene.util.TimSorter;
import org.apache.lucene.util.packed.PackedInts;
import org.openjdk.jmh.annotations.*;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
// first iteration is complete garbage, so make sure we really warmup
@Warmup(iterations = 3, time = 1)
// real iterations. not useful to spend tons of time here, better to fork more
@Measurement(iterations = 5, time = 1)
// engage some noise reduction
@Fork(
    value = 1,
    jvmArgsAppend = {"-Xmx2g", "-Xms2g", "-XX:+AlwaysPreTouch"})
public class DocSorterBenchmark {

  private DocOffsetTimSorter timSorter;
  private DocOffsetLSBRadixSorter lsbRadixSorter;
  private DocOffsetMSBRadixSorter msbRadixSorter;
  private DocOffsetSorter docOffsetSorter;

  @Param({"100000"})
  int size;

  @Param({"natural", "reverse", "random", "partial", "natural_exception", "reverse_exception"})
  String order;

  @Param({"24", "31"})
  int bit;

  static {
    DocSorterBenchmark docSorterBenchmark = new DocSorterBenchmark();
    docSorterBenchmark.size = 100000;
    docSorterBenchmark.bit = 31;
    for (String order :
        Arrays.asList(
            "natural", "reverse", "random", "partial", "natural_exception", "reverse_exception")) {
      docSorterBenchmark.order = order;
      docSorterBenchmark.init();
      docSorterBenchmark.msbSorter();
      docSorterBenchmark.lsbSorter();
      docSorterBenchmark.timSorter();
      docSorterBenchmark.offSorter();
      int size = docSorterBenchmark.size;
      if (!Arrays.equals(
          docSorterBenchmark.timSorter.docs,
          0,
          size,
          docSorterBenchmark.lsbRadixSorter.docs,
          0,
          size)) {
        throw new RuntimeException("lsb wrong");
      }
      if (!Arrays.equals(
          docSorterBenchmark.timSorter.docs,
          0,
          size,
          docSorterBenchmark.msbRadixSorter.docs,
          0,
          size)) {
        throw new RuntimeException("msb wrong");
      }
      if (!Arrays.equals(
          docSorterBenchmark.timSorter.docs,
          0,
          size,
          docSorterBenchmark.docOffsetSorter.docs,
          0,
          size)) {
        throw new RuntimeException("off wrong");
      }
      //      System.out.println("tmp slots usage for order " + order + " : ");
      //      System.out.println("tim sorter: " + docSorterBenchmark.timSorter.tmpDocs.length);
      //      System.out.println("lsb sorter: " +
      // docSorterBenchmark.lsbRadixSorter.destDocs.length);
      //      System.out.println("off sorter: " +
      // docSorterBenchmark.docOffsetSorter.tmpDocs.length);
    }
  }

  @Setup(Level.Invocation)
  public void init() {
    ThreadLocalRandom random = ThreadLocalRandom.current();

    int maxDoc = 1 << (bit - 1);
    assert PackedInts.bitsRequired(maxDoc) == bit;
    // random byte arrays for binary methods
    int[] doc = IntStream.generate(() -> random.nextInt(maxDoc)).distinct().limit(size).toArray();
    long[] off = LongStream.generate(() -> random.nextLong(Long.MAX_VALUE)).limit(size).toArray();

    switch (order) {
      case "natural" -> Arrays.sort(doc);
      case "reverse" -> {
        List<Integer> docs = Arrays.stream(doc).sorted().boxed().collect(Collectors.toList());
        Collections.reverse(docs);
        doc = docs.stream().mapToInt(i -> i).toArray();
      }
      case "partial" -> {
        int p = random.nextInt(2, 16);
        int pSize = size / p;
        for (int i = 0; i < p; i++) {
          Arrays.sort(doc, pSize * i, pSize * (i + 1));
        }
        Arrays.sort(doc, p * pSize, size);
      }
      case "natural_exception" -> {
        Arrays.sort(doc);
        for (int i = 0; i < 16; i++) {
          doc[random.nextInt(doc.length)] = random.nextInt(maxDoc);
        }
      }
      case "reverse_exception" -> {
        List<Integer> docs = Arrays.stream(doc).sorted().boxed().collect(Collectors.toList());
        Collections.reverse(docs);
        doc = docs.stream().mapToInt(i -> i).toArray();
        for (int i = 0; i < 16; i++) {
          doc[random.nextInt(doc.length)] = random.nextInt(maxDoc);
        }
      }
    }

    timSorter = new DocOffsetTimSorter(size);
    timSorter.reset(doc.clone(), off.clone());
    lsbRadixSorter = new DocOffsetLSBRadixSorter().reset(bit, doc.clone(), off.clone());
    msbRadixSorter = new DocOffsetMSBRadixSorter(bit, doc.clone(), off.clone());
    docOffsetSorter = new DocOffsetSorter(size);
    docOffsetSorter.reset(bit, doc.clone(), off.clone());
  }

  @Benchmark
  public void timSorter() {
    timSorter.sort(0, size);
  }

  @Benchmark
  public void lsbSorter() {
    lsbRadixSorter.sort(0, size);
  }

  //  @Benchmark
  public void msbSorter() {
    msbRadixSorter.sort(0, size);
  }

  @Benchmark
  public void offSorter() {
    docOffsetSorter.sort(0, size);
  }

  private static final class DocOffsetTimSorter extends TimSorter {

    private int[] docs;
    private long[] offsets;
    private int[] tmpDocs;
    private long[] tmpOffsets;

    public DocOffsetTimSorter(int numTempSlots) {
      super(numTempSlots);
      this.tmpDocs = IntsRef.EMPTY_INTS;
      this.tmpOffsets = LongsRef.EMPTY_LONGS;
    }

    public void reset(int[] docs, long[] offsets) {
      this.docs = docs;
      this.offsets = offsets;
    }

    @Override
    protected int compare(int i, int j) {
      return docs[i] - docs[j];
    }

    @Override
    protected void swap(int i, int j) {
      int tmpDoc = docs[i];
      docs[i] = docs[j];
      docs[j] = tmpDoc;

      long tmpOffset = offsets[i];
      offsets[i] = offsets[j];
      offsets[j] = tmpOffset;
    }

    @Override
    protected void copy(int src, int dest) {
      docs[dest] = docs[src];
      offsets[dest] = offsets[src];
    }

    @Override
    protected void save(int i, int len) {
      if (tmpDocs.length < len) {
        tmpDocs = new int[ArrayUtil.oversize(len, Integer.BYTES)];
        tmpOffsets = new long[tmpDocs.length];
      }
      System.arraycopy(docs, i, tmpDocs, 0, len);
      System.arraycopy(offsets, i, tmpOffsets, 0, len);
    }

    @Override
    protected void restore(int i, int j) {
      docs[j] = tmpDocs[i];
      offsets[j] = tmpOffsets[i];
    }

    @Override
    protected int compareSaved(int i, int j) {
      return tmpDocs[i] - docs[j];
    }
  }

  private static final class DocOffsetLSBRadixSorter extends BaseLSBRadixSorter {

    private int[] docs;
    private int[] srcDocs;
    private int[] destDocs;

    private long[] offsets;
    private long[] srcOffsets;
    private long[] destOffsets;

    public DocOffsetLSBRadixSorter() {
      super(-1);
      destDocs = IntsRef.EMPTY_INTS;
      destOffsets = LongsRef.EMPTY_LONGS;
    }

    public DocOffsetLSBRadixSorter reset(int bits, int[] docs, long[] offsets) {
      this.bits = bits;
      this.docs = docs;
      this.srcDocs = docs;
      this.destDocs = ArrayUtil.growNoCopy(destDocs, docs.length);
      this.offsets = offsets;
      this.srcOffsets = offsets;
      this.destOffsets = ArrayUtil.growNoCopy(destOffsets, offsets.length);
      return this;
    }

    @Override
    protected void switchBuffer() {
      int[] tmp = srcDocs;
      srcDocs = destDocs;
      destDocs = tmp;

      long[] tmpOff = srcOffsets;
      srcOffsets = destOffsets;
      destOffsets = tmpOff;
    }

    @Override
    protected void buildHistogram(int from, int to, int[] histogram, int shift) {
      for (int i = from; i < to; ++i) {
        final int b = (srcDocs[i] >>> shift) & 0xFF;
        histogram[b] += 1;
      }
    }

    @Override
    protected void reorder(int from, int to, int[] histogram, int shift) {
      for (int i = from; i < to; ++i) {
        final int b = (srcDocs[i] >>> shift) & 0xFF;
        int j = from + histogram[b]++;
        destDocs[j] = srcDocs[i];
        destOffsets[j] = srcOffsets[i];
      }
    }

    @Override
    protected void restore(int from, int to) {
      if (srcDocs != docs) {
        assert srcOffsets != offsets;
        System.arraycopy(srcDocs, from, docs, from, to - from);
        System.arraycopy(srcOffsets, from, offsets, from, to - from);
        this.destDocs = srcDocs;
        this.destOffsets = srcOffsets;
      }
    }

    @Override
    protected int compare(int i, int j) {
      return srcDocs[i] - srcDocs[j];
    }

    @Override
    protected void swap(int i, int j) {
      int tmp = srcDocs[i];
      srcDocs[i] = srcDocs[j];
      srcDocs[j] = tmp;

      long tmpOffset = srcOffsets[i];
      srcOffsets[i] = srcOffsets[j];
      srcOffsets[j] = tmpOffset;
    }
  }

  private static final class DocOffsetMSBRadixSorter extends MSBRadixSorter {

    private final int[] docs;
    private final long[] offsets;
    private int k;

    public DocOffsetMSBRadixSorter(int bits, int[] docs, long[] offsets) {
      super((bits + 7) / 8);
      this.docs = docs;
      this.offsets = offsets;
    }

    @Override
    protected int byteAt(int i, int k) {
      return byteShift(i, shift(k));
    }

    private int shift(int k) {
      return (maxLength - k - 1) << 3;
    }

    private int byteShift(int i, int shift) {
      return ((docs[i] >>> shift) & 0xFF);
    }

    private int bucketShift(int i, int shift) {
      return byteShift(i, shift) + 1;
    }

    @Override
    protected void buildHistogram(
        int prefixCommonBucket, int prefixCommonLen, int from, int to, int k, int[] histogram) {
      this.k = k;
      final int shift = shift(k);
      histogram[prefixCommonBucket] = prefixCommonLen;
      for (int i = from; i < to; ++i) {
        int b = bucketShift(i, shift);
        histogram[b]++;
      }
    }

    @Override
    protected Sorter getFallbackSorter(int k) {
      return new IntroSorter() {
        int pivot;

        @Override
        protected void setPivot(int i) {
          this.pivot = docs[i];
        }

        @Override
        protected int comparePivot(int j) {
          return pivot - docs[j];
        }

        @Override
        protected int compare(int i, int j) {
          return docs[i] - docs[j];
        }

        @Override
        protected void swap(int i, int j) {
          DocOffsetMSBRadixSorter.this.swap(i, j);
        }
      };
    }

    @Override
    protected void reorder(int from, int to, int[] startOffsets, int[] endOffsets, int k) {
      assert this.k == k;
      final int shift = shift(k);
      for (int i = 0; i < HISTOGRAM_SIZE; ++i) {
        final int limit = endOffsets[i];
        for (int h1 = startOffsets[i]; h1 < limit; h1 = startOffsets[i]) {
          final int b = bucketShift(from + h1, shift);
          final int h2 = startOffsets[b]++;
          swap(from + h1, from + h2);
        }
      }
    }

    @Override
    protected void swap(int i, int j) {
      int tmp = docs[i];
      docs[i] = docs[j];
      docs[j] = tmp;

      long tmpOffset = offsets[i];
      offsets[i] = offsets[j];
      offsets[j] = tmpOffset;
    }
  }

  private static final class DocOffsetSorter extends TimSorter {

    private int bits;
    private int[] docs;
    private long[] offsets;
    private int[] tmpDocs;
    private long[] tmpOffsets;

    public DocOffsetSorter(int numTempSlots) {
      super(numTempSlots);
      this.tmpDocs = IntsRef.EMPTY_INTS;
      this.tmpOffsets = LongsRef.EMPTY_LONGS;
    }

    public void reset(int bits, int[] docs, long[] offsets) {
      this.bits = bits;
      this.docs = docs;
      this.offsets = offsets;
    }

    @Override
    protected int compare(int i, int j) {
      return docs[i] - docs[j];
    }

    @Override
    protected void doSort(int from, int to, int sortedTo) {
      if (to - from <= 64) {
        binarySort(from, to, sortedTo);
      } else {
        growTmp(to - from);
        new BaseLSBRadixSorter(bits) {

          int srcOff = 0;
          int[] srcDocs = DocOffsetSorter.this.docs;
          long[] srcOffsets = DocOffsetSorter.this.offsets;
          int destOff = from;
          int[] destDocs = DocOffsetSorter.this.tmpDocs;
          long[] destOffsets = DocOffsetSorter.this.tmpOffsets;

          @Override
          protected int compare(int i, int j) {
            return DocOffsetSorter.this.compare(i, j);
          }

          @Override
          protected void swap(int i, int j) {
            DocOffsetSorter.this.swap(i, j);
          }

          @Override
          protected void buildHistogram(int from, int to, int[] histogram, int shift) {
            final int srcFrom = from - srcOff;
            final int srcTo = to - srcOff;
            for (int i = srcFrom; i < srcTo; ++i) {
              final int b = (srcDocs[i] >>> shift) & 0xFF;
              histogram[b] += 1;
            }
          }

          @Override
          protected void reorder(int from, int to, int[] histogram, int shift) {
            final int srcFrom = from - srcOff;
            final int srcTo = to - srcOff;
            final int destFrom = from - destOff;
            for (int i = srcFrom; i < srcTo; ++i) {
              int srcDoc = srcDocs[i];
              final int b = (srcDoc >>> shift) & 0xFF;
              int j = destFrom + histogram[b]++;
              destDocs[j] = srcDoc;
              destOffsets[j] = srcDoc;
            }
          }

          @Override
          protected void switchBuffer() {
            int[] tmp = srcDocs;
            srcDocs = destDocs;
            destDocs = tmp;

            long[] tmpOff = srcOffsets;
            srcOffsets = destOffsets;
            destOffsets = tmpOff;

            int tmpOffset = srcOff;
            srcOff = destOff;
            destOff = tmpOffset;
          }

          @Override
          protected void restore(int from, int to) {
            if (srcDocs != docs) {
              assert srcOffsets != offsets;
              System.arraycopy(srcDocs, from - srcOff, docs, destOff + from, to - from);
              System.arraycopy(srcOffsets, from - srcOff, offsets, destOff + from, to - from);
            }
          }
        }.sort(from, to);
      }
    }

    @Override
    protected int minRunLength(int length) {
      return Math.min(Math.max(tmpDocs.length, length / 8), maxTempSlots);
    }

    @Override
    protected void swap(int i, int j) {
      int tmpDoc = docs[i];
      docs[i] = docs[j];
      docs[j] = tmpDoc;

      long tmpOffset = offsets[i];
      offsets[i] = offsets[j];
      offsets[j] = tmpOffset;
    }

    @Override
    protected void copy(int src, int dest) {
      docs[dest] = docs[src];
      offsets[dest] = offsets[src];
    }

    @Override
    protected void save(int i, int len) {
      growTmp(len);
      System.arraycopy(docs, i, tmpDocs, 0, len);
      System.arraycopy(offsets, i, tmpOffsets, 0, len);
    }

    private void growTmp(int len) {
      if (tmpDocs.length < len) {
        tmpDocs = new int[ArrayUtil.oversize(len, Integer.BYTES)];
        tmpOffsets = new long[tmpDocs.length];
      }
    }

    @Override
    protected void restore(int i, int j) {
      docs[j] = tmpDocs[i];
      offsets[j] = tmpOffsets[i];
    }

    @Override
    protected int compareSaved(int i, int j) {
      return tmpDocs[i] - docs[j];
    }
  }
}
