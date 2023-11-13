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
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BaseLSBRadixSorter;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.LongsRef;
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
  private DocOffsetRadixSorter radixSorter;

  @Param({"100000"})
  int size;

  @Param({"natural", "reverse", "random", "partial"})
  String order;

  @Param({"24", "31"})
  int bit;

  @Setup(Level.Invocation)
  public void init() {
    ThreadLocalRandom random = ThreadLocalRandom.current();

    int maxDoc = 1 << (bit - 1);
    assert PackedInts.bitsRequired(maxDoc) == bit;
    // random byte arrays for binary methods
    int[] doc = new int[size];
    long[] off = new long[size];
    for (int i = 0; i < size; ++i) {
      doc[i] = random.nextInt(maxDoc);
      off[i] = random.nextLong(Long.MAX_VALUE);
    }

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
    }

    timSorter = new DocOffsetTimSorter(size / 64);
    timSorter.reset(doc.clone(), off.clone());
    radixSorter = new DocOffsetRadixSorter(bit, doc.clone(), off.clone());
  }

  @Benchmark
  public void timSorter() {
    timSorter.sort(0, size);
  }

  @Benchmark
  public void radixSorter() {
    radixSorter.sort(0, size);
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

  private static final class DocOffsetRadixSorter extends BaseLSBRadixSorter {

    private int[] docs;
    private int[] srcDocs;
    private int[] destDocs;

    private long[] offsets;
    private long[] srcOffsets;
    private long[] destOffsets;

    public DocOffsetRadixSorter(int bits, int[] docs, long[] offsets) {
      super(bits);
      destDocs = IntsRef.EMPTY_INTS;
      destOffsets = LongsRef.EMPTY_LONGS;
      reset(bits, docs, offsets);
    }

    public DocOffsetRadixSorter reset(int bits, int[] docs, long[] offsets) {
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
    protected int bucket(int i, int shift) {
      return (srcDocs[i] >>> shift) & 0xFF;
    }

    @Override
    protected void save(int i, int j) {
      destDocs[j] = srcDocs[i];
      destOffsets[j] = srcOffsets[i];
    }

    @Override
    protected void restore(int from, int to) {
      if (srcDocs != docs) {
        System.arraycopy(srcDocs, from, docs, from, to - from);
        assert srcOffsets != offsets;
        System.arraycopy(srcOffsets, from, offsets, from, to - from);
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
}
