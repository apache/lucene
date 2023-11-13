package org.apache.lucene.benchmark.jmh;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.UnsignedIntLSBRadixSorter;
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
public class LSBSorterBenchmark {

  final LSBRadixSorter lsb = new LSBRadixSorter();
  final UnsignedIntLSBRadixSorter lsbNew = new UnsignedIntLSBRadixSorter();

  int[] doc;

  @Param({"100000"})
  int size;

  @Param({"natural", "reverse", "random"})
  String order;

  @Param({"24", "31"})
  int bit;

  @Setup(Level.Invocation)
  public void init() {
    ThreadLocalRandom random = ThreadLocalRandom.current();

    int maxDoc = 1 << (bit - 1);
    assert PackedInts.bitsRequired(maxDoc) == bit;
    // random byte arrays for binary methods
    doc = new int[size];
    for (int i = 0; i < size; ++i) {
      doc[i] = random.nextInt(maxDoc);
    }

    switch (order) {
      case "natural" -> Arrays.sort(doc);
      case "reverse" -> {
        Arrays.sort(doc);
        List<Integer> docs = new ArrayList<>(Arrays.stream(doc).boxed().toList());
        Collections.reverse(docs);
        doc = docs.stream().mapToInt(i -> i).toArray();
      }
    }
  }

  @Benchmark
  public void oldLSB() {
    lsb.sort(bit, doc, size);
  }

  @Benchmark
  public void newLSB() {
    lsbNew.reset(bit, doc).sort(0, size);
  }

  private static final class LSBRadixSorter {

    private static final int INSERTION_SORT_THRESHOLD = 30;
    private static final int HISTOGRAM_SIZE = 256;

    private final int[] histogram = new int[HISTOGRAM_SIZE];
    private int[] buffer = new int[0];

    private static void buildHistogram(int[] array, int len, int[] histogram, int shift) {
      for (int i = 0; i < len; ++i) {
        final int b = (array[i] >>> shift) & 0xFF;
        histogram[b] += 1;
      }
    }

    private static void sumHistogram(int[] histogram) {
      int accum = 0;
      for (int i = 0; i < HISTOGRAM_SIZE; ++i) {
        final int count = histogram[i];
        histogram[i] = accum;
        accum += count;
      }
    }

    private static void reorder(int[] array, int len, int[] histogram, int shift, int[] dest) {
      for (int i = 0; i < len; ++i) {
        final int v = array[i];
        final int b = (v >>> shift) & 0xFF;
        dest[histogram[b]++] = v;
      }
    }

    private static boolean sort(int[] array, int len, int[] histogram, int shift, int[] dest) {
      Arrays.fill(histogram, 0);
      buildHistogram(array, len, histogram, shift);
      if (histogram[0] == len) {
        return false;
      }
      sumHistogram(histogram);
      reorder(array, len, histogram, shift, dest);
      return true;
    }

    private static void insertionSort(int[] array, int off, int len) {
      for (int i = off + 1, end = off + len; i < end; ++i) {
        for (int j = i; j > off; --j) {
          if (array[j - 1] > array[j]) {
            int tmp = array[j - 1];
            array[j - 1] = array[j];
            array[j] = tmp;
          } else {
            break;
          }
        }
      }
    }

    /**
     * Sort {@code array[0:len]} in place.
     *
     * @param numBits how many bits are required to store any of the values in {@code array[0:len]}.
     *     Pass {@code 32} if unknown.
     */
    public void sort(int numBits, final int[] array, int len) {
      if (len < INSERTION_SORT_THRESHOLD) {
        insertionSort(array, 0, len);
        return;
      }

      buffer = ArrayUtil.grow(buffer, len);

      int[] arr = array;

      int[] buf = buffer;

      for (int shift = 0; shift < numBits; shift += 8) {
        if (sort(arr, len, histogram, shift, buf)) {
          // swap arrays
          int[] tmp = arr;
          arr = buf;
          buf = tmp;
        }
      }

      if (array == buf) {
        System.arraycopy(arr, 0, array, 0, len);
      }
    }
  }
}
