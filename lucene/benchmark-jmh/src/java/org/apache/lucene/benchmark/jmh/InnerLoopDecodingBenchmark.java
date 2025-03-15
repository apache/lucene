package org.apache.lucene.benchmark.jmh;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.bkd.BKDConfig;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/** NO COMMIT: remove before merge */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@Fork(value = 1)
public class InnerLoopDecodingBenchmark {

  private static final int SIZE = BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE;

  private Directory dir;
  private IndexInput in;
  private int[] docs;
  private int[] scratch;

  @Setup(Level.Trial)
  public void setupTrial() throws IOException {
    Path path = Files.createTempDirectory("innerloop");
    dir = MMapDirectory.open(path);
    try (IndexOutput out = dir.createOutput("docs", IOContext.DEFAULT)) {
      Random random = new Random(0L);
      for (int i = 0; i < SIZE; i++) {
        out.writeInt(random.nextInt());
      }
    }
    docs = new int[SIZE];
    scratch = new int[SIZE];
    in = dir.openInput("docs", IOContext.DEFAULT);
  }

  @Setup(Level.Invocation)
  public void setupInvocation() throws IOException {
    in.seek(0);
  }

  @TearDown(Level.Trial)
  public void tearDownTrial() throws IOException {
    IOUtils.close(in, dir);
  }

  private int count(int iter) {
    return iter % 20 == 0 ? SIZE - 1 : SIZE;
  }

  @Benchmark
  public void hybridInnerLoop(Blackhole bh) throws IOException {
    for (int i = 0; i <= 100; i++) {
      int count = count(i);
      hybridInnerLoop(in, count, docs, scratch);
      bh.consume(docs);
      setupInvocation();
    }
  }

  private static void hybridInnerLoop(IndexInput in, int count, int[] docIDs, int[] scratch)
      throws IOException {
    int k = 0;
    for (int bound = count - 511; k < bound; k += 512) {
      in.readInts(scratch, k, 384);
      shift(k, docIDs, scratch, 384);
      // Can be inlined to make offsets consistent so that loop get auto-vectorized.
      remainder24(k, docIDs, scratch, 128, 256, 384);
    }
    for (int bound = count - 127; k < bound; k += 128) {
      in.readInts(scratch, k, 96);
      shift(k, docIDs, scratch, 96);
      remainder24(k, docIDs, scratch, 32, 64, 96);
    }
    readScalarInts24(in, count - k, docIDs, k);
  }

  private static void shift(int k, int[] docIds, int[] scratch, int halfAndQuarter) {
    for (int i = k, to = k + halfAndQuarter; i < to; i++) {
      docIds[i] = scratch[i] >>> 8;
    }
  }

  private static void remainder24(
      int k, int[] docIds, int[] scratch, int quarter, int half, int halfAndQuarter) {
    for (int i = k, to = k + quarter; i < to; i++) {
      docIds[i + halfAndQuarter] =
          ((scratch[i] & 0xFF) << 16)
              | ((scratch[i + quarter] & 0xFF) << 8)
              | (scratch[i + half] & 0xFF);
    }
  }

  private static void readScalarInts24(IndexInput in, int count, int[] docIDs, int offset)
      throws IOException {
    int i;
    for (i = offset; i < offset + count - 7; i += 8) {
      long l1 = in.readLong();
      long l2 = in.readLong();
      long l3 = in.readLong();
      docIDs[i] = (int) (l1 >>> 40);
      docIDs[i + 1] = (int) (l1 >>> 16) & 0xffffff;
      docIDs[i + 2] = (int) (((l1 & 0xffff) << 8) | (l2 >>> 56));
      docIDs[i + 3] = (int) (l2 >>> 32) & 0xffffff;
      docIDs[i + 4] = (int) (l2 >>> 8) & 0xffffff;
      docIDs[i + 5] = (int) (((l2 & 0xff) << 16) | (l3 >>> 48));
      docIDs[i + 6] = (int) (l3 >>> 24) & 0xffffff;
      docIDs[i + 7] = (int) l3 & 0xffffff;
    }
    for (; i < offset + count; ++i) {
      docIDs[i] = (Short.toUnsignedInt(in.readShort()) << 8) | Byte.toUnsignedInt(in.readByte());
    }
  }

  @Benchmark
  public void specializedRead(Blackhole bh) throws IOException {
    for (int i = 0; i <= 100; i++) {
      int count = count(i);
      specializedRead(in, count, docs, scratch);
      bh.consume(docs);
      setupInvocation();
    }
  }

  private static void specializedRead(IndexInput in, int count, int[] docIDs, int[] scratch)
      throws IOException {
    if (count == BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE) {
      // Same format, but enabling the JVM to specialize the decoding logic for the default number
      // of points per node proved to help on benchmarks
      doReadInts24(in, 512, docIDs, scratch);
    } else {
      doReadInts24(in, count, docIDs, scratch);
    }
  }

  private static void doReadInts24(IndexInput in, int count, int[] docIDs, int[] scratch)
      throws IOException {
    // Read the first (count - count % 4) values
    int quarter = count >> 2;
    int numBytes = quarter * 3;
    in.readInts(scratch, 0, numBytes);
    for (int i = 0; i < numBytes; ++i) {
      docIDs[i] = scratch[i] >>> 8;
      scratch[i] &= 0xFF;
    }
    for (int i = 0; i < quarter; ++i) {
      docIDs[numBytes + i] =
          scratch[i] | (scratch[quarter + i] << 8) | (scratch[2 * quarter + i] << 16);
    }
    // Now read the remaining 0, 1, 2 or 3 values
    for (int i = quarter << 2; i < count; ++i) {
      docIDs[i] = (in.readShort() & 0xFFFF) | (in.readByte() & 0xFF) << 16;
    }
  }

  @Benchmark
  public void specializedDecode(Blackhole bh) throws IOException {
    for (int i = 0; i <= 100; i++) {
      int count = count(i);
      specializedDecode(in, count, docs, scratch);
      bh.consume(docs);
      setupInvocation();
    }
  }

  private static void specializedDecode(IndexInput in, int count, int[] docIDs, int[] scratch)
      throws IOException {
    int quarter = count >> 2;
    int numBytes = quarter * 3;
    in.readInts(scratch, 0, numBytes);
    if (count == BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE) {
      // Same format, but enabling the JVM to specialize the decoding logic for the default number
      // of points per node proved to help on benchmarks
      decode24(
          docIDs,
          scratch,
          BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE / 4,
          BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE / 4 * 3);
    } else {
      decode24(docIDs, scratch, quarter, numBytes);
      // Now read the remaining 0, 1, 2 or 3 values
      for (int i = quarter << 2; i < count; ++i) {
        docIDs[i] = (in.readShort() & 0xFFFF) | (in.readByte() & 0xFF) << 16;
      }
    }
  }

  private static void decode24(int[] docIds, int[] scratch, int quarter, int numInts) {
    for (int i = 0; i < numInts; ++i) {
      docIds[i] = scratch[i] >>> 8;
      scratch[i] &= 0xFF;
    }
    for (int i = 0; i < quarter; i++) {
      docIds[i + numInts] =
          (scratch[i] << 16) | (scratch[i + quarter] << 8) | scratch[i + quarter * 2];
    }
  }

  @Benchmark
  public void specializedDecodeMaskInRemainder(Blackhole bh) throws IOException {
    for (int i = 0; i <= 100; i++) {
      int count = count(i);
      specializedDecodeMaskInRemainder(in, count, docs, scratch);
      bh.consume(docs);
      setupInvocation();
    }
  }

  private static void specializedDecodeMaskInRemainder(
      IndexInput in, int count, int[] docIDs, int[] scratch) throws IOException {
    int quarter = count >> 2;
    int numBytes = quarter * 3;
    in.readInts(scratch, 0, numBytes);
    if (count == BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE) {
      // Same format, but enabling the JVM to specialize the decoding logic for the default number
      // of points per node proved to help on benchmarks
      decode24MaskInRemainder(
          docIDs,
          scratch,
          BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE / 4,
          BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE / 4 * 3);
    } else {
      decode24MaskInRemainder(docIDs, scratch, quarter, numBytes);
      // Now read the remaining 0, 1, 2 or 3 values
      for (int i = quarter << 2; i < count; ++i) {
        docIDs[i] = (in.readShort() & 0xFFFF) | (in.readByte() & 0xFF) << 16;
      }
    }
  }

  private static void decode24MaskInRemainder(
      int[] docIds, int[] scratch, int quarter, int numInts) {
    for (int i = 0; i < numInts; ++i) {
      docIds[i] = scratch[i] >>> 8;
    }
    for (int i = 0; i < quarter; i++) {
      docIds[i + numInts] =
          ((scratch[i] & 0xFF) << 16)
              | ((scratch[i + quarter] & 0xFF) << 8)
              | (scratch[i + quarter * 2] & 0xFF);
    }
  }
}
