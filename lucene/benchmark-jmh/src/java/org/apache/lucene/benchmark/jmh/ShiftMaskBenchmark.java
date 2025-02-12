package org.apache.lucene.benchmark.jmh;

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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1)
public class ShiftMaskBenchmark {

  private int[] counts;
  private int[] source;
  private int[] dest;

  @Setup(Level.Trial)
  public void setupTrial() throws IOException {
    Random r = new Random(0);
    source = new int[1024];
    dest = new int[1024];
    for (int i=0; i< 512; i++) {
      source[i] = r.nextInt(1 << 24);
    }
    counts = new int[] {255, 256, 511, 512};
  }

  @Benchmark
  public void varOffset(Blackhole bh) throws IOException {
    for (int count : counts) {
      shiftMask(source, dest, count & 0x1, count, 8, 0xFF);
    }
  }

  @Benchmark
  public void fixOffset(Blackhole bh) throws IOException {
    for (int count : counts) {
      shiftMask(source, dest, 1, count, 8, 0xFF);
    }
  }

  private static void shiftMask(int[] src, int[] dst, int offset, int count, int shift, int mask) {
    for (int i=0; i < count; i++) {
      dst[i] = (src[i + offset] >> shift) & mask;
    }
  }
}
