package org.apache.lucene.benchmark.jmh;

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorShape;
import jdk.incubator.vector.VectorSpecies;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@Fork(value = 1, jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
public class BKDCodecBenchmark {

  private Directory dir;
  private IndexInput in;

  private int[] tmpInts;
  private int[] outputInts;

  @Setup(Level.Trial)
  public void setupTrial() throws IOException {
    Path path = Files.createTempDirectory("bkd");
    dir = MMapDirectory.open(path);
    try (IndexOutput out = dir.createOutput("docs", IOContext.DEFAULT)) {
      Random r = new Random(0);
      for (int i = 0; i < 512; ++i) {
        out.writeInt(r.nextInt(Integer.MAX_VALUE));
      }
    }
    in = dir.openInput("docs", IOContext.DEFAULT);
    outputInts = new int[512];
    tmpInts = new int[512];
  }

  @Setup(Level.Invocation)
  public void setupInvocation() throws IOException {
    // Reset the position of the buffer
    in.seek(0);
  }

  @TearDown(Level.Trial)
  public void tearDownTrial() throws IOException {
    in.close();
    dir.close();
  }

  @Benchmark
  public void readInts24Legacy(Blackhole bh) throws IOException {
    for (int i = 0; i <= 100; i++) {
      int count = count(i);
      decode24Legacy(outputInts, tmpInts, in, count);
      bh.consume(outputInts);
      setupInvocation();
    }
  }

  private static void decode24Legacy(int[] outputInts, int[] tmpInts, IndexInput input, int count) throws IOException {
    int i;
    for (i = 0; i < count - 7; i += 8) {
      long l1 = input.readLong();
      long l2 = input.readLong();
      long l3 = input.readLong();
      outputInts[i] = (int) (l1 >>> 40);
      outputInts[i + 1] = (int) (l1 >>> 16) & 0xffffff;
      outputInts[i + 2] = (int) (((l1 & 0xffff) << 8) | (l2 >>> 56));
      outputInts[i + 3] = (int) (l2 >>> 32) & 0xffffff;
      outputInts[i + 4] = (int) (l2 >>> 8) & 0xffffff;
      outputInts[i + 5] = (int) (((l2 & 0xff) << 16) | (l3 >>> 48));
      outputInts[i + 6] = (int) (l3 >>> 24) & 0xffffff;
      outputInts[i + 7] = (int) l3 & 0xffffff;
    }
    for (; i < count; ++i) {
      outputInts[i] = (Short.toUnsignedInt(input.readShort()) << 8) | Byte.toUnsignedInt(input.readByte());
    }
  }

  @Benchmark
  public void readInts16ForUtil(Blackhole bh) throws IOException {
    for (int i = 0; i <= 100; i++) {
      int count = count(i);
      decodeForUtil16(outputInts, tmpInts, in, count);
      bh.consume(outputInts);
      setupInvocation();
    }
  }

  private static void decodeForUtil16(int[] outputInts, int[] tmpInts, IndexInput in, int count) throws IOException{
    final int min = in.readVInt();
    final int halfLen = count >>> 1;
    in.readInts(outputInts, 0, halfLen);
    for (int i = 0; i < halfLen; ++i) {
      int l = outputInts[i];
      outputInts[i] = (l >>> 16) + min;
      outputInts[halfLen + i] = (l & 0xFFFF) + min;
    }
    if ((count & 1) == 1) {
      outputInts[count - 1] = Short.toUnsignedInt(in.readShort()) + min;
    }
  }

  @Benchmark
  public void readInts24ForUtil(Blackhole bh) throws IOException {
    for (int i = 0; i <= 100; i++) {
      int count = count(i);
      decodeForUtil24(outputInts, tmpInts, in, count);
      bh.consume(outputInts);
      setupInvocation();
    }
  }

  private static void decodeForUtil24(int[] outputInts, int[] tmpInts, IndexInput in, int count) throws IOException{
    final int quarterLen = count >> 2;
    final int quarterLen3 = quarterLen * 3;
    in.readInts(tmpInts, 0, quarterLen3);
    for (int i = 0; i < quarterLen3; ++i) {
      outputInts[i] = tmpInts[i] >>> 8;
    }
    for (int i = 0; i < quarterLen; i++) {
      outputInts[i + quarterLen3] =
          ((tmpInts[i] & 0xFF) << 16)
              | ((tmpInts[i + quarterLen] & 0xFF) << 8)
              | (tmpInts[i + quarterLen * 2] & 0xFF);
    }
    int remainder = count & 0x3;
    if (remainder > 0) {
      in.readInts(outputInts, quarterLen << 2, remainder);
    }
  }

  @Benchmark
  public void readInts16Vector(Blackhole bh) throws IOException {
    for (int i = 0; i <= 100; i++) {
      int count = count(i);
      decodeVector16(outputInts, tmpInts, in, count);
      bh.consume(outputInts);
      setupInvocation();
    }
  }

  private static void decodeVector16(int[] outputInts, int[] tmpInts, IndexInput in, int count) throws IOException{
    final int min = in.readVInt();
    final int halfLen = count >>> 1;
    in.readInts(outputInts, 0, halfLen);
    int upperBound = PREFERRED_INT_SPECIES.loopBound(halfLen);
    int i = 0;
    for (; i < upperBound; i += PREFERRED_INT_SPECIES.length()) {
      IntVector vector = IntVector.fromArray(PREFERRED_INT_SPECIES, outputInts, i);
      vector
          .lanewise(VectorOperators.LSHR, 16).lanewise(VectorOperators.ADD, min)
          .intoArray(outputInts, i);
      vector
          .lanewise(VectorOperators.AND, 0xFFFF).lanewise(VectorOperators.ADD, min)
          .intoArray(outputInts, i + halfLen);
    }

    for (;i < halfLen; ++i) {
      int l = outputInts[i];
      outputInts[i] = (l >>> 16) + min;
      outputInts[halfLen + i] = (l & 0xFFFF) + min;
    }
    if ((count & 1) == 1) {
      outputInts[count - 1] = Short.toUnsignedInt(in.readShort()) + min;
    }
  }

  static final VectorSpecies<Integer> PREFERRED_INT_SPECIES =
      VectorSpecies.of(int.class, VectorShape.forBitSize(VectorShape.preferredShape().vectorBitSize()));

  @Benchmark
  public void readInts24Vector(Blackhole bh) throws IOException {
    for (int i = 0; i <= 100; i++) {
      int count = count(i);
      decodeVector24(outputInts, tmpInts, in, count);
      bh.consume(outputInts);
      setupInvocation();
    }
  }
  
  private static int count(int iter) {
    // to make benchmark more realistic
    return iter % 10 == 0 ? 511 : 512;
  }

  private static void decodeVector24(int[] outputInts, int[] tmpInts, IndexInput in, int count) throws IOException {
    final int quarterLen = count >> 2;
    final int halfLen = quarterLen << 1;
    final int quarterLen3 = quarterLen * 3;
    in.readInts(tmpInts, 0, quarterLen3);
    int upperBound = PREFERRED_INT_SPECIES.loopBound(quarterLen3);
    int i = 0;
    for (; i < upperBound; i += PREFERRED_INT_SPECIES.length()) {
      IntVector.fromArray(PREFERRED_INT_SPECIES, tmpInts, i).lanewise(VectorOperators.LSHR, 8).intoArray(outputInts, i);
    }
    for (; i < quarterLen3; ++i) {
      outputInts[i] = tmpInts[i] >>> 8;
    }

    i = 0;
    int upperBound2 = PREFERRED_INT_SPECIES.loopBound(quarterLen);
    for (; i < upperBound2; i += PREFERRED_INT_SPECIES.length()) {
      IntVector.fromArray(PREFERRED_INT_SPECIES, tmpInts, i).lanewise(VectorOperators.AND, 0xFF).lanewise(VectorOperators.LSHL, 16)
          .lanewise(VectorOperators.OR, IntVector.fromArray(PREFERRED_INT_SPECIES, tmpInts, i + quarterLen).lanewise(VectorOperators.AND, 0xFF).lanewise(VectorOperators.LSHL, 8))
          .lanewise(VectorOperators.OR, IntVector.fromArray(PREFERRED_INT_SPECIES, tmpInts, i + halfLen).lanewise(VectorOperators.AND, 0xFF))
          .intoArray(outputInts, quarterLen3 + i);
    }
    for (; i < quarterLen; i++) {
      outputInts[i + quarterLen3] =
          ((tmpInts[i] & 0xFF) << 16)
              | ((tmpInts[i + quarterLen] & 0xFF) << 8)
              | (tmpInts[i + quarterLen * 2] & 0xFF);
    }

    int remainder = count & 0x3;
    if (remainder > 0) {
      in.readInts(outputInts, quarterLen << 2, remainder);
    }
  }

  static {
    try {
      Path path = Files.createTempDirectory("test");
      Directory dir = MMapDirectory.open(path);
      try (IndexOutput out = dir.createOutput("docs", IOContext.DEFAULT)) {
        Random r = new Random(0);
        for (int i = 0; i < 512; ++i) {
          out.writeInt(r.nextInt(Integer.MAX_VALUE));
        }
      }
      IndexInput in = dir.openInput("docs", IOContext.DEFAULT);
      int[] outputInts = new int[512];
      int[] outputInts2 = new int[512];
      int[] tmpInts = new int[512];

      in.seek(0);
      decodeForUtil16(outputInts, tmpInts, in, 512);
      in.seek(0);
      decodeVector16(outputInts2, tmpInts, in, 512);
      if (Arrays.equals(outputInts, outputInts2) == false) {
        throw new RuntimeException("expect: " + Arrays.toString(outputInts)
            + ", got: " + Arrays.toString(outputInts2));
      }

      in.seek(0);
      decodeForUtil24(outputInts, tmpInts, in, 512);
      in.seek(0);
      decodeVector24(outputInts2, tmpInts, in, 512);
      if (Arrays.equals(outputInts, outputInts2) == false) {
        throw new RuntimeException("expect: " + Arrays.toString(outputInts)
            + ", got: " + Arrays.toString(outputInts2));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
