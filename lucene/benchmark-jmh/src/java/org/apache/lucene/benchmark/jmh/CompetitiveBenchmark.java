package org.apache.lucene.benchmark.jmh;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.VectorUtil;
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
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(
    value = 1,
    jvmArgsAppend = {
      "-Xmx1g",
      "-Xms1g",
      "-XX:+AlwaysPreTouch",
      "--add-modules",
      "jdk.incubator.vector"
    })
public class CompetitiveBenchmark {

  private Random R = new Random(0);

  @Param("128")
  int size;

  double[] scores;
  int[] docs;
  double minScoreInclusive;

  @Setup(Level.Trial)
  public void setUpTrial() {
    scores = new double[size];
    docs = new int[size];
    R = new Random(System.currentTimeMillis());
  }

  @Setup(Level.Invocation)
  public void setUpInvocation() {
    for (int i = 0; i < size; i++) {
      docs[i] = R.nextInt(Integer.MAX_VALUE);
      scores[i] = R.nextDouble();
    }
    minScoreInclusive = R.nextDouble();
  }

  @Benchmark
  public int baseline() {
    int newSize = 0;
    for (int i = 0; i < size; ++i) {
      if (scores[i] >= minScoreInclusive) {
        docs[newSize] = docs[i];
        scores[newSize] = scores[i];
        newSize++;
      }
    }
    return newSize;
  }

  @Benchmark
  public int candidate() {
    return VectorUtil.filterByScore(docs, scores, minScoreInclusive, size);
  }

  public static void main(String[] args) {
    CompetitiveBenchmark baseline = new CompetitiveBenchmark();
    baseline.size = 128;
    baseline.setUpTrial();
    baseline.setUpInvocation();
    int baselineSize = baseline.baseline();

    CompetitiveBenchmark candidate = new CompetitiveBenchmark();
    candidate.size = 128;
    candidate.setUpTrial();
    candidate.setUpInvocation();
    int candidateSize = candidate.candidate();

    if (baselineSize != candidateSize) {
      throw new IllegalArgumentException("incorrect size");
    }

    if (Arrays.equals(baseline.docs, 0, baselineSize, candidate.docs, 0, candidateSize) == false) {
      throw new IllegalArgumentException(
          "incorrect docs,"
              + "\nbaseline: "
              + Arrays.toString(ArrayUtil.copyOfSubArray(baseline.docs, 0, baselineSize))
              + "\ncandidate: "
              + Arrays.toString(ArrayUtil.copyOfSubArray(candidate.docs, 0, candidateSize)));
    }

    if (Arrays.equals(baseline.scores, 0, baselineSize, candidate.scores, 0, candidateSize)
        == false) {
      throw new IllegalArgumentException("incorrect scores");
    }
  }
}
