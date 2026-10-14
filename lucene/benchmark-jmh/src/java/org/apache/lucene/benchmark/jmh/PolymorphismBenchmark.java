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

import java.util.Random;
import java.util.concurrent.TimeUnit;
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

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(
    value = 1,
    jvmArgsAppend = {"-Xmx1g", "-Xms1g", "-XX:+AlwaysPreTouch"})
public class PolymorphismBenchmark {

  public abstract static class IntList {

    public abstract int size();

    public abstract int get(int index);

    public final long sum1() {
      long sum = 0;
      for (int i = 0; i < size(); ++i) {
        sum += get(i);
      }
      return sum;
    }

    public long sum2() {
      long sum = 0;
      for (int i = 0; i < size(); ++i) {
        sum += get(i);
      }
      return sum;
    }

    public abstract long sum3();
  }

  public static class List1 extends IntList {

    @Override
    public int size() {
      return 128;
    }

    @Override
    public int get(int index) {
      return 1;
    }

    @Override
    public long sum2() {
      return super.sum2();
    }

    @Override
    public long sum3() {
      long sum = 0;
      for (int i = 0; i < size(); ++i) {
        sum += get(i);
      }
      return sum;
    }
  }

  public static class List2 extends IntList {

    @Override
    public int size() {
      return 128;
    }

    @Override
    public int get(int index) {
      return 2;
    }

    @Override
    public long sum2() {
      return super.sum2();
    }

    @Override
    public long sum3() {
      long sum = 0;
      for (int i = 0; i < size(); ++i) {
        sum += get(i);
      }
      return sum;
    }
  }

  public static class List3 extends IntList {

    @Override
    public int size() {
      return 128;
    }

    @Override
    public int get(int index) {
      return 3;
    }

    @Override
    public long sum2() {
      return super.sum2();
    }

    @Override
    public long sum3() {
      long sum = 0;
      for (int i = 0; i < size(); ++i) {
        sum += get(i);
      }
      return sum;
    }
  }

  private IntList[] lists;

  @Setup(Level.Trial)
  public void setup() throws Exception {
    lists = new IntList[100];
    Random r = new Random(0);
    for (int i = 0; i < lists.length; ++i) {
      lists[i] =
          switch (r.nextInt(4)) {
            case 0 -> new List1();
            case 1 -> new List2();
            default -> new List3();
          };
    }
  }

  @Benchmark
  public long defaultImpl() {
    long sum = 0;
    for (IntList list : lists) {
      sum += list.sum1();
    }
    return sum;
  }

  @Benchmark
  public long delegateToDefaultImpl() {
    long sum = 0;
    for (IntList list : lists) {
      sum += list.sum2();
    }
    return sum;
  }

  @Benchmark
  public long specializedImpl() {
    long sum = 0;
    for (IntList list : lists) {
      sum += list.sum3();
    }
    return sum;
  }
}
