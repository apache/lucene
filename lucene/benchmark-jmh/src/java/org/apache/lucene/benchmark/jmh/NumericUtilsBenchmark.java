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

import java.math.BigInteger;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.NumericUtils;
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
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
// first iteration is complete garbage, so make sure we really warmup
@Warmup(iterations = 4, time = 1)
// real iterations. not useful to spend tons of time here, better to fork more
@Measurement(iterations = 5, time = 1)
// engage some noise reduction
@Fork(
    value = 3,
    jvmArgsAppend = {"-Xmx2g", "-Xms2g", "-XX:+AlwaysPreTouch"})
public class NumericUtilsBenchmark {
  @Param({"1", "128", "207", "256", "300", "512", "702", "1024"})
  int size;

  private byte[] subA;
  private byte[] subB;
  private byte[] subResult;
  private byte[] subExpected;

  private byte[] addA;
  private byte[] addB;
  private byte[] addResult;
  private byte[] addExpected;

  @Setup(Level.Iteration)
  public void subInit() {
    ThreadLocalRandom random = ThreadLocalRandom.current();

    subA = new byte[size];
    subB = new byte[size];
    subResult = new byte[size];
    subExpected = new byte[size];

    random.nextBytes(subA);
    random.nextBytes(subB);

    // Treat as unsigned integers
    BigInteger aBig = new BigInteger(1, subA);
    BigInteger bBig = new BigInteger(1, subB);

    // Swap a <-> b if a < b
    if (aBig.compareTo(bBig) < 0) {
      byte[] temp = subA;
      subA = subB;
      subB = temp;

      BigInteger tempBig = aBig;
      aBig = bBig;
      bBig = tempBig;
    }

    byte[] temp = aBig.subtract(bBig).toByteArray();
    if (temp.length == size + 1) { // BigInteger pads with extra 0 if MSB is 1
      assert temp[0] == 0;
      System.arraycopy(temp, 1, subExpected, 0, size);
    } else {
      System.arraycopy(temp, 0, subExpected, size - temp.length, temp.length);
    }
  }

  @Setup(Level.Iteration)
  public void addInit() {
    ThreadLocalRandom random = ThreadLocalRandom.current();

    addA = new byte[size];
    addB = new byte[size];
    addResult = new byte[size];
    addExpected = new byte[size];

    random.nextBytes(addA);
    random.nextBytes(addB);

    // Treat as unsigned integers
    BigInteger aBig = new BigInteger(1, addA);
    BigInteger bBig = new BigInteger(1, addB);

    byte[] temp = aBig.add(bBig).toByteArray();
    if (temp.length == size + 1) { // BigInteger pads with extra 0 if MSB is 1
      if (temp[0] != 0) { // overflow
        addInit(); // re-init
        return;
      }
      System.arraycopy(temp, 1, addExpected, 0, size);
    } else {
      System.arraycopy(temp, 0, addExpected, size - temp.length, temp.length);
    }
  }

  @Benchmark
  public void subtract() {
    NumericUtils.subtract(size, 0, subA, subB, subResult);
    assert Arrays.equals(subExpected, subResult);
  }

  @Benchmark
  public void add() {
    NumericUtils.add(size, 0, addA, addB, addResult);
    assert Arrays.equals(addExpected, addResult);
  }
}
