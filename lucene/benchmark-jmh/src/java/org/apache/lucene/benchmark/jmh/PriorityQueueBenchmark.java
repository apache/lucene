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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.PriorityQueue;
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
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
// first iteration is complete garbage, so make sure we really warmup
@Warmup(iterations = 4, time = 1)
// real iterations. not useful to spend tons of time here, better to fork more
@Measurement(iterations = 5, time = 1)
// engage some noise reduction
@Fork(
    value = 3,
    jvmArgsAppend = {
      "-Xmx2g",
      "-Xms2g",
      "-XX:+AlwaysPreTouch",
      "--add-modules",
      "jdk.incubator.vector"
    })
public class PriorityQueueBenchmark {

  private int[] ints = new int[1_000_000];
  private static final int TOP = 100;

  private Comparator<CompObj> comparator =
      new Comparator<CompObj>() {
        @Override
        public int compare(CompObj o1, CompObj o2) {
          final int c = o1.value - o2.value;
          if (c != 0) {
            return c;
          } else {
            return o1.index - o2.index;
          }
        }
      };

  private TreeSet<CompObj> treeSet = new TreeSet<>(comparator);
  private PriorityQueue<CompObj> heap =
      new PriorityQueue<>(TOP) {
        @Override
        protected boolean lessThan(CompObj o1, CompObj o2) {
          // big top heap.
          final int c = o1.value - o2.value;
          if (c != 0) {
            return c > 0;
          } else {
            return o1.index > o2.index;
          }
        }
      };

  @Setup(Level.Iteration)
  public void init() {
    ThreadLocalRandom random = ThreadLocalRandom.current();

    for (int i = 0; i < ints.length; i++) {
      ints[i] = random.nextInt(0, Integer.MAX_VALUE);
    }
  }

  @Benchmark
  public List<CompObj> getTopNWithTreeMap() {
    treeSet.clear();
    for (int i = 0; i < ints.length; i++) {
      if (treeSet.size() < TOP) {
        treeSet.add(new CompObj(ints[i], i));
      } else {
        CompObj tmp = new CompObj(ints[i], i);
        if (comparator.compare(treeSet.last(), tmp) > 0) {
          treeSet.pollLast();
          treeSet.add(tmp);
        }
      }
    }

    final List<CompObj> result = new ArrayList<>();
    for (CompObj obj : treeSet) {
      result.add(obj);
    }
    return result;
  }

  @Benchmark
  public List<CompObj> getTopNWithHeap() {
    heap.clear();
    for (int i = 0; i < ints.length; i++) {
      if (heap.size() < TOP) {
        heap.add(new CompObj(ints[i], i));
      } else {
        CompObj tmp = new CompObj(ints[i], i);
        if (comparator.compare(heap.top(), tmp) > 0) {
          heap.updateTop(tmp);
        }
      }
    }

    final List<CompObj> result = new ArrayList<>();
    while (heap.size() > 0) {
      result.add(0, heap.pop());
    }
    return result;
  }

  // validate result
  public static void main(String[] args) {
    PriorityQueueBenchmark priorityQueueBenchmark = new PriorityQueueBenchmark();
    priorityQueueBenchmark.init();
    List<CompObj> top100Heap = priorityQueueBenchmark.getTopNWithHeap();
    List<CompObj> top100TreeMap = priorityQueueBenchmark.getTopNWithTreeMap();

    for (int i = 0; i < top100Heap.size(); i++) {
      if (top100Heap.get(i).equals(top100TreeMap.get(i)) == false) {
        throw new AssertionError(
            "i: "
                + i
                + ", top100Heap: "
                + top100Heap.get(i)
                + ", top100TreeMap: "
                + top100TreeMap.get(i));
      }
    }
  }

  private record CompObj(int value, int index) {}
}
