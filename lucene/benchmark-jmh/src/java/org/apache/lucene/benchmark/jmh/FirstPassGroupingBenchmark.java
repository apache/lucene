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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
public class FirstPassGroupingBenchmark {

  private int[] values;
  private int[] groups;
  private static final int TOP = 10;

  int size = 10_000;
  int MAX = 10_000;

  private Comparator<Group> comparator =
      new Comparator<Group>() {
        @Override
        public int compare(Group o1, Group o2) {
          final int c = o1.value - o2.value;
          if (c != 0) {
            return c;
          } else {
            return o1.id - o2.id;
          }
        }
      };

  private TreeSet<Group> treeSet;
  private PriorityQueue<Group> heap;

  @Setup(Level.Iteration)
  public void init() {
    values = new int[size];
    groups = new int[size];
    ThreadLocalRandom random = ThreadLocalRandom.current();

    for (int i = 0; i < size; i++) {
      groups[i] = random.nextInt(0, MAX);
      values[i] = random.nextInt(0, MAX);
    }
  }

  @Benchmark
  public List<Group> getTopNWithTreeMap() {
    treeSet = new TreeSet<>(comparator);
    Map<Integer, Group> groupMap = new HashMap<>();
    for (int i = 0; i < size; i++) {
      if (groupMap.containsKey(groups[i])) {
        Group group = groupMap.get(groups[i]);
        if (group.value > values[i]) {
          // Group get more competitive, re-add it.
          if (treeSet.isEmpty() == false) {
            treeSet.remove(group);
            group.value = values[i];
            group.id = i;
            treeSet.add(group);
          } else {
            group.value = values[i];
            group.id = i;
          }
        }
      } else {
        Group group = new Group(groups[i], values[i], i);
        if (groupMap.size() < TOP) {
          groupMap.put(group.group, group);
          if (groupMap.size() == TOP) {
            treeSet.addAll(groupMap.values());
          }
        } else if (treeSet.isEmpty() == false) {
          Group last = treeSet.last();
          if (comparator.compare(last, group) > 0) {
            treeSet.pollLast();
            treeSet.add(group);
            groupMap.remove(last.group);
            groupMap.put(group.group, group);
          }
        }
      }
    }

    final List<Group> result = new ArrayList<>();
    for (Group obj : treeSet) {
      result.add(obj);
    }
    return result;
  }

  @Benchmark
  public List<Group> getTopNWithHeap() {
    heap =
        new PriorityQueue<>(TOP) {
          @Override
          protected boolean lessThan(Group o1, Group o2) {
            // big top heap.
            final int c = o1.value - o2.value;
            if (c != 0) {
              return c > 0;
            } else {
              return o1.id > o2.id;
            }
          }
        };

    Map<Integer, Group> groupMap = new HashMap<>();
    for (int i = 0; i < size; i++) {
      if (groupMap.containsKey(groups[i])) {
        Group group = groupMap.get(groups[i]);
        if (group.value > values[i]) {
          group.value = values[i];
          group.id = i;
          // Group get more competitive, re-add it.
          if (heap.size() > 0) {
            heap.remove(group);
            heap.add(group);
          }
        }
      } else {
        Group group = new Group(groups[i], values[i], i);
        if (groupMap.size() < TOP) {
          groupMap.put(group.group, group);
          if (groupMap.size() == TOP) {
            heap.addAll(groupMap.values());
          }
        } else if (heap.size() > 0) {
          Group top = heap.top();
          if (comparator.compare(top, group) > 0) {
            heap.updateTop(group);
            groupMap.remove(top.group);
            groupMap.put(group.group, group);
          }
        }
      }
    }

    final List<Group> result = new ArrayList<>();
    while (heap.size() > 0) {
      result.add(0, heap.pop());
    }
    return result;
  }

  // validate result
  public static void main(String[] args) {
    FirstPassGroupingBenchmark benchmark = new FirstPassGroupingBenchmark();
    benchmark.init();
    List<Group> top100Heap = benchmark.getTopNWithHeap();
    List<Group> top100TreeMap = benchmark.getTopNWithTreeMap();

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

  private class Group {
    private int group;
    private int value;
    private int id;

    private Group(int group, int value, int id) {
      this.group = group;
      this.value = value;
      this.id = id;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj instanceof Group == false) return false;
      return this.group == ((Group) obj).group
          && this.value == ((Group) obj).value
          && this.id == ((Group) obj).id;
    }

    @Override
    public int hashCode() {
      int result = Integer.hashCode(this.group);
      result = 31 * result + Integer.hashCode(this.value);
      result = 31 * result + Integer.hashCode(this.id);
      return result;
    }
  }
}
