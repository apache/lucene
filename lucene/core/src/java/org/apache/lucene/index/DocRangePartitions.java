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
package org.apache.lucene.index;

import java.util.Arrays;
import java.util.List;

/**
 * How a partitioned merge splits its inputs: for every input segment, the doc range each output
 * takes from it. Returned by {@link MergePolicy.OneMerge#getDocRangePartitions(List)}.
 *
 * @lucene.experimental
 */
public final class DocRangePartitions {

  private final int[][] boundaries;

  /**
   * Builds a partitioning from one boundary array per input segment, in the order of {@link
   * MergePolicy.OneMerge#segments}. Each array holds {@code outputCount + 1} non-decreasing doc ids
   * starting at {@code 0}, so output {@code o} takes docs {@code [b[o], b[o + 1])}; every array
   * must have the same length, and the last entry must be that input's {@code maxDoc}, which {@link
   * IndexWriter} checks against the readers.
   */
  public static DocRangePartitions of(int[][] boundariesPerInput) {
    if (boundariesPerInput.length == 0) {
      throw new IllegalArgumentException("a partitioning must cover at least one input segment");
    }
    final int length = boundariesPerInput[0].length;
    if (length < 2) {
      throw new IllegalArgumentException(
          "a partitioning must have at least one output, got boundaries of length " + length);
    }
    final int[][] copy = new int[boundariesPerInput.length][];
    for (int input = 0; input < boundariesPerInput.length; input++) {
      final int[] b = boundariesPerInput[input].clone();
      if (b.length != length) {
        throw new IllegalArgumentException(
            "input "
                + input
                + " has "
                + (b.length - 1)
                + " outputs but input 0 has "
                + (length - 1)
                + "; every input is split into the same number of outputs");
      }
      if (b[0] != 0) {
        throw new IllegalArgumentException("input " + input + " must start at doc 0, got " + b[0]);
      }
      for (int output = 1; output < b.length; output++) {
        if (b[output] < b[output - 1]) {
          throw new IllegalArgumentException(
              "input " + input + " has boundaries that go backwards at output " + output);
        }
      }
      copy[input] = b;
    }
    return new DocRangePartitions(copy);
  }

  private DocRangePartitions(int[][] boundaries) {
    this.boundaries = boundaries;
  }

  /** Number of input segments, matching {@link MergePolicy.OneMerge#segments}. */
  public int inputCount() {
    return boundaries.length;
  }

  /** Number of output segments the merge writes. */
  public int outputCount() {
    return boundaries[0].length - 1;
  }

  /** First doc of {@code input} that {@code output} takes. */
  public int start(int input, int output) {
    return boundaries[input][output];
  }

  /** Doc after the last one of {@code input} that {@code output} takes. */
  public int end(int input, int output) {
    return boundaries[input][output + 1];
  }

  /**
   * The boundaries of {@code input}, as {@link #of} takes them. Returns a copy, so a policy
   * deriving a partitioning from another one can adjust them in place.
   */
  public int[] boundaries(int input) {
    return boundaries[input].clone();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("DocRangePartitions(outputs=").append(outputCount());
    for (int input = 0; input < boundaries.length; input++) {
      sb.append(input == 0 ? ", " : " ").append(Arrays.toString(boundaries[input]));
    }
    return sb.append(')').toString();
  }
}
