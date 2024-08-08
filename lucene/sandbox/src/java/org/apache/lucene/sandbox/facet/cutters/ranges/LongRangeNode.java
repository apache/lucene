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
package org.apache.lucene.sandbox.facet.cutters.ranges;

import org.apache.lucene.internal.hppc.IntArrayList;

/**
 * Holds one node of the segment tree.
 *
 * <p>TODO: dedup existing LongRangeNode.
 */
final class LongRangeNode {
  final LongRangeNode left;
  final LongRangeNode right;

  // Our range, inclusive:
  final long start;
  final long end;

  // Which range indices to output when a query goes
  // through this node:
  IntArrayList outputs;

  /** add doc * */
  LongRangeNode(long start, long end, LongRangeNode left, LongRangeNode right) {
    this.start = start;
    this.end = end;
    this.left = left;
    this.right = right;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    toString(sb, 0);
    return sb.toString();
  }

  private static void indent(StringBuilder sb, int depth) {
    sb.append("  ".repeat(depth));
  }

  /** Recursively assigns range outputs to each node. */
  public void addOutputs(LongRangeFacetCutter.LongRangeAndPos range) {
    if (start >= range.range().min && end <= range.range().max) {
      // Our range is fully included in the incoming
      // range; add to our output list:
      if (outputs == null) {
        outputs = new IntArrayList();
      }
      outputs.add(range.pos());
    } else if (left != null) {
      assert right != null;
      // Recurse:
      left.addOutputs(range);
      right.addOutputs(range);
    }
  }

  private void toString(StringBuilder sb, int depth) {
    indent(sb, depth);
    if (left == null) {
      assert right == null;
      sb.append("leaf: ").append(start).append(" to ").append(end);
    } else {
      sb.append("node: ").append(start).append(" to ").append(end);
    }
    if (outputs != null) {
      sb.append(" outputs=");
      sb.append(outputs);
    }
    sb.append('\n');

    if (left != null) {
      assert right != null;
      left.toString(sb, depth + 1);
      right.toString(sb, depth + 1);
    }
  }
}
