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
package org.apache.lucene.codecs.lucene103.blocktree;

import java.io.IOException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;

class TrieReader {

  private static final long NO_OUTPUT = -1;
  private static final long NO_FLOOR_DATA = -1;
  private static final long[] BYTES_MINUS_1_MASK =
      new long[] {
        0xFFL,
        0xFFFFL,
        0xFFFFFFL,
        0xFFFFFFFFL,
        0xFFFFFFFFFFL,
        0xFFFFFFFFFFFFL,
        0xFFFFFFFFFFFFFFL,
        0xFFFFFFFFFFFFFFFFL
      };

  static class Node {

    // single child
    private long childDeltaFp;

    // multi children
    private long strategyFp;
    private int childSaveStrategy;
    private int strategyBytes;
    private int childrenDeltaFpBytes;

    // common
    private int sign;
    private long fp;
    private int minChildrenLabel;
    int label;
    long outputFp;
    boolean hasTerms;
    long floorDataFp;

    boolean hasOutput() {
      return outputFp != NO_OUTPUT;
    }

    boolean isFloor() {
      return floorDataFp != NO_FLOOR_DATA;
    }

    IndexInput floorData(TrieReader r) throws IOException {
      assert isFloor();
      r.input.seek(floorDataFp);
      return r.input;
    }
  }

  final RandomAccessInput access;
  final IndexInput input;
  final Node root;

  TrieReader(IndexInput input, long rootFP) throws IOException {
    this.access = input.randomAccessSlice(0, input.length());
    this.input = input;
    this.root = new Node();
    load(root, rootFP);
  }

  private void load(Node node, long fp) throws IOException {
    node.fp = fp;
    long termFlagsLong = access.readLong(fp);
    int termFlags = (int) termFlagsLong;
    int sign = node.sign = termFlags & 0x03;

    if (sign == TrieBuilder.SIGN_NO_CHILDREN) {
      loadLeafNode(fp, termFlags, termFlagsLong, node);
    } else if (sign == TrieBuilder.SIGN_MULTI_CHILDREN) {
      loadMultiChildrenNode(fp, termFlags, termFlagsLong, node);
    } else {
      loadSingleChildNode(fp, sign, termFlags, termFlagsLong, node);
    }
  }

  private void loadLeafNode(long fp, int term, long termLong, Node node) throws IOException {

    // [n bytes] floor data
    // [n bytes] output fp
    // [1bit] x | [1bit] has floor | [1bit] has terms | [3bit] output fp bytes | [2bit] sign

    int fpBytesMinus1 = (term >>> 2) & 0x07;
    if (fpBytesMinus1 <= 6) {
      node.outputFp = (termLong >>> 8) & BYTES_MINUS_1_MASK[fpBytesMinus1];
    } else {
      node.outputFp = access.readLong(fp + 1);
    }
    node.hasTerms = (term & TrieBuilder.LEAF_NODE_HAS_TERMS) != 0;
    if ((term & TrieBuilder.LEAF_NODE_HAS_FLOOR) != 0) { // has floor
      node.floorDataFp = fp + 2 + fpBytesMinus1;
    } else {
      node.floorDataFp = NO_FLOOR_DATA;
    }
  }

  private void loadSingleChildNode(long fp, int sign, int term, long termLong, Node node)
      throws IOException {

    // [n bytes] floor data
    // [n bytes] encoded output fp | [n bytes] child fp | [1 byte] label
    // [3bit] encoded output fp bytes | [3bit] child fp bytes | [2bit] sign

    int childDeltaFpBytesMinus1 = (term >>> 2) & 0x07;
    long l = childDeltaFpBytesMinus1 <= 5 ? termLong >>> 16 : access.readLong(fp + 2);
    node.childDeltaFp = l & BYTES_MINUS_1_MASK[childDeltaFpBytesMinus1];
    node.minChildrenLabel = (term >>> 8) & 0xFF;

    if (sign == TrieBuilder.SIGN_SINGLE_CHILD_WITHOUT_OUTPUT) {
      node.outputFp = NO_OUTPUT;
    } else { // has output
      assert sign == TrieBuilder.SIGN_SINGLE_CHILD_WITH_OUTPUT;
      int encodedOutputFpBytesMinus1 = (term >>> 5) & 0x07;
      long offset = fp + childDeltaFpBytesMinus1 + 3;
      long encodedFp = access.readLong(offset) & BYTES_MINUS_1_MASK[encodedOutputFpBytesMinus1];
      node.outputFp = encodedFp >>> 2;
      node.hasTerms = (encodedFp & TrieBuilder.NON_LEAF_NODE_HAS_TERMS) != 0;
      if ((encodedFp & TrieBuilder.NON_LEAF_NODE_HAS_FLOOR) != 0) { // has floor
        node.floorDataFp = offset + encodedOutputFpBytesMinus1 + 1;
      } else {
        node.floorDataFp = NO_FLOOR_DATA;
      }
    }
  }

  private void loadMultiChildrenNode(long fp, int term, long termLong, Node node)
      throws IOException {

    // [n bytes] floor data
    // [n bytes] children fps | [n bytes] strategy data
    // [1 byte] children count (if floor data) | [n bytes] encoded output fp | [1 byte] label
    // [5bit] strategy bytes | 2bit children strategy | [3bit] encoded output fp bytes
    // [1bit] has output | [3bit] children fp bytes | [2bit] sign

    node.childrenDeltaFpBytes = ((term >>> 2) & 0x07) + 1;
    node.childSaveStrategy = (term >>> 9) & 0x03;
    node.strategyBytes = ((term >>> 11) & 0x1F) + 1;
    node.minChildrenLabel = (term >>> 16) & 0xFF;

    if ((term & 0x20) != 0) { // has output
      int encodedOutputFpBytesMinus1 = (term >>> 6) & 0x07;
      long l = encodedOutputFpBytesMinus1 <= 4 ? termLong >>> 24 : access.readLong(fp + 3);
      long encodedFp = l & BYTES_MINUS_1_MASK[encodedOutputFpBytesMinus1];
      node.outputFp = encodedFp >>> 2;
      node.hasTerms = (encodedFp & TrieBuilder.NON_LEAF_NODE_HAS_TERMS) != 0;

      if ((encodedFp & TrieBuilder.NON_LEAF_NODE_HAS_FLOOR) != 0) { // has floor
        long offset = fp + 4 + encodedOutputFpBytesMinus1;
        long childrenNum = (access.readByte(offset) & 0xFFL) + 1L;
        node.strategyFp = offset + 1L;
        node.floorDataFp =
            node.strategyFp + node.strategyBytes + childrenNum * node.childrenDeltaFpBytes;
      } else {
        node.floorDataFp = NO_FLOOR_DATA;
        node.strategyFp = fp + 4 + encodedOutputFpBytesMinus1;
      }
    } else {
      node.outputFp = NO_OUTPUT;
      node.strategyFp = fp + 3;
    }
  }

  /** Overwrite (and return) the incoming Node child, or null if the targetLabel was not found. */
  Node lookupChild(int targetLabel, Node parent, Node child) throws IOException {
    final int sign = parent.sign;
    if (sign == TrieBuilder.SIGN_NO_CHILDREN) {
      return null;
    }

    if (sign != TrieBuilder.SIGN_MULTI_CHILDREN) {
      // single child
      if (targetLabel != parent.minChildrenLabel) {
        return null;
      }
      child.label = targetLabel;
      load(child, parent.fp - parent.childDeltaFp);
      return child;
    }

    final long strategyBytesStartFp = parent.strategyFp;
    final int minLabel = parent.minChildrenLabel;
    final int strategyBytes = parent.strategyBytes;

    int position = -1;
    if (targetLabel == minLabel) {
      position = 0;
    } else if (targetLabel > minLabel) {
      position =
          TrieBuilder.ChildSaveStrategy.byCode(parent.childSaveStrategy)
              .lookup(targetLabel, access, strategyBytesStartFp, strategyBytes, minLabel);
    }

    if (position < 0) {
      return null;
    }

    final int bytesPerEntry = parent.childrenDeltaFpBytes;
    final long pos = strategyBytesStartFp + strategyBytes + (long) bytesPerEntry * position;
    final long fp = parent.fp - (access.readLong(pos) & BYTES_MINUS_1_MASK[bytesPerEntry - 1]);
    child.label = targetLabel;
    load(child, fp);

    return child;
  }
}
