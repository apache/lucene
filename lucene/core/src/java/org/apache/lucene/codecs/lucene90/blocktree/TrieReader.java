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
package org.apache.lucene.codecs.lucene90.blocktree;

import java.io.IOException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;

class TrieReader {

  private static final long NO_OUTPUT = -1;
  private static final long NO_FLOOR_DATA = -1;
  private static final long[] BYTES_MASK =
      new long[] {
        0L,
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
    private long childFp;

    // multi children
    private long positionFp;
    private int childrenStrategy;
    private int positionBytes;
    private int childrenFpBytes;

    // common vars
    private long fp;
    private int childrenNum;
    private int minChildrenLabel;
    int label;

    // output vars
    long outputFp;
    boolean hasTerms;
    long floorDataFp; // only makes sense when outputFp != NO_OUTPUT;

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
    long termLong = access.readLong(fp);
    int term = (int) termLong;
    int sign = term & 0x03;

    if (sign == Trie.SIGN_NO_CHILDREN) {

      // [n bytes] floor data
      // [n bytes] output fp
      // [1bit] nothing | [1bit] has floor | [1bit] has terms | [3bit] output fp bytes | [2bit]
      // sign

      node.childrenNum = 0;
      int fpBytes = ((term >>> 2) & 0x07) + 1;
      node.outputFp =
          fpBytes <= 7 ? (termLong >>> 8) & bytesAsMask(fpBytes) : access.readLong(fp + 1);
      node.hasTerms = (term & 0x20) != 0;
      if ((term & 0x40) != 0) {
        node.floorDataFp = fp + 1 + fpBytes;
      } else {
        node.floorDataFp = NO_FLOOR_DATA;
      }

      return;
    }

    if (sign == Trie.SIGN_SINGLE_CHILDREN_WITHOUT_OUTPUT
        || sign == Trie.SIGN_SINGLE_CHILDREN_WITH_OUTPUT) {

      // [n bytes] floor data
      // [n bytes] encoded output fp | [n bytes] child fp | [1 byte] label
      // [3bit] encoded output fp bytes | [3bit] child fp bytes | [2bit] sign

      node.childrenNum = 1;
      int childFpBytes = ((term >>> 2) & 0x07) + 1;
      int encodedOutputFpBytes = ((term >>> 5) & 0x07) + 1;
      long l = childFpBytes <= 6 ? termLong >>> 16 : access.readLong(fp + 2);
      node.childFp = l & bytesAsMask(childFpBytes);
      node.minChildrenLabel = (term >>> 8) & 0xFF;

      if (sign == Trie.SIGN_SINGLE_CHILDREN_WITHOUT_OUTPUT) {
        node.outputFp = NO_OUTPUT;
      } else {
        long offset = fp + childFpBytes + 2;
        long encodedFp = access.readLong(offset) & bytesAsMask(encodedOutputFpBytes);
        node.outputFp = encodedFp >>> 2;
        node.hasTerms = (encodedFp & 0x02L) != 0;
        if ((encodedFp & 0x01L) != 0) {
          node.floorDataFp = offset + encodedOutputFpBytes;
        } else {
          node.floorDataFp = NO_FLOOR_DATA;
        }
      }

      return;
    }

    assert sign == Trie.SIGN_MULTI_CHILDREN;

    // [n bytes] floor data
    // [n bytes] children fps | [n bytes] position data
    // [n bytes] encoded output fp | [1 byte] children count | [1 byte] label
    // [5bit] position bytes | 2bit children strategy | [3bit] encoded output fp bytes
    // [1bit] has output | [3bit] children fp bytes | [2bit] sign

    node.childrenFpBytes = ((term >>> 2) & 0x07) + 1;
    boolean hasOutput = (term & 0x20) > 0;
    node.childrenStrategy = (term >>> 9) & 0x03;
    node.positionBytes = ((term >>> 11) & 0x1F) + 1;
    node.minChildrenLabel = (term >>> 16) & 0xFF;
    node.childrenNum = ((term >>> 24) & 0xFF) + 1;

    if (hasOutput) {
      int encodedOutputFpBytes = ((term >>> 6) & 0x07) + 1;
      long l = encodedOutputFpBytes <= 4 ? termLong >>> 32 : access.readLong(fp + 4);
      long encodedFp = l & bytesAsMask(encodedOutputFpBytes);
      node.outputFp = encodedFp >>> 2;
      node.hasTerms = (encodedFp & 0x02L) != 0;
      node.positionFp = fp + 4 + encodedOutputFpBytes;
      if ((encodedFp & 0x01L) != 0) {
        node.floorDataFp =
            node.positionFp + node.positionBytes + (long) node.childrenNum * node.childrenFpBytes;
      } else {
        node.floorDataFp = NO_FLOOR_DATA;
      }
    } else {
      node.outputFp = NO_OUTPUT;
      node.positionFp = fp + 4;
    }
  }

  private static long bytesAsMask(int bytes) {
    assert bytes > 0 && bytes <= 8 : "" + bytes;
    return BYTES_MASK[bytes];
  }

  Node lookupChild(int targetLabel, Node parent, Node child) throws IOException {
    if (parent.childrenNum == 0) {
      return null;
    }

    if (parent.childrenNum == 1) {
      if (targetLabel != parent.minChildrenLabel) {
        return null;
      }
      child.label = targetLabel;
      load(child, parent.fp - parent.childFp);
      return child;
    }

    final long positionBytesStartFp = parent.positionFp;
    final int minLabel = parent.minChildrenLabel;
    final int positionBytes = parent.positionBytes;

    int position;
    if (targetLabel < minLabel) {
      position = -1;
    } else if (targetLabel == minLabel) {
      position = 0;
    } else {
      position =
          Trie.PositionStrategy.byCode(parent.childrenStrategy)
              .lookup(targetLabel, access, positionBytesStartFp, positionBytes, minLabel);
    }

    if (position < 0) {
      return null;
    }

    final int codeBytes = parent.childrenFpBytes;
    final long pos = positionBytesStartFp + positionBytes + (long) codeBytes * position;
    final long fp = parent.fp - (access.readLong(pos) & bytesAsMask(codeBytes));
    child.label = targetLabel;
    load(child, fp);

    return child;
  }
}
