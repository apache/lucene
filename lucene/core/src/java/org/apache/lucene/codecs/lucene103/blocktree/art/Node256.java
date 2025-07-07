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
package org.apache.lucene.codecs.lucene103.blocktree.art;

import static java.lang.Long.numberOfTrailingZeros;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

public class Node256 extends Node {

  Node[] children = new Node[256];
  // a helper utility field
  long[] bitmapMask = new long[4];
  private static final long LONG_MASK = 0xffffffffffffffffL;

  public Node256(int compressedPrefixSize) {
    super(NodeType.NODE256, compressedPrefixSize);
  }

  @Override
  public int getChildPos(byte k) {
    int pos = Byte.toUnsignedInt(k);
    if (children[pos] != null) {
      return pos;
    }
    return ILLEGAL_IDX;
  }

  @Override
  public Node getChild(int pos) {
    return children[pos];
  }

  @Override
  public void replaceNode(int pos, Node freshOne) {
    children[pos] = freshOne;
  }

  @Override
  public int getNextLargerPos(int pos) {
    if (pos == ILLEGAL_IDX) {
      pos = 0;
    } else {
      pos++;
    }
    int longPos = pos >> 6;
    if (longPos >= 4) {
      return ILLEGAL_IDX;
    }
    long longVal = bitmapMask[longPos] & (LONG_MASK << pos);
    while (true) {
      if (longVal != 0) {
        return (longPos * 64) + Long.numberOfTrailingZeros(longVal);
      }
      if (++longPos == 4) {
        return ILLEGAL_IDX;
      }
      longVal = bitmapMask[longPos];
    }
  }

  @Override
  public int getMaxPos() {
    for (int i = 3; i >= 0; i--) {
      long longVal = bitmapMask[i];
      int v = Long.numberOfLeadingZeros(longVal);
      if (v == 64) {
        continue;
      } else {
        int res = i * 64 + (63 - v);
        return res;
      }
    }
    return ILLEGAL_IDX;
  }

  /**
   * insert the child node into the node256 node with the key byte
   *
   * @param currentNode the node256
   * @param child the child node
   * @param key the key byte
   * @return the node256 node
   */
  public static Node256 insert(Node currentNode, Node child, byte key) {
    Node256 node256 = (Node256) currentNode;
    node256.count++;
    int i = Byte.toUnsignedInt(key);
    node256.children[i] = child;
    setBit(key, node256.bitmapMask);
    return node256;
  }

  static void setBit(byte key, long[] bitmapMask) {
    int i = Byte.toUnsignedInt(key);
    int longIdx = i >>> 6;
    final long previous = bitmapMask[longIdx];
    long newVal = previous | (1L << i);
    bitmapMask[longIdx] = newVal;
  }

  public void saveChildIndex(IndexOutput dataOutput) throws IOException {
    // little endian
    for (long longV : bitmapMask) {
      dataOutput.writeLong(Long.reverseBytes(longV));
    }
  }

  @Override
  public void readChildIndex(IndexInput dataInput) throws IOException {
    for (int i = 0; i < bitmapMask.length; i++) {
      bitmapMask[i] = Long.reverseBytes(dataInput.readLong());
    }
  }

  @Override
  public void setChildren(Node[] children) {
    if (children.length == this.children.length) {
      this.children = children;
      return;
    } else if (output != null && children.length + 1 == this.children.length) {
      System.arraycopy(children, 0, this.children, 1, children.length);
      return;
    }

    int offset = 0;
    int x = 0;
    for (long longv : bitmapMask) {
      int w = Long.bitCount(longv);
      for (int i = 0; i < w; i++) {
        int pos = x * 64 + numberOfTrailingZeros(longv);
        this.children[pos] = children[offset + i];
        longv &= (longv - 1);
      }
      offset += w;
      x++;
    }
  }

  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj instanceof Node256 == false) {
      return false;
    }
    if (Arrays.equals(bitmapMask, ((Node256) obj).bitmapMask) == false) {
      return false;
    }
    if (Arrays.equals(children, ((Node256) obj).children) == false) {
      return false;
    }
    return super.equals(obj);
  }
}
