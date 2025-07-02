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

public class Node256 extends Node {

  Node[] children = new Node[256];
  //a helper utility field
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
  public byte getChildKey(int pos) {
    return (byte) pos;
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
  public int getMinPos() {
    for (int i = 0; i < 4; i++) {
      long longVal = bitmapMask[i];
      int v = Long.numberOfTrailingZeros(longVal);
      if (v == 64) {
        continue;
      } else {
        int res = i * 64 + v;
        return res;
      }
    }
    return ILLEGAL_IDX;
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

  @Override
  public int getNextSmallerPos(int pos) {
    if (pos == ILLEGAL_IDX) {
      pos = 256;
    }
    if (pos == 0) {
      return ILLEGAL_IDX;
    }
    pos--;
    int longPos = pos >>> 6;
    long longVal = bitmapMask[longPos] & (LONG_MASK >>> -(pos + 1));
    while (true) {
      if (longVal != 0) {
        return (longPos + 1) * 64 - 1 - Long.numberOfLeadingZeros(longVal);
      }
      if (longPos-- == 0) {
        return ILLEGAL_IDX;
      }
      longVal = bitmapMask[longPos];
    }
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

  @Override
  public Node remove(int pos) {
    this.children[pos] = null;
    int longPos = pos >>> 6;
    bitmapMask[longPos] &= ~(1L << pos);
    this.count--;
    if (this.count <= 36) {
      Node48 node48 = new Node48(this.prefixLength);
      int j = 0;
      int currentPos = ILLEGAL_IDX;
      while ((currentPos = getNextLargerPos(currentPos)) != ILLEGAL_IDX) {
        Node child = getChild(currentPos);
        node48.children[j] = child;
        Node48.setOneByte(currentPos, (byte) j, node48.childIndex);
        j++;
      }
      node48.count = (short) j;
      copyPrefix(this, node48);
      return node48;
    }
    return this;
  }
}

