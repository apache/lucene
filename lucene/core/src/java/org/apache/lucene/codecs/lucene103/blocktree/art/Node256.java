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

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;

/** An ART node with children, the children's count limit from 49 to 256. */
public class Node256 extends Node {

  // For testing.
  public Node[] children = new Node[256];
  // a helper utility field
  long[] bitmapMask = new long[4];
  private static final long LONG_MASK = 0xffffffffffffffffL;
  // Cache number of zeros for the first 3 bit mask.
  int[] cache = new int[] {-1, -1, -1};

  public Node256(int compressedPrefixSize) {
    super(NodeType.NODE256, compressedPrefixSize);
  }

  @Override
  public int getChildPos(byte indexByte) {
    int pos = Byte.toUnsignedInt(indexByte);
    int longIdx = pos >> 6;

    final long longVal = bitmapMask[longIdx];
    if (((longVal >> pos) & 1) == 1L) {
      return pos;
    }
    return ILLEGAL_IDX;
  }

  //  @Override
  //  public int getChildPos(byte k) {
  //    int pos = Byte.toUnsignedInt(k);
  //    if (children[pos] != null) {
  //      return pos;
  //    }
  //    return ILLEGAL_IDX;
  //  }

  @Override
  public byte getChildIndexByte(int pos) {
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

  // Returns the number of null child from 0 to this pos. this is useful when we need to calculate
  // child's delta fp position to load child.
  public int numberOfNullChildren(int pos) {
    int longPos = pos >> 6;
    if (longPos >= 4) {
      return ILLEGAL_IDX;
    }

    int numberOfZeros = 0;
    for (int i = 0; i < longPos; i++) {
      if (cache[i] == -1) {
        // calculate the whole long bits' number of zero.
        long wholeMask = bitmapMask[i];
        int x = numberOfZeros((int) (wholeMask >>> 32)) + numberOfZeros((int) wholeMask);
        cache[i] = x;
      }
      numberOfZeros += cache[i];
    }

    long currentMask = bitmapMask[longPos] | (LONG_MASK << pos);
    numberOfZeros += numberOfZeros((int) (currentMask >>> 32)) + numberOfZeros((int) currentMask);
    return numberOfZeros;
  }

  // Returns the number of zero bits of i.
  // https://www.baeldung.com/cs/integer-bitcount
  public int numberOfZeros(int i) {
    // i equals 0 is more than unusual for long's low 32 bits.
    if (i == 0) return 32;
    i = (i & 0x55555555) + ((i >> 1) & 0x55555555);
    i = (i & 0x33333333) + ((i >> 2) & 0x33333333);
    i = (i & 0x0F0F0F0F) + ((i >> 4) & 0x0F0F0F0F);
    i = (i & 0x00FF00FF) + ((i >> 8) & 0x00FF00FF);
    i = (i & 0x0000FFFF) + ((i >> 16) & 0x0000FFFF);
    return 32 - i;
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

  /** Insert the child node into this with the index byte. */
  @Override
  public Node insert(Node child, byte indexByte) {
    // There already is a child exits in pos is implemented in ARTBuilder#insert(Node node, Node
    // child, int depth).
    assert getChildPos(indexByte) == ILLEGAL_IDX;

    this.childrenCount++;
    int i = Byte.toUnsignedInt(indexByte);
    this.children[i] = child;
    setBit(indexByte, this.bitmapMask);
    return this;
  }

  static void setBit(byte indexByte, long[] bitmapMask) {
    int i = Byte.toUnsignedInt(indexByte);
    int longIdx = i >>> 6;
    final long previous = bitmapMask[longIdx];
    long newVal = previous | (1L << i);
    bitmapMask[longIdx] = newVal;
  }

  @Override
  public void saveChildIndex(IndexOutput dataOutput) throws IOException {
    for (long longV : bitmapMask) {
      // TODO: Skip -1 like node48.
      dataOutput.writeLong(longV);
    }
  }

  @Override
  public void readChildIndex(IndexInput dataInput) throws IOException {
    for (int i = 0; i < bitmapMask.length; i++) {
      bitmapMask[i] = dataInput.readLong();
    }
  }

  @Override
  public int readChildIndex(RandomAccessInput access, long fp) throws IOException {
    int offset = 0;
    for (int i = 0; i < bitmapMask.length; i++) {
      bitmapMask[i] = access.readLong(fp + offset);
      offset += 8;
    }
    return offset;
  }

  @Override
  public void setChildren(Node[] children) {
    if (children.length == this.children.length) {
      this.children = children;
      return;
    }

    int offset = 0;
    int x = 0;
    for (long longv : bitmapMask) {
      int w = Long.bitCount(longv);
      for (int i = 0; i < w; i++) {
        int pos = x * 64 + Long.numberOfTrailingZeros(longv);
        this.children[pos] = children[offset + i];
        longv &= (longv - 1);
      }
      offset += w;
      x++;
    }
  }

  @Override
  Node[] getChildren() {
    return children;
  }

  @Override
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
