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

/** An ART node with children, the children's count limit from 17 to 48. */
public class Node48 extends Node {

  // the actual byte value of childIndex content won't be beyond 48
  long[] childIndex = new long[32];
  Node[] children = new Node[48];
  static final int INDEX_SHIFT = 3; // 2^3 == BYTES_PER_LONG
  static final int POS_MASK = 0x7; // the mask to access the pos in the long for the byte
  static final byte EMPTY_VALUE = -1;
  static final long INIT_LONG_VALUE = -1L;

  public Node48(int compressedPrefixSize) {
    super(NodeType.NODE48, compressedPrefixSize);
    Arrays.fill(childIndex, INIT_LONG_VALUE);
  }

  // Return the original byte, we will calculate the real index in getChild.
  @Override
  public int getChildPos(byte indexByte) {
    // Different with other type's node, this pos is a position value(byte value) to calculate the
    // position (long position and byte position) in childIndex, the value in this position is the
    // index in
    // children.
    int unsignedIdx = Byte.toUnsignedInt(indexByte);
    int childIdx = childrenIdx(unsignedIdx, childIndex);
    if (childIdx != EMPTY_VALUE) {
      return unsignedIdx;
    }
    return ILLEGAL_IDX;
  }

  public int getChildIndex(byte k) {
    int unsignedIdx = Byte.toUnsignedInt(k);
    return childrenIdx(unsignedIdx, childIndex);
  }

  @Override
  public byte getChildIndexByte(int pos) {
    return (byte) pos;
  }

  @Override
  public Node getChild(int pos) {
    byte idx = childrenIdx(pos, childIndex);
    return children[(int) idx];
  }

  @Override
  public void replaceNode(int pos, Node freshOne) {
    byte idx = childrenIdx(pos, childIndex);
    children[(int) idx] = freshOne;
  }

  /**
   * For Node48, this return the next index byte, we can use this index byte to calculate real index
   * with #getChildIndex.
   */
  @Override
  public int getNextLargerPos(int pos) {
    if (pos == ILLEGAL_IDX) {
      pos = -1;
    }
    pos++;
    int i = pos >>> INDEX_SHIFT;
    int offset = pos & POS_MASK;
    for (; i < 32; i++) {
      long longv = childIndex[i];
      if (offset == 0) {
        if (longv == INIT_LONG_VALUE) {
          // skip over empty bytes
          pos += 8;
          continue;
        }
      }
      for (int j = offset; j <= 7; j++) {
        int shiftNum = (7 - j) * 8;
        byte v = (byte) (longv >>> shiftNum);
        if (v != EMPTY_VALUE) {
          return pos;
        }
        pos++;
      }
      offset = 0;
    }
    return ILLEGAL_IDX;
  }

  @Override
  public int getMaxPos() {
    int pos = 255;
    for (int i = 31; i >= 0; i--) {
      long longv = childIndex[i];
      if (longv == INIT_LONG_VALUE) {
        pos -= 8;
        continue;
      } else {
        for (int j = 0; j <= 7; j++) {
          byte v = (byte) (longv >>> j * 8);
          if (v != EMPTY_VALUE) {
            return pos;
          }
          pos--;
        }
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

    if (this.childrenCount < 48) {
      // insert leaf node into current node
      int pos = this.childrenCount;
      assert this.children[pos] == null;
      this.children[pos] = child;
      int unsignedByte = Byte.toUnsignedInt(indexByte);
      int longPosition = unsignedByte >>> INDEX_SHIFT;
      int bytePosition = unsignedByte & POS_MASK;
      long original = this.childIndex[longPosition];
      byte[] bytes = LongUtils.toBDBytes(original);
      bytes[bytePosition] = (byte) pos;
      this.childIndex[longPosition] = LongUtils.fromBDBytes(bytes);
      this.childrenCount++;
      return this;
    } else {
      // grow to Node256
      Node256 node256 = new Node256(this.prefixLength);
      int currentPos = ILLEGAL_IDX;
      while ((currentPos = this.getNextLargerPos(currentPos)) != ILLEGAL_IDX) {
        Node childNode = this.getChild(currentPos);
        node256.children[currentPos] = childNode;
        Node256.setBit((byte) currentPos, node256.bitmapMask);
      }
      node256.childrenCount = this.childrenCount;
      copyNode(this, node256);
      return node256.insert(child, indexByte);
    }
  }

  private static byte childrenIdx(int pos, long[] childIndex) {
    int longPos = pos >>> INDEX_SHIFT;
    int bytePos = pos & POS_MASK;
    long longv = childIndex[longPos];
    byte idx = (byte) ((longv) >>> (7 - bytePos) * 8);
    return idx;
  }

  static void setOneByte(int pos, byte v, long[] childIndex) {
    int longPos = pos >>> INDEX_SHIFT;
    int bytePos = pos & POS_MASK;
    long preVal = childIndex[longPos];
    byte[] bytes = LongUtils.toBDBytes(preVal);
    bytes[bytePos] = v;
    childIndex[longPos] = LongUtils.fromBDBytes(bytes);
  }

  @Override
  public void saveChildIndex(IndexOutput dataOutput) throws IOException {
    // TODO: It seems a regression after remove duplicate LE order.
    // TODO: Use mask can save store, but it is hard to say whether benefit to performance.
    dataOutput.writeInt(getMask());
    for (int i = 0; i < 32; i++) {
      // TODO: We can calculate mask in this loop, and write back it. But this will need to
      // implement IndexOutput#setFilePointer,
      // And write back may harm performance. Maybe we can calculate mask in when inserting node.
      if (childIndex[i] != -1) {
        dataOutput.writeLong(childIndex[i]);
      }
    }
  }

  /** Get a 32bit mask to skip -1 in childIndex. */
  private int getMask() {
    int mask = 0;
    for (int i = 0; i < 32; i++) {
      if (childIndex[i] != -1) {
        mask |= 1 << i;
      }
    }
    return mask;
  }

  @Override
  public void readChildIndex(IndexInput dataInput) throws IOException {
    final int mask = dataInput.readInt();
    for (int i = 0; i < 32; i++) {
      if (((mask >>> i) & 1) == 1) {
        childIndex[i] = dataInput.readLong();
      }
    }
  }

  @Override
  public int readChildIndex(RandomAccessInput access, long fp) throws IOException {
    final int mask = access.readInt(fp);
    int offset = 4;
    for (int i = 0; i < 32; i++) {
      if (((mask >>> i) & 1) == 1) {
        childIndex[i] = access.readLong(fp + offset);
        offset += 8;
      }
    }
    return offset;
  }

  @Override
  public void setChildren(Node[] children) {
    int step = 0;
    for (int i = 0; i < 32; i++) {
      long longVal = childIndex[i];
      if (longVal == -1) {
        continue;
      }
      for (int j = 7; j >= 0; j--) {
        byte bytePos = (byte) (longVal >>> (j * 8));
        int unsignedPos = Byte.toUnsignedInt(bytePos);
        if (bytePos != EMPTY_VALUE) {
          this.children[unsignedPos] = children[step];
          step++;
        }
      }
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

    if (obj instanceof Node48 == false) {
      return false;
    }
    if (Arrays.equals(childIndex, ((Node48) obj).childIndex) == false) {
      return false;
    }
    if (Arrays.equals(children, ((Node48) obj).children) == false) {
      return false;
    }
    return super.equals(obj);
  }
}
