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
    int i = pos >>> 3;
    int offset = pos & (8 - 1);
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
  public static Node insert(Node currentNode, Node child, byte indexByte) {
    Node48 node48 = (Node48) currentNode;
    if (node48.childrenCount < 48) {
      // insert leaf node into current node
      int pos = node48.childrenCount;
      assert node48.children[pos] == null;
      node48.children[pos] = child;
      int unsignedByte = Byte.toUnsignedInt(indexByte);
      int longPosition = unsignedByte >>> 3;
      int bytePosition = unsignedByte & (8 - 1);
      long original = node48.childIndex[longPosition];
      byte[] bytes = LongUtils.toBDBytes(original);
      bytes[bytePosition] = (byte) pos;
      node48.childIndex[longPosition] = LongUtils.fromBDBytes(bytes);
      node48.childrenCount++;
      return node48;
    } else {
      // grow to Node256
      Node256 node256 = new Node256(node48.prefixLength);
      int currentPos = ILLEGAL_IDX;
      while ((currentPos = node48.getNextLargerPos(currentPos)) != ILLEGAL_IDX) {
        Node childNode = node48.getChild(currentPos);
        node256.children[currentPos] = childNode;
        Node256.setBit((byte) currentPos, node256.bitmapMask);
      }
      node256.childrenCount = node48.childrenCount;
      copyNode(node48, node256);
      Node freshOne = Node256.insert(node256, child, indexByte);
      return freshOne;
    }
  }

  /** Insert the child node into this with the index byte. */
  @Override
  public Node insert(Node child, byte indexByte) {
    if (indexByte == 114) {
      System.out.println();
    }
    if (getChildPos(indexByte) != ILLEGAL_IDX) {
      Node oldChild = getChild(indexByte);
      Node newChild = null;
      if (child.nodeType.equals(NodeType.LEAF_NODE)) {
        assert child.key.length > 1;
        newChild = oldChild.insert(child, child.key.bytes[child.key.offset + 1]);
      } else {
        assert child.prefixLength > 1;
        newChild = oldChild.insert(child, child.prefix[1]);
        //        updateNodePrefix(child, 2);
      }
      assert newChild != null;
      replaceNode(indexByte, newChild);
      return this;
    } else {
      if (this.childrenCount < 48) {
        // insert leaf node into current node
        int pos = this.childrenCount;
        assert this.children[pos] == null;
        this.children[pos] = child;
        int unsignedByte = Byte.toUnsignedInt(indexByte);
        int longPosition = unsignedByte >>> 3;
        int bytePosition = unsignedByte & (8 - 1);
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
        Node freshOne = Node256.insert(node256, child, indexByte);
        return freshOne;
      }
    }
  }

  /**
   * Insert the child node into this. Calculate the prefix and index byte internal.
   *
   * @param childNode
   */
  @Override
  public Node insert(Node childNode) {
    return null;
  }

  private static byte childrenIdx(int pos, long[] childIndex) {
    int longPos = pos >>> 3;
    int bytePos = pos & (8 - 1);
    long longv = childIndex[longPos];
    byte idx = (byte) ((longv) >>> (7 - bytePos) * 8);
    return idx;
  }

  static void setOneByte(int pos, byte v, long[] childIndex) {
    int longPos = pos >>> 3;
    int bytePos = pos & (8 - 1);
    long preVal = childIndex[longPos];
    byte[] bytes = LongUtils.toBDBytes(preVal);
    bytes[bytePos] = v;
    childIndex[longPos] = LongUtils.fromBDBytes(bytes);
  }

  @Override
  public void saveChildIndex(IndexOutput dataOutput) throws IOException {
    for (int i = 0; i < 32; i++) {
      long longV = childIndex[i];
      // TODO: Skip -1 and write VLong.
      //      dataOutput.writeVLong(longV);
      dataOutput.writeLong(longV);
    }
  }

  @Override
  public void readChildIndex(IndexInput dataInput) throws IOException {
    for (int i = 0; i < 32; i++) {
      childIndex[i] = dataInput.readLong();
    }
  }

  @Override
  public void readChildIndex(RandomAccessInput access, long fp) throws IOException {
    int offset = 0;
    for (int i = 0; i < 32; i++) {
      childIndex[i] = access.readLong(fp + offset);
      offset += 8;
    }
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
