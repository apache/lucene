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

/** An ART node with children, the children's count limit from 5 to 16. */
public class Node16 extends Node {

  long firstChildIndex = 0L;
  long secondChildIndex = 0L;
  Node[] children = new Node[16];

  public Node16(int compressionLength) {
    super(NodeType.NODE16, compressionLength);
  }

  @Override
  public int getChildPos(byte indexByte) {
    byte[] firstBytes = LongUtils.toBDBytes(firstChildIndex);
    if (childrenCount <= 8) {
      return Node.binarySearch(firstBytes, 0, childrenCount, indexByte);
    } else {
      int pos = Node.binarySearch(firstBytes, 0, 8, indexByte);
      if (pos != ILLEGAL_IDX) {
        return pos;
      } else {
        byte[] secondBytes = LongUtils.toBDBytes(secondChildIndex);
        pos = Node.binarySearch(secondBytes, 0, (childrenCount - 8), indexByte);
        if (pos != ILLEGAL_IDX) {
          return 8 + pos;
        } else {
          return ILLEGAL_IDX;
        }
      }
    }
  }

  @Override
  public byte getChildIndexByte(int pos) {
    int posInLong;
    if (pos <= 7) {
      posInLong = pos;
      byte[] firstBytes = LongUtils.toBDBytes(firstChildIndex);
      return firstBytes[posInLong];
    } else {
      posInLong = pos - 8;
      byte[] secondBytes = LongUtils.toBDBytes(secondChildIndex);
      return secondBytes[posInLong];
    }
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
      return 0;
    }
    pos++;
    return pos < childrenCount ? pos : ILLEGAL_IDX;
  }

  @Override
  public int getMaxPos() {
    return childrenCount - 1;
  }

  /** Insert the child node into this with the index byte. */
  @Override
  public Node insert(Node childNode, byte indexByte) {
    // There already is a child exits in pos is implemented in ARTBuilder#insert(Node node, Node
    // child, int depth).
    assert getChildPos(indexByte) == ILLEGAL_IDX;

    if (this.childrenCount < 8) {
      // first
      byte[] bytes = LongUtils.toBDBytes(this.firstChildIndex);
      bytes[this.childrenCount] = indexByte;
      this.firstChildIndex = LongUtils.fromBDBytes(bytes);
      this.children[this.childrenCount] = childNode;
      this.childrenCount++;
      return this;
    } else if (this.childrenCount < 16) {
      // second
      byte[] bytes = LongUtils.toBDBytes(this.secondChildIndex);
      bytes[this.childrenCount - 8] = indexByte;
      this.secondChildIndex = LongUtils.fromBDBytes(bytes);
      this.children[this.childrenCount] = childNode;
      this.childrenCount++;
      return this;
    } else {
      Node48 node48 = new Node48(this.prefixLength);
      byte[] firstBytes = LongUtils.toBDBytes(this.firstChildIndex);
      for (int i = 0; i < 8; i++) {
        byte v = firstBytes[i];
        int unsignedIdx = Byte.toUnsignedInt(v);
        // i won't be beyond 48
        Node48.setOneByte(unsignedIdx, (byte) i, node48.childIndex);
        node48.children[i] = this.children[i];
      }
      byte[] secondBytes = LongUtils.toBDBytes(this.secondChildIndex);
      for (int i = 8; i < this.childrenCount; i++) {
        byte v = secondBytes[i - 8];
        int unsignedIdx = Byte.toUnsignedInt(v);
        // i won't be beyond 48
        Node48.setOneByte(unsignedIdx, (byte) i, node48.childIndex);
        node48.children[i] = this.children[i];
      }
      copyNode(this, node48);
      node48.childrenCount = this.childrenCount;
      return node48.insert(childNode, indexByte);
    }
  }

  @Override
  public void saveChildIndex(IndexOutput dataOutput) throws IOException {
    dataOutput.writeLong(firstChildIndex);
    if (childrenCount > 8) {
      dataOutput.writeLong(secondChildIndex);
    }
  }

  @Override
  public void readChildIndex(IndexInput dataInput) throws IOException {
    firstChildIndex = dataInput.readLong();
    if (childrenCount > 8) {
      secondChildIndex = dataInput.readLong();
    }
  }

  @Override
  public int readChildIndex(RandomAccessInput access, long fp) throws IOException {
    int offset = 0;
    firstChildIndex = access.readLong(fp);
    offset += 8;
    if (childrenCount > 8) {
      secondChildIndex = access.readLong(fp + 8);
      offset += 8;
    }
    return offset;
  }

  @Override
  public void setChildren(Node[] children) {
    System.arraycopy(children, 0, this.children, 0, children.length);
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

    if (obj instanceof Node16 == false) {
      return false;
    }
    if (firstChildIndex != ((Node16) obj).firstChildIndex) {
      return false;
    }
    if (secondChildIndex != ((Node16) obj).secondChildIndex) {
      return false;
    }
    if (Arrays.equals(children, ((Node16) obj).children) == false) {
      return false;
    }
    return super.equals(obj);
  }
}
