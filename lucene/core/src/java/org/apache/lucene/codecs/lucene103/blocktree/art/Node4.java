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

/** An ART node with children, the children's count limit from 1 to 4. */
public class Node4 extends Node {

  int childIndex = 0;
  Node[] children = new Node[4];

  public Node4(int compressedPrefixSize) {
    super(NodeType.NODE4, compressedPrefixSize);
  }

  @Override
  public int getChildPos(byte indexByte) {
    for (int i = 0; i < childrenCount; i++) {
      int shiftLeftLen = (3 - i) * 8;
      byte v = (byte) (childIndex >> shiftLeftLen);
      if (v == indexByte) {
        return i;
      }
    }
    return ILLEGAL_IDX;
  }

  @Override
  public byte getChildIndexByte(int pos) {
    int shiftLeftLen = (3 - pos) * 8;
    byte v = (byte) (childIndex >> shiftLeftLen);
    return v;
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

    if (this.childrenCount < 4) {
      // insert leaf into current node
      this.childIndex = IntegerUtil.setByte(this.childIndex, indexByte, this.childrenCount);
      this.children[this.childrenCount] = childNode;
      this.childrenCount++;
      return this;
    } else {
      // grow to Node16
      Node16 node16 = new Node16(this.prefixLength);
      node16.childrenCount = 4;
      node16.firstChildIndex = LongUtils.initWithFirst4Byte(this.childIndex);
      System.arraycopy(this.children, 0, node16.children, 0, 4);
      copyNode(this, node16);
      return node16.insert(childNode, indexByte);
    }
  }

  @Override
  public void saveChildIndex(IndexOutput dataOutput) throws IOException {
    dataOutput.writeInt(childIndex);
  }

  @Override
  public void readChildIndex(IndexInput dataInput) throws IOException {
    childIndex = dataInput.readInt();
  }

  @Override
  public int readChildIndex(RandomAccessInput access, long fp) throws IOException {
    childIndex = access.readInt(fp);
    return 4;
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

    if (obj instanceof Node4 == false) {
      return false;
    }
    if (childIndex != ((Node4) obj).childIndex) {
      return false;
    }
    if (Arrays.equals(children, ((Node4) obj).children) == false) {
      return false;
    }
    return super.equals(obj);
  }
}
