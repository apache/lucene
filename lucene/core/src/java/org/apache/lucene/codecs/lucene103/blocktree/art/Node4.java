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

/** An ART node with children, the children's count limit from 1 to 4. */
public class Node4 extends Node {

  int childIndex = 0;
  Node[] children = new Node[4];

  public Node4(int compressedPrefixSize) {
    super(NodeType.NODE4, compressedPrefixSize);
  }

  @Override
  public int getChildPos(byte k) {
    for (int i = 0; i < count; i++) {
      int shiftLeftLen = (3 - i) * 8;
      byte v = (byte) (childIndex >> shiftLeftLen);
      if (v == k) {
        return i;
      }
    }
    return ILLEGAL_IDX;
  }

  @Override
  public byte getChildKey(int pos) {
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
    return pos < count ? pos : ILLEGAL_IDX;
  }

  @Override
  public int getMaxPos() {
    return count - 1;
  }

  /**
   * insert the child node into the node4 with the key byte
   *
   * @param node the node4 to insert into
   * @param childNode the child node
   * @param key the key byte
   * @return the input node4 or an adaptive generated node16
   */
  public static Node insert(Node node, Node childNode, byte key) {
    Node4 current = (Node4) node;
    if (current.count < 4) {
      // insert leaf into current node
      current.childIndex = IntegerUtil.setByte(current.childIndex, key, current.count);
      current.children[current.count] = childNode;
      current.count++;
      return current;
    } else {
      // grow to Node16
      Node16 node16 = new Node16(current.prefixLength);
      node16.count = 4;
      node16.firstChildIndex = LongUtils.initWithFirst4Byte(current.childIndex);
      System.arraycopy(current.children, 0, node16.children, 0, 4);
      copyNode(current, node16);
      Node freshOne = Node16.insert(node16, childNode, key);
      return freshOne;
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
