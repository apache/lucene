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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Node16 extends Node {

  long firstChildIndex = 0L;
  long secondChildIndex = 0L;
  Node[] children = new Node[16];

  public Node16(int compressionLength) {
    super(NodeType.NODE16, compressionLength);
  }

  @Override
  public int getChildPos(byte k) {
    byte[] firstBytes = LongUtils.toBDBytes(firstChildIndex);
    if (count <= 8) {
      return Node.binarySearch(firstBytes, 0, count, k);
    } else {
      int pos = Node.binarySearch(firstBytes, 0, 8, k);
      if (pos != ILLEGAL_IDX) {
        return pos;
      } else {
        byte[] secondBytes = LongUtils.toBDBytes(secondChildIndex);
        pos = Node.binarySearch(secondBytes, 0, (count - 8), k);
        if (pos != ILLEGAL_IDX) {
          return 8 + pos;
        } else {
          return ILLEGAL_IDX;
        }
      }
    }
  }

  @Override
  public byte getChildKey(int pos) {
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
  public int getMinPos() {
    return 0;
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

  @Override
  public int getNextSmallerPos(int pos) {
    if (pos == ILLEGAL_IDX) {
      return count - 1;
    }
    pos--;
    return pos >= 0 ? pos : ILLEGAL_IDX;
  }

  /**
   * insert a child into the node with the key byte
   *
   * @param node the node16 to insert into
   * @param child the child node to be inserted
   * @param key the key byte
   * @return the adaptive changed node of the parent node16
   */
  public static Node insert(Node node, Node child, byte key) {
    Node16 currentNode16 = (Node16) node;
    if (currentNode16.count < 8) {
      //first
      byte[] bytes = LongUtils.toBDBytes(currentNode16.firstChildIndex);
      bytes[currentNode16.count] = key;
      currentNode16.firstChildIndex = LongUtils.fromBDBytes(bytes);
      currentNode16.children[currentNode16.count] = child;
      currentNode16.count++;
      return currentNode16;
    } else if (currentNode16.count < 16) {
      //second
      byte[] bytes = LongUtils.toBDBytes(currentNode16.secondChildIndex);
      bytes[currentNode16.count - 8] = key;
      currentNode16.secondChildIndex = LongUtils.fromBDBytes(bytes);
      currentNode16.children[currentNode16.count] = child;
      currentNode16.count++;
      return currentNode16;
    } else {
      Node48 node48 = new Node48(currentNode16.prefixLength);
      byte[] firstBytes = LongUtils.toBDBytes(currentNode16.firstChildIndex);
      for (int i = 0; i < 8; i++) {
        byte v = firstBytes[i];
        int unsignedIdx = Byte.toUnsignedInt(v);
        //i won't be beyond 48
        Node48.setOneByte(unsignedIdx, (byte) i, node48.childIndex);
        node48.children[i] = currentNode16.children[i];
      }
      byte[] secondBytes = LongUtils.toBDBytes(currentNode16.secondChildIndex);
      for (int i = 8; i < currentNode16.count; i++) {
        byte v = secondBytes[i - 8];
        int unsignedIdx = Byte.toUnsignedInt(v);
        //i won't be beyond 48
        Node48.setOneByte(unsignedIdx, (byte) i, node48.childIndex);
        node48.children[i] = currentNode16.children[i];
      }
      copyPrefix(currentNode16, node48);
      node48.count = currentNode16.count;
      Node freshOne = Node48.insert(node48, child, key);
      return freshOne;
    }
  }

  @Override
  public Node remove(int pos) {
    children[pos] = null;
    ByteBuffer byteBuffer = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN);
    byte[] bytes = byteBuffer.putLong(firstChildIndex).putLong(secondChildIndex).array();
    System.arraycopy(bytes, pos + 1, bytes, pos, (16 - pos - 1));
    System.arraycopy(children, pos + 1, children, pos, (16 - pos - 1));
    firstChildIndex = byteBuffer.getLong(0);
    secondChildIndex = byteBuffer.getLong(8);
    count--;
    if (count <= 3) {
      //shrink to node4
      Node4 node4 = new Node4(prefixLength);
      //copy the keys
      node4.childIndex = (int) (firstChildIndex >> 32);
      System.arraycopy(children, 0, node4.children, 0, count);
      node4.count = count;
      copyPrefix(this, node4);
      return node4;
    }
    return this;
  }
}
