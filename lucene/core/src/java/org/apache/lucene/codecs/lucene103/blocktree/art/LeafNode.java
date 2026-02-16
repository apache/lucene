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
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BytesRef;

/** An ART node without children. */
public class LeafNode extends Node {
  /**
   * constructor
   *
   * @param key input
   * @param output the corresponding container index
   */
  public LeafNode(BytesRef key, Output output) {
    super(NodeType.LEAF_NODE, 0);
    if (key != null && key.length > 0) {
      this.key = key;
    }
    this.output = output;
  }

  /**
   * insert the child node into this with the key byte
   *
   * @param childNode the child node
   * @param indexByte the key byte
   * @return the input node4 or an adaptive generated node16
   */
  @Override
  public Node insert(Node childNode, byte indexByte) {
    // This is implemented in ARTBuilder#insert(Node node, Node child, int depth).
    return null;
  }

  @Override
  public int getChildPos(byte indexByte) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte getChildIndexByte(int pos) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Node getChild(int pos) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void replaceNode(int pos, Node freshOne) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getNextLargerPos(int pos) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getMaxPos() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void saveChildIndex(IndexOutput dataOutput) throws IOException {
    // empty
  }

  @Override
  public void readChildIndex(IndexInput dataInput) throws IOException {
    // empty
  }

  @Override
  public int readChildIndex(RandomAccessInput access, long fp) throws IOException {
    return 0;
  }

  @Override
  void setChildren(Node[] children) {
    throw new UnsupportedOperationException();
  }

  @Override
  Node[] getChildren() {
    throw new UnsupportedOperationException();
  }

  /** Write leaf node to output. Save like trie. */
  @Override
  public void saveNode(IndexOutput index) throws IOException {
    int outputFpBytes = bytesRequiredVLong(this.output.fp());
    // highest 1 bit isLeaf(always 1), 2 bit keyLengthCode, 1 bit has floor, 1 bit has term, 3 bits
    // outputFpBytes.
    int keyLengthCode = 0;
    if (key != null && key.length > 0) {
      keyLengthCode = bytesRequiredVLong(key.length);
      assert keyLengthCode >= 1 && keyLengthCode <= 4;
      if (keyLengthCode == 4) {
        keyLengthCode = 3;
      }
    }
    int header =
        (outputFpBytes - 1)
            | (output.hasTerms() ? LEAF_NODE_HAS_TERMS : 0)
            | (output.floorData() != null ? LEAF_NODE_HAS_FLOOR : 0)
            | keyLengthCode << 5
            | 1 << 7;
    index.writeByte(((byte) header));
    assert this.childrenCount == 0 : "leaf node should not have children";
    assert this.prefixLength == 0 : "leaf node should not have prefix";
    // write key.
    if (key != null) {
      assert keyLengthCode > 0;
      switch (keyLengthCode) {
        case 1 -> index.writeByte((byte) key.length);
        case 2 -> index.writeShort((short) key.length);
        case 3 -> index.writeInt(key.length);
      }
      index.writeBytes(key.bytes, key.offset, key.length);
    }

    writeLongNBytes(output.fp(), outputFpBytes, index);

    assert this.output != null : "leaf nodes should have output.";
    Output output = this.output;
    if (output.floorData() != null) {
      index.writeBytes(
          output.floorData().bytes, output.floorData().offset, output.floorData().length);
    }
  }

  public static Node load(RandomAccessInput access, long fp, int header) throws IOException {
    int offset = 0;
    BytesRef key = null;
    int keyCode = (header >>> 5) & 3;
    if (keyCode > 0) {
      int keyLength = 0;
      switch (keyCode) {
        case 1 -> {
          keyLength = access.readByte(fp + offset) & (int) BYTES_MINUS_1_MASK[0];
          offset += 1;
        }
        case 2 -> {
          keyLength = access.readShort(fp + offset) & (int) BYTES_MINUS_1_MASK[1];
          offset += 2;
        }
        case 3 -> {
          keyLength = access.readInt(fp + offset);
          offset += 4;
        }
      }
      assert keyLength > 0;
      byte[] keyBytes = new byte[keyLength];
      access.readBytes(fp + offset, keyBytes, 0, keyLength);
      offset += keyLength;
      key = new BytesRef(keyBytes);
    }

    int outputFpBytes = (header & 0x07) + 1;
    long outputFP = access.readLong(fp + offset);
    offset += outputFpBytes;
    if (outputFpBytes < 8) {
      outputFP = outputFP & BYTES_MINUS_1_MASK[outputFpBytes - 1];
    }
    LeafNode leafNode = new LeafNode(key, null);
    leafNode.hasTerms = (header & LEAF_NODE_HAS_TERMS) != 0;
    leafNode.outputFp = outputFP;
    if ((header & LEAF_NODE_HAS_FLOOR) != 0) {
      leafNode.floorDataFp = fp + offset;
    } else {
      leafNode.floorDataFp = NO_FLOOR_DATA;
    }

    return leafNode;
  }
}
