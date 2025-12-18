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

  @Override
  public int getChildPos(byte k) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte getChildKey(int pos) {
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
    // TODO: save key
  }

  @Override
  public void readChildIndex(IndexInput dataInput) throws IOException {
    // empty
  }

  @Override
  public void readChildIndex(RandomAccessInput access, long fp) throws IOException {
    // empty
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
  public void saveNode(IndexOutput index) throws IOException {
    // Node type.
    index.writeByte((byte) this.nodeType.ordinal());
    assert this.childrenCount == 0 : "leaf node should not have children";
    assert this.prefixLength == 0 : "leaf node should not have prefix";
    // write key.
    if (key != null) {
      index.writeInt(key.length);
      index.writeBytes(key.bytes, key.offset, key.length);
    } else {
      index.writeInt(0);
    }

    assert this.output != null : "leaf nodes should have output.";
    Output output = this.output;
    int outputFpBytes = bytesRequiredVLong(this.output.fp());
    // TODO: save floor data length.
    int header =
        (outputFpBytes - 1)
            | (output.hasTerms() ? LEAF_NODE_HAS_TERMS : 0)
            | (output.floorData() != null ? LEAF_NODE_HAS_FLOOR : 0);
    index.writeByte(((byte) header));
    writeLongNBytes(output.fp(), outputFpBytes, index);
    if (output.floorData() != null) {
      index.writeBytes(
          output.floorData().bytes, output.floorData().offset, output.floorData().length);
    }
  }

  public static Node load(RandomAccessInput access, long fp) throws IOException {
    // TODO: Impl write/read VInt like DataInput#readVInt.
    // from fp: 1 byte nodeType, 4 byte keyLength, n bytes key, 1 byte header(1 bit has floor, 1 bit
    // has terms,
    // 3 bit outputFpBytes - 1), n bytes outputFp, n bytes floorData
    // TODO: compress nodeType, header to 1 byte or keyLength.
    // We read node type in Node.
    int offset = 1;
    int keyLength = access.readInt(fp + offset);
    offset += 4;
    BytesRef key = null;
    if (keyLength > 0) {
      byte[] keyBytes = new byte[keyLength];
      access.readBytes(fp + offset, keyBytes, 0, keyLength);
      offset += keyLength;
      key = new BytesRef(keyBytes);
    }

    int header = access.readByte(fp + offset);
    offset += 1;
    int outputFpBytes = (header & 0x07) + 1;
    // TODO: If EOF, we can readLongFromNBytes.
    long outputFP = access.readLong(fp + offset);
    offset += outputFpBytes;
    if (outputFpBytes < 8) {
      outputFP = outputFP & BYTES_MINUS_1_MASK[outputFpBytes - 1];
    }
    // TODO: merge output, outputFP.
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
