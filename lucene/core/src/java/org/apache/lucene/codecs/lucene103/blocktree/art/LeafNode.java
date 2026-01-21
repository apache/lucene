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
import org.apache.lucene.util.ArrayUtil;
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
    return null;
    // This should be implemented in ARTBuilder#insert(Node node, Node child, int depth).
    //    return insert(childNode, 0);
    //    // This happens at final, we will append all sub blocks to an empty block.
    //    if (this.key == null) {
    //      Node4 node4 = new Node4(0);
    //      node4.output = this.output;
    //      node4.insert(childNode, indexByte);
    //      return node4;
    //    } else {
    //
    //      // This case childNode's prefix/key must start with this node's key, e.g.: insert 're'
    //      //into 'r'. or insert 're' into 'ra'.
    //      // TODO: Maybe we cloud ignore the above condition, and use this method replace similar
    //      // part
    //      // in ARTBuilder#insert.
    //      if (key.length > 1) {
    //        System.out.println("key.length:" + key.length);
    //      }
    //      Node4 node4 = new Node4(this.key.length);
    //      node4.output = this.output;
    //      // TODO: check key's offset, length.
    //      node4.prefix = this.key.bytes;
    //      if (node4.prefixLength > 0) {
    //        node4.prefix = new byte[node4.prefixLength];
    //        System.arraycopy(this.key.bytes, this.key.offset, node4.prefix, 0,
    // node4.prefixLength);
    //      }
    //      node4.insert(childNode, indexByte);
    //      return node4;
    //    }
  }

  /**
   * Insert the child node into this. Calculate the prefix and index byte internal. This is
   * typically used when we insert this childNode into a parent, but this node already inserted in
   * the save pos. This node and childNode have same index byte in parent, so we insert
   * childNode(without index byte) into this node.
   */
  @Override
  public Node insert(Node childNode, int depth) {
    // LEAF_NODE should be inserted in ARTBuilder#insert, we also implement here.
    // This happens at final, we will append all sub blocks to an empty block.
    if (this.key == null) {
      Node4 node4 = new Node4(0);
      node4.output = this.output;
      if (childNode.nodeType.equals(NodeType.LEAF_NODE)) {
        node4.insert(childNode, childNode.key.bytes[0]);
        updateKey(childNode, 1);
      } else {
        node4.insert(childNode, childNode.prefix[depth]);
        updatePrefix(childNode, depth + 1);
      }
      return node4;
    } else {
      // e.g.: insert 're' into 'r'. or insert 're' into 'ra'.
      // TODO: use this method replace similar part in ARTBuilder#insert.
      Node4 node4;
      if (childNode.nodeType.equals(NodeType.LEAF_NODE)) {
        final int prefixLength =
            ARTUtil.commonPrefixLength(
                this.key.bytes,
                this.key.offset,
                this.key.offset + this.key.length,
                childNode.key.bytes,
                childNode.key.offset,
                childNode.key.offset + childNode.key.length);
        assert childNode.key.length > prefixLength;

        node4 = new Node4(prefixLength);
        node4.output = this.output;
        if (prefixLength > 0) {
          node4.prefix =
              ArrayUtil.copyOfSubArray(
                  this.key.bytes, this.key.offset, this.key.offset + prefixLength);
        }
        if (this.key.length > prefixLength) {
          node4.insert(this, this.key.bytes[this.key.offset + prefixLength]);
          updateKey(this, this.key.offset + prefixLength + 1);
        }

        node4.insert(childNode, childNode.key.bytes[childNode.key.offset + prefixLength]);
        updateKey(childNode, childNode.key.offset + prefixLength + 1);
      } else {
        // TODO: childNode.prefix should have a depth(offset), similar to ARTBuilder#insert.
        final int prefixLength =
            ARTUtil.commonPrefixLength(
                this.key.bytes,
                this.key.offset,
                this.key.offset + this.key.length,
                childNode.prefix,
                depth,
                childNode.prefixLength);
        assert childNode.prefixLength - depth > prefixLength;

        node4 = new Node4(prefixLength);

        if (prefixLength > 0) {
          node4.prefix =
              ArrayUtil.copyOfSubArray(
                  this.key.bytes, this.key.offset, this.key.offset + prefixLength);
        }
        if (this.key.length > prefixLength) {
          // Keep output in leaf node.
          node4.insert(this, this.key.bytes[this.key.offset + prefixLength]);
          updateKey(this, this.key.offset + prefixLength + 1);
        } else {
          // Use leaf node's output if we hide this leaf node.
          node4.output = this.output;
        }

        node4.insert(childNode, childNode.prefix[depth + prefixLength]);
        updatePrefix(childNode, depth + prefixLength + 1);
      }

      return node4;
    }
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
  @Override
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
    // 3 bits outputFpBytes, 1 bit has term, 1 bit has floor.
    int header =
        (outputFpBytes - 1)
            | (output.hasTerms() ? LEAF_NODE_HAS_TERMS : 0)
            | (output.floorData() != null ? LEAF_NODE_HAS_FLOOR : 0);
    index.writeByte(((byte) header));
    writeLongNBytes(output.fp(), outputFpBytes, index);
    if (output.floorData() != null) {
      index.writeInt(output.floorData().length);
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
      leafNode.floorDataLen = access.readInt(fp + offset);
      offset += 4;
      leafNode.floorDataFp = fp + offset;
    } else {
      leafNode.floorDataFp = NO_FLOOR_DATA;
    }

    return leafNode;
  }
}
