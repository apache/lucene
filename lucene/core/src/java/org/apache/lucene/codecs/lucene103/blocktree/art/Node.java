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
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;

/** The whole path of a node is: prefix bytes + childIndex byte + key bytes. */
public abstract class Node {
  // TODO: move to leafNode if key just exists in leafNode?
  BytesRef key;
  Output output;

  // node type
  public NodeType nodeType;
  // length of compressed path(prefix)
  public int prefixLength;
  // the compressed path path (prefix)
  protected byte[] prefix;
  // number of non-null children, the largest value will not beyond 255
  // to benefit calculation, we keep the value as a short type
  protected short count;
  public static final int ILLEGAL_IDX = -1;

  /**
   * constructor
   *
   * @param nodeType the node type
   * @param compressedPrefixSize the prefix byte array size,less than or equal to 6
   */
  public Node(NodeType nodeType, int compressedPrefixSize) {
    this.nodeType = nodeType;
    this.prefixLength = compressedPrefixSize;
    prefix = new byte[prefixLength];
    count = 0;
  }

  /**
   * get the position of a child corresponding to the input key 'k'
   *
   * @param k a key value of the byte range
   * @return the child position corresponding to the key 'k'
   */
  public abstract int getChildPos(byte k);

  /**
   * get the child at the specified position in the node, the 'pos' range from 0 to count
   *
   * @param pos the position
   * @return a Node corresponding to the input position
   */
  public abstract Node getChild(int pos);

  /**
   * replace the position child to the fresh one
   *
   * @param pos the position
   * @param freshOne the fresh node to replace the old one
   */
  public abstract void replaceNode(int pos, Node freshOne);

  /**
   * get the next position in the node
   *
   * @param pos current position,-1 to start from the min one
   * @return the next larger byte key's position which is close to 'pos' position,-1 for end
   */
  public abstract int getNextLargerPos(int pos);

  /**
   * get the max child's position
   *
   * @return the max byte key's position
   */
  public abstract int getMaxPos();

  /**
   * Write node to output.
   *
   * @param data
   * @throws IOException
   */
  public void save(IndexOutput data) throws IOException {
    // node type.
    data.writeByte((byte) this.nodeType.ordinal());
    // children count.
    // TODO: max 255, maybe write byte.
    data.writeShort(Short.reverseBytes(this.count));
    // write prefix.
    data.writeVInt(this.prefixLength);
    if (prefixLength > 0) {
      data.writeBytes(this.prefix, 0, this.prefixLength);
    }
    // write key.
    if (key != null) {
      data.writeVInt(key.length);
      data.writeBytes(key.bytes, key.offset, key.length);
    } else {
      data.writeVInt(0);
    }

    // Write output exists flag.
    if (this.output != null) {
      Output output = this.output;
      long encodedFP = encodeFP(output);
      writeLongNBytes(encodedFP, bytesRequiredVLong(output.fp()), data);
      if (output.floorData() != null) {
        data.writeBytes(
            output.floorData().bytes, output.floorData().offset, output.floorData().length);
      }
    }

    saveChildIndex(data);
  }

  /**
   * Write childIndex to output.
   *
   * @param data
   * @throws IOException
   */
  public abstract void saveChildIndex(IndexOutput data) throws IOException;

  protected long encodeFP(Output output) {
    // TODO: where did this?
    assert output.fp() < 1L << 62;
    return (output.floorData() != null ? 1 : 0) | (output.hasTerms() ? 1 : 0) | (output.fp() << 2);
  }

  protected static int bytesRequiredVLong(long v) {
    return Long.BYTES - (Long.numberOfLeadingZeros(v | 1) >>> 3);
  }

  /**
   * Write the first (LSB order) n bytes of the given long v into the DataOutput.
   *
   * <p>This differs from writeVLong because it can write more bytes than would be needed for vLong
   * when the incoming int n is larger.
   */
  protected static void writeLongNBytes(long v, int n, DataOutput out) throws IOException {
    for (int i = 0; i < n; i++) {
      // Note that we sometimes write trailing 0 bytes here, when the incoming int n is bigger than
      // would be required for a "normal" vLong
      out.writeByte((byte) v);
      v >>>= 8;
    }
    assert v == 0;
  }

  /**
   * insert the LeafNode as a child of the current internal node
   *
   * @param current current internal node
   * @param childNode the leaf node
   * @param key the key byte reference to the child leaf node
   * @return an adaptive changed node of the input 'current' node
   */
  public static Node insertLeaf(Node current, LeafNode childNode, byte key) {
    switch (current.nodeType) {
      case NODE4:
        return Node4.insert(current, childNode, key);
      case NODE16:
        return Node16.insert(current, childNode, key);
      case NODE48:
        return Node48.insert(current, childNode, key);
      case NODE256:
        return Node256.insert(current, childNode, key);
      default:
        throw new IllegalArgumentException("Not supported node type!");
    }
  }

  /**
   * copy the prefix and output between two nodes
   *
   * @param src the source node
   * @param dst the destination node
   */
  public static void copyNode(Node src, Node dst) {
    dst.prefixLength = src.prefixLength;
    System.arraycopy(src.prefix, 0, dst.prefix, 0, src.prefixLength);
    dst.output = src.output;
  }

  /**
   * search the position of the input byte key in the node's key byte array part
   *
   * @param key the input key byte array
   * @param fromIndex inclusive
   * @param toIndex exclusive
   * @param k the target key byte value
   * @return the array offset of the target input key 'k' or -1 to not found
   */
  public static int binarySearch(byte[] key, int fromIndex, int toIndex, byte k) {
    int inputUnsignedByte = Byte.toUnsignedInt(k);
    int low = fromIndex;
    int high = toIndex - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      int midVal = Byte.toUnsignedInt(key[mid]);

      if (midVal < inputUnsignedByte) {
        low = mid + 1;
      } else if (midVal > inputUnsignedByte) {
        high = mid - 1;
      } else {
        return mid; // key found
      }
    }
    // key not found.
    return ILLEGAL_IDX;
  }
}
