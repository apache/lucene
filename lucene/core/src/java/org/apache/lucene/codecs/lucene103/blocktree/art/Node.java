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
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BytesRef;

/**
 * The whole path of a node is: prefix bytes + childIndex byte + key bytes. Non leafNode: prefix
 * (allow null) + childIndex. LeafNode: key (allow null).
 */
public abstract class Node {
  // we save one byte for node type, this sign is needless.
  static final int SIGN_NO_CHILDREN = 0x00;
  static final int SIGN_SINGLE_CHILD_WITH_OUTPUT = 0x01;
  static final int SIGN_SINGLE_CHILD_WITHOUT_OUTPUT = 0x02;
  static final int SIGN_MULTI_CHILDREN = 0x03;

  static final int LEAF_NODE_HAS_TERMS = 1 << 3;
  static final int LEAF_NODE_HAS_FLOOR = 1 << 4;
  static final int NON_LEAF_NODE_HAS_OUTPUT = 1 << 3;
  static final long NON_LEAF_NODE_HAS_TERMS = 1L << 1;
  static final long NON_LEAF_NODE_HAS_FLOOR = 1L << 0;

  static final long[] BYTES_MINUS_1_MASK =
      new long[] {
        0xFFL,
        0xFFFFL,
        0xFFFFFFL,
        0xFFFFFFFFL,
        0xFFFFFFFFFFL,
        0xFFFFFFFFFFFFL,
        0xFFFFFFFFFFFFFFL,
        0xFFFFFFFFFFFFFFFFL
      };

  static final long NO_OUTPUT = -1;
  static final long NO_FLOOR_DATA = -1;

  // Save fp for reader
  // The file pointer point to where the node saved. -1 means the node has not been saved.
  // We just need write root node's fp, and children's fps to walk index.
  long fp = -1;
  long outputFp = -1;
  boolean hasTerms;
  long floorDataFp = -1;
  int floorDataLen;
  int childrenDeltaFpBytes;
  long childrenDeltaFpStart;

  // The latest child that have been saved. null means no child has been saved.
  int savedChildPos = -1;

  // TODO: move to leafNode if key just exists in leafNode?
  // TODO: rename to suffix.
  BytesRef key;
  Output output;

  static final int HAS_OUTPUT = 1;
  static final int HAS_FLOOR = 1 << 1;
  static final int HAS_TERMS = 1 << 2;

  // node type
  public NodeType nodeType;
  // length of compressed path(prefix)
  public int prefixLength;
  // the compressed path path (prefix)
  protected byte[] prefix;
  // number of non-null children, the largest value will not beyond 255
  // to benefit calculation,we keep the value as a short type
  protected short childrenCount;
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
    childrenCount = 0;
    if (prefixLength > 0) {
      prefix = new byte[prefixLength];
    }
  }

  /** Return node's output. */
  public Output getOutput() {
    return output;
  }

  /**
   * get the position of a child corresponding to the input key 'k'
   *
   * @param k a key value of the byte range
   * @return the child position corresponding to the key 'k'
   */
  public abstract int getChildPos(byte k);

  /**
   * get the corresponding key byte of the requested position
   *
   * @param pos the position
   * @return the corresponding key byte
   */
  public abstract byte getChildKey(int pos);

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

  /** Read node from data input. */
  public static Node read(IndexInput dataInput) throws IOException {
    // Node type.
    int nodeTypeOrdinal = dataInput.readByte();
    // Children count.
    short count = Short.reverseBytes(dataInput.readShort());
    // Prefix.
    int prefixLength = dataInput.readVInt();
    byte[] prefix = null;
    if (prefixLength > 0) {
      prefix = new byte[prefixLength];
      dataInput.readBytes(prefix, 0, prefixLength);
    }
    // Key.
    int keyLength = dataInput.readVInt();
    BytesRef key = null;
    if (keyLength > 0) {
      byte[] keyBytes = new byte[keyLength];
      dataInput.readBytes(keyBytes, 0, keyLength);
      key = new BytesRef(keyBytes);
    }
    // Output.
    Output output = null;
    byte outputFlag = dataInput.readByte();
    if ((outputFlag & HAS_OUTPUT) != 0) {
      long fp = dataInput.readVLong();
      boolean hasTerms = (outputFlag & HAS_TERMS) != 0;
      BytesRef floorData = null;
      if ((outputFlag & HAS_FLOOR) != 0) {
        int floorDataLength = dataInput.readVInt();
        byte[] bytes = new byte[floorDataLength];
        dataInput.readBytes(bytes, 0, floorDataLength);
        floorData = new BytesRef(bytes);
      }
      output = new Output(fp, hasTerms, floorData);
    }

    Node node;
    if (nodeTypeOrdinal == NodeType.NODE4.ordinal()) {
      node = new Node4(prefixLength);
    } else if (nodeTypeOrdinal == NodeType.NODE16.ordinal()) {
      node = new Node16(prefixLength);
    } else if (nodeTypeOrdinal == NodeType.NODE48.ordinal()) {
      node = new Node48(prefixLength);
    } else if (nodeTypeOrdinal == NodeType.NODE256.ordinal()) {
      node = new Node256(prefixLength);
    } else if (nodeTypeOrdinal == NodeType.LEAF_NODE.ordinal()) {
      node = new LeafNode(key, output);
    } else {
      throw new IOException("read error: bad nodeTypeOrdinal");
    }

    node.output = output;
    node.key = key;
    node.prefix = prefix;
    node.childrenCount = count;

    node.readChildIndex(dataInput);
    return node;
  }

  /** Write node to output. */
  public void save(IndexOutput data) throws IOException {
    // Node type.
    data.writeByte((byte) this.nodeType.ordinal());
    // TODO: Only save count, prefix for non-leaf node. Only save key for leaf node.
    // Children count.
    data.writeShort(Short.reverseBytes(this.childrenCount));
    // Write prefix.
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

    int outputFlag = 0;
    if (this.output != null) {
      outputFlag = outputFlag | HAS_OUTPUT;
      if (this.output.floorData() != null) {
        outputFlag = outputFlag | HAS_FLOOR;
      }
      if (this.output.hasTerms()) {
        outputFlag = outputFlag | HAS_TERMS;
      }
    }

    data.writeByte((byte) outputFlag);
    // Write output exists flag.
    if (this.output != null) {
      Output output = this.output;
      // TODO: writeLongNBytes like TrieBuilder.
      data.writeVLong(output.fp());
      if (output.floorData() != null) {
        data.writeVInt(output.floorData().length);
        data.writeBytes(
            output.floorData().bytes, output.floorData().offset, output.floorData().length);
      }
    }

    saveChildIndex(data);
  }

  /**
   * Write non leaf node to output. TODO: Move this to a non leaf node, or move leaf node into this.
   */
  public void saveNode(IndexOutput index) throws IOException {
    // Node type.
    index.writeByte((byte) this.nodeType.ordinal());
    // Only save count, prefix for non-leaf node. Only save key for leaf node.
    // Children count.
    index.writeShort(Short.reverseBytes(this.childrenCount));
    // Write prefix.
    // TODO: Impl write/read VInt like DataInput#readVInt.
    index.writeInt(this.prefixLength);
    if (prefixLength > 0) {
      index.writeBytes(this.prefix, 0, this.prefixLength);
    }

    // Get first child to compute max delta fp between parent and children.
    // Children fps are in order, so the first child's fp is min, then delta is max.

    // For node48, its position is the key byte, we use the first child in children.
    Node child;
    if (nodeType.equals(NodeType.NODE48)) {
      child = getChildren()[0];
    } else {
      child = getChild(getNextLargerPos(Node.ILLEGAL_IDX));
    }
    assert child.fp > -1 : "child should written before parent";
    long maxChildDeltaFp = fp - child.fp;
    assert maxChildDeltaFp > 0 : "parent always written after all children";

    int childrenFpBytes = bytesRequiredVLong(maxChildDeltaFp);
    int encodedOutputFpBytes = output == null ? 1 : bytesRequiredVLong(output.fp() << 2);
    // 3 bit encodedOutputFpBytes - 1, 1 bit has output, 3bit childrenFpBytes - 1
    int header =
        (childrenFpBytes - 1)
            | (output != null ? NON_LEAF_NODE_HAS_OUTPUT : 0)
            | ((encodedOutputFpBytes - 1) << 4);

    index.writeByte((byte) header);

    if (output != null) {
      long encodedFp = encodeFP(output);
      writeLongNBytes(encodedFp, encodedOutputFpBytes, index);
    }

    // Write children's delta fps from first one.
    // For node48, we should write fps by iterate children[], because its getNextLargerPos
    // returns next byte,
    // We can use getChildIndex(nextPos) to calculate real index in children when load child.
    if (nodeType.equals(NodeType.NODE48)) {
      Node[] children = getChildren();
      for (int i = 0; i < childrenCount; i++) {
        child = children[i];
        assert child != null;
        assert fp > child.fp : "parent always written after all children";
        writeLongNBytes(fp - child.fp, childrenFpBytes, index);
      }
    } else {
      int nextPos = Node.ILLEGAL_IDX;
      while ((nextPos = getNextLargerPos(nextPos)) != Node.ILLEGAL_IDX) {
        child = getChild(nextPos);
        assert child != null;
        assert fp > child.fp : "parent always written after all children";
        writeLongNBytes(fp - child.fp, childrenFpBytes, index);
      }
    }

    // write floor data
    // We can use node's fp, childrenCount * childrenFpBytes to calculate floorData's fp.
    if (output != null && output.floorData() != null) {
      BytesRef floorData = output.floorData();
      // We need floorData.length to calculate ChildIndex fp.
      index.writeInt(floorData.length);
      index.writeBytes(floorData.bytes, floorData.offset, floorData.length);
    }

    saveChildIndex(index);
  }

  /** Read node from data input. */
  public static Node load(RandomAccessInput access, long fp) throws IOException {
    // Node type.
    int offset = 0;
    int nodeTypeOrdinal = access.readByte(fp + offset);

    assert nodeTypeOrdinal >= 0 && nodeTypeOrdinal <= 4 : "Wrong nodeTypeOrdinal.";

    offset += 1;
    if (nodeTypeOrdinal == NodeType.LEAF_NODE.ordinal()) {
      // TODO: adjust this call architecture.
      return LeafNode.load(access, fp);
    }
    // Children count.
    short childrenCount = Short.reverseBytes(access.readShort(fp + offset));
    assert childrenCount > 0;
    offset += 2;
    // Prefix.
    int prefixLength = access.readInt(fp + offset);
    offset += 4;
    byte[] prefix = null;
    if (prefixLength > 0) {
      prefix = new byte[prefixLength];
      access.readBytes(fp + offset, prefix, 0, prefixLength);
      offset += prefixLength;
    }

    // TODO: Change the constructor.
    Node node;
    if (nodeTypeOrdinal == NodeType.NODE4.ordinal()) {
      node = new Node4(prefixLength);
    } else if (nodeTypeOrdinal == NodeType.NODE16.ordinal()) {
      node = new Node16(prefixLength);
    } else if (nodeTypeOrdinal == NodeType.NODE48.ordinal()) {
      node = new Node48(prefixLength);
    } else if (nodeTypeOrdinal == NodeType.NODE256.ordinal()) {
      node = new Node256(prefixLength);
    } else {
      throw new IOException("read error: bad nodeTypeOrdinal");
    }

    node.fp = fp;
    node.prefix = prefix;
    node.childrenCount = childrenCount;

    // 3 bit encodedOutputFpBytes - 1, 1 bit has output, 3bit childrenFpBytes
    int header = access.readByte(fp + offset);
    offset += 1;

    // Read children delta fp bytes and children delta fp start(for loading child).
    // TODO: If we want calculate child's delta fp with childrenDeltaFpStart, childrenDeltaFpBytes.
    // children's pos must saved
    // without gap(node4, node16, node48), we should resolve gap issue for node256.
    node.childrenDeltaFpBytes = (header & 0x07) + 1;
    node.childrenDeltaFpStart = fp + offset;

    if ((header & NON_LEAF_NODE_HAS_OUTPUT) != 0) {
      int encodedOutputFpBytes = ((header >>> 4) & 0x07) + 1;
      // TODO: impl readLongFromNBytes.
      long encodedOutputFp = access.readLong(fp + offset);
      offset += encodedOutputFpBytes;
      // Refresh childrenDeltaFpStart;
      node.childrenDeltaFpStart = fp + offset;
      if (encodedOutputFpBytes < 8) {
        encodedOutputFp = encodedOutputFp & BYTES_MINUS_1_MASK[encodedOutputFpBytes - 1];
      }

      node.outputFp = encodedOutputFp >>> 2;
      node.hasTerms = (encodedOutputFp & NON_LEAF_NODE_HAS_TERMS) != 0;
      // Skip children delta fp bytes even if no floor.
      offset += childrenCount * node.childrenDeltaFpBytes;

      // Read floor.
      if ((encodedOutputFp & NON_LEAF_NODE_HAS_FLOOR) != 0) {
        node.floorDataLen = access.readInt(fp + offset);
        offset += 4;
        node.floorDataFp = fp + offset;
      }
    } else {
      // Skip children delta fp bytes.
      offset += childrenCount * node.childrenDeltaFpBytes;
    }

    node.readChildIndex(access, fp + offset + node.floorDataLen);
    return node;
  }

  private long encodeFP(Output output) {
    assert output.fp() < 1L << 62;
    return (output.floorData() != null ? NON_LEAF_NODE_HAS_FLOOR : 0)
        | (output.hasTerms() ? NON_LEAF_NODE_HAS_TERMS : 0)
        | (output.fp() << 2);
  }

  /** Write childIndex to output. */
  public abstract void saveChildIndex(IndexOutput dataOutput) throws IOException;

  /** Write childIndex to output. */
  public abstract void readChildIndex(IndexInput dataInput) throws IOException;

  public abstract void readChildIndex(RandomAccessInput access, long fp) throws IOException;

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
      case LEAF_NODE:
      case DUMMY_ROOT:
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

    if (src.prefixLength > 0) {
      System.arraycopy(src.prefix, 0, dst.prefix, 0, src.prefixLength);
    }
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

  /**
   * Set the node's children to right index while doing the read phase.
   *
   * @param children all the not null children nodes in key byte ascending order, no null element.
   */
  abstract void setChildren(Node[] children);

  /** Get the node's children. */
  abstract Node[] getChildren();

  @Override
  public String toString() {
    StringBuilder stb = new StringBuilder();
    stb.append(" nodeType=").append(nodeType);
    stb.append(" prefix=").append(prefix == null ? "" : Arrays.toString(prefix));
    // May throw exception.
    stb.append(" key=").append(key == null ? "" : key.utf8ToString());
    stb.append(" count=").append(childrenCount);
    return stb.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj instanceof Node == false) {
      return false;
    }
    if (this.nodeType != ((Node) obj).nodeType) {
      return false;
    }
    if (this.childrenCount != ((Node) obj).childrenCount) {
      return false;
    }
    if (this.prefixLength != ((Node) obj).prefixLength) {
      return false;
    }
    if (this.prefix != null || ((Node) obj).prefix != null) {
      if (Arrays.equals(this.prefix, ((Node) obj).prefix) == false) {
        return false;
      }
    }
    if (this.key != null || ((Node) obj).key != null) {
      if (this.key == null || ((Node) obj).key == null) {
        return false;
      }
      if (this.key.equals(((Node) obj).key) == false) {
        return false;
      }
    }
    if (this.output != null || ((Node) obj).output != null) {
      if (this.output == null || ((Node) obj).output == null) {
        return false;
      }
      if (this.output.equals(((Node) obj).output) == false) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    // TODO
    return super.hashCode();
  }
}
