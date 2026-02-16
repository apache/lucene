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
  static final int LEAF_NODE_HAS_TERMS = 1 << 3;
  static final int LEAF_NODE_HAS_FLOOR = 1 << 4;
  static final int LEAF_NODE = 1 << 7;
  static final int LEAF_NODE_HAS_KEY = 1 << 5;
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
  public long outputFp = -1;
  public boolean hasTerms;
  long floorDataFp = -1;
  int childrenDeltaFpBytes;
  long childrenDeltaFpStart;

  // The latest child that have been saved. null means no child has been saved.
  int savedChildPos = -1;

  // TODO: move to leafNode if key just exists in leafNode?
  public BytesRef key;
  Output output;

  static final int HAS_OUTPUT = 1;
  static final int HAS_FLOOR = 1 << 1;
  static final int HAS_TERMS = 1 << 2;

  // node type
  public NodeType nodeType;
  // length of compressed path(prefix)
  public int prefixLength;
  // the compressed path path (prefix)
  public byte[] prefix;
  // number of non-null children, the largest value will not beyond 255
  // to benefit calculation,we keep the value as a short type
  protected short childrenCount;
  public static final int ILLEGAL_IDX = -1;

  /**
   * constructor
   *
   * @param nodeType the node type
   * @param compressedPrefixSize the prefix byte array size
   */
  public Node(NodeType nodeType, int compressedPrefixSize) {
    this.nodeType = nodeType;
    this.prefixLength = compressedPrefixSize;
    if (prefixLength > 0) {
      prefix = new byte[prefixLength];
    }
  }

  /** Return node's output. */
  public Output getOutput() {
    return output;
  }

  /** Get the position of a child corresponding to the input index byte. */
  public abstract int getChildPos(byte indexByte);

  /** Get the corresponding index byte of the requested position */
  public abstract byte getChildIndexByte(int pos);

  /** Get the child at the specified position in the node, the 'pos' range from 0 to count */
  public abstract Node getChild(int pos);

  /** Replace the position child to the fresh one */
  public abstract void replaceNode(int pos, Node freshOne);

  /** Get the next position in the node */
  public abstract int getNextLargerPos(int pos);

  /** Get the max child's position */
  public abstract int getMaxPos();

  /** Write non leaf node to output. */
  public void saveNode(IndexOutput index) throws IOException {
    // Get first child to calculate max delta fp between parent and children.
    // Children fps are in order, so the first child's fp is min, then delta is max.
    // For node48, its position is the child index byte, we use the first child in children.
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
    // highest 1 bit isLeaf(always 0), 3 bit encodedOutputFpBytes - 1, 1 bit has output, 3bit
    // childrenFpBytes - 1
    int header =
        (childrenFpBytes - 1)
            | (output != null ? NON_LEAF_NODE_HAS_OUTPUT : 0)
            | ((encodedOutputFpBytes - 1) << 4);

    index.writeByte((byte) header);

    // Only save count, prefix for non-leaf node. Only save key for leaf node.
    // Children count.
    // TODO: If we can save prefixLength in other byte, we can write childrenCount in one byte, for
    // count > 127, write count - 256, and & 0xFF after reading it.
    int prefixLengthBytes = prefixLength > 0 ? bytesRequiredVLong(prefixLength) : 0;
    assert prefixLengthBytes <= 4 && prefixLengthBytes >= 0;
    index.writeShort((short) ((childrenCount << 3) | prefixLengthBytes));
    // Write prefix.
    if (prefixLength > 0) {
      writeLongNBytes(prefixLength, prefixLengthBytes, index);
      index.writeBytes(prefix, 0, prefixLength);
    }

    saveChildIndex(index);

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

    if (output != null) {
      long encodedFp = encodeFP(output);
      writeLongNBytes(encodedFp, encodedOutputFpBytes, index);
      // write floor data
      // We can use node's fp, childrenCount * childrenFpBytes to calculate floorData's fp.
      if (output.floorData() != null) {
        BytesRef floorData = output.floorData();
        index.writeBytes(floorData.bytes, floorData.offset, floorData.length);
      }
    }
  }

  /** Read node from data input. */
  public static Node load(RandomAccessInput access, long fp) throws IOException {
    // Node type.
    int offset = 0;

    int header = access.readByte(fp + offset);
    offset += 1;
    if ((header & LEAF_NODE) != 0) {
      return LeafNode.load(access, fp + offset, header);
    }

    // Children count.
    short childrenCountAndPrefixLengthBytes = access.readShort(fp + offset);
    offset += 2;
    short childrenCount = (short) (childrenCountAndPrefixLengthBytes >>> 3);
    assert childrenCount > 0 && childrenCount <= 256;
    // Number of prefixLengthBytes.
    int prefixLengthBytes = childrenCountAndPrefixLengthBytes & 0x07;
    assert prefixLengthBytes <= 4;

    // Read prefix.
    int prefixLength = 0;
    byte[] prefix = null;
    // Prefix.
    if (prefixLengthBytes > 0) {
      switch (prefixLengthBytes) {
        case 1 -> prefixLength = access.readByte(fp + offset) & (int) BYTES_MINUS_1_MASK[0];
        case 2 -> prefixLength = access.readShort(fp + offset) & (int) BYTES_MINUS_1_MASK[1];
        case 3 -> {
          prefixLength = access.readInt(fp + offset);
          prefixLength = prefixLength & (int) BYTES_MINUS_1_MASK[2];
        }
        case 4 -> prefixLength = access.readInt(fp + offset);
      }
      offset += prefixLengthBytes;
      assert prefixLength > 0;
      prefix = new byte[prefixLength];
      access.readBytes(fp + offset, prefix, 0, prefixLength);
      offset += prefixLength;
    }

    Node node;
    if (childrenCount > 48) {
      node = new Node256(prefixLength);
    } else if (childrenCount > 16) {
      node = new Node48(prefixLength);
    } else if (childrenCount > 4) {
      node = new Node16(prefixLength);
    } else {
      node = new Node4(prefixLength);
    }

    node.fp = fp;
    node.prefix = prefix;
    node.childrenCount = childrenCount;

    // 3 bit encodedOutputFpBytes - 1, 1 bit has output, 3bit childrenFpBytes

    offset += node.readChildIndex(access, fp + offset);

    // Read children delta fp bytes and children delta fp start(for loading child).
    // If we want to calculate child's delta fp with childrenDeltaFpStart, childrenDeltaFpBytes.
    // children's pos must be saved
    // without gap(node4, node16, node48), we should resolve gap issue for node256.
    node.childrenDeltaFpBytes = (header & 0x07) + 1;
    node.childrenDeltaFpStart = fp + offset;

    // Skip children delta fp bytes.
    offset += childrenCount * node.childrenDeltaFpBytes;

    if ((header & NON_LEAF_NODE_HAS_OUTPUT) != 0) {
      int encodedOutputFpBytes = ((header >>> 4) & 0x07) + 1;
      long encodedOutputFp = access.readLong(fp + offset);
      offset += encodedOutputFpBytes;
      if (encodedOutputFpBytes < 8) {
        encodedOutputFp = encodedOutputFp & BYTES_MINUS_1_MASK[encodedOutputFpBytes - 1];
      }

      node.outputFp = encodedOutputFp >>> 2;
      node.hasTerms = (encodedOutputFp & NON_LEAF_NODE_HAS_TERMS) != 0;
      // Read floor.
      if ((encodedOutputFp & NON_LEAF_NODE_HAS_FLOOR) != 0) {
        node.floorDataFp = fp + offset;
      }
    }

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

  public abstract int readChildIndex(RandomAccessInput access, long fp) throws IOException;

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

  /** Insert the child node into this with the index byte. */
  public abstract Node insert(Node childNode, byte indexByte);

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
   * Search the position of the input index byte in the node's index byte array part
   *
   * @param indexBytes the input indexBytes byte array
   * @param fromIndex inclusive
   * @param toIndex exclusive
   * @param indexByte the target indexBytes byte value
   * @return the array offset of the target input indexBytes 'indexByte' or -1 to not found
   */
  public static int binarySearch(byte[] indexBytes, int fromIndex, int toIndex, byte indexByte) {
    int inputUnsignedByte = Byte.toUnsignedInt(indexByte);
    int low = fromIndex;
    int high = toIndex - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      int midVal = Byte.toUnsignedInt(indexBytes[mid]);

      if (midVal < inputUnsignedByte) {
        low = mid + 1;
      } else if (midVal > inputUnsignedByte) {
        high = mid - 1;
      } else {
        return mid; // indexBytes found
      }
    }
    // indexBytes not found.
    return ILLEGAL_IDX;
  }

  /** Set the node's children to right index while doing the read phase. */
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

  public boolean hasOutput() {
    return outputFp != NO_OUTPUT;
  }

  public boolean isFloor() {
    return floorDataFp != NO_FLOOR_DATA;
  }

  public IndexInput floorData(ARTReader r) throws IOException {
    assert isFloor();
    r.input.seek(floorDataFp);
    return r.input;
  }

  @Override
  public int hashCode() {
    // TODO
    return super.hashCode();
  }
}
