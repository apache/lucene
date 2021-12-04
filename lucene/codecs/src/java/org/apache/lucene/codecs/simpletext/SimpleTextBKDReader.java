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
package org.apache.lucene.codecs.simpletext;

import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.BLOCK_COUNT;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.BLOCK_DOC_ID;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.BLOCK_VALUE;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.MathUtil;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.bkd.BKDConfig;
import org.apache.lucene.util.bkd.BKDReader;

/** Forked from {@link BKDReader} and simplified/specialized for SimpleText's usage */
final class SimpleTextBKDReader extends PointValues {
  // Packed array of byte[] holding all split values in the full binary tree:
  private final byte[] splitPackedValues;
  final long[] leafBlockFPs;
  private final int leafNodeOffset;
  final BKDConfig config;
  final int bytesPerIndexEntry;
  final IndexInput in;
  final byte[] minPackedValue;
  final byte[] maxPackedValue;
  final long pointCount;
  final int docCount;
  final int version;

  public SimpleTextBKDReader(
      IndexInput in,
      int numDims,
      int numIndexDims,
      int maxPointsInLeafNode,
      int bytesPerDim,
      long[] leafBlockFPs,
      byte[] splitPackedValues,
      byte[] minPackedValue,
      byte[] maxPackedValue,
      long pointCount,
      int docCount) {
    this.in = in;
    this.config = new BKDConfig(numDims, numIndexDims, bytesPerDim, maxPointsInLeafNode);
    // no version check here because callers of this API (SimpleText) have no back compat:
    bytesPerIndexEntry = numIndexDims == 1 ? bytesPerDim : bytesPerDim + 1;
    this.leafNodeOffset = leafBlockFPs.length;
    this.leafBlockFPs = leafBlockFPs;
    this.splitPackedValues = splitPackedValues;
    this.minPackedValue = minPackedValue;
    this.maxPackedValue = maxPackedValue;
    this.pointCount = pointCount;
    this.docCount = docCount;
    this.version = SimpleTextBKDWriter.VERSION_CURRENT;
    assert minPackedValue.length == config.packedIndexBytesLength;
    assert maxPackedValue.length == config.packedIndexBytesLength;
  }

  @Override
  public PointTree getPointTree() {
    return new SimpleTextPointTree(in.clone(), 1, 1, minPackedValue, maxPackedValue);
  }

  private class SimpleTextPointTree implements PointTree {

    final int[] scratchDocIDs;
    final byte[] scratchPackedValue;
    int nodeID;
    int level;
    final int rootNode;
    // holds the min / max value of the current node.
    private final byte[] minPackedValue, maxPackedValue;
    // holds the previous value of the split dimension
    private final byte[][] splitDimValueStack;
    // holds the splitDim for each level:
    private final int[] splitDims;

    private final IndexInput in;

    private SimpleTextPointTree(
        IndexInput in, int nodeID, int level, byte[] minPackedValue, byte[] maxPackedValue) {
      this.in = in;
      this.scratchDocIDs = new int[config.maxPointsInLeafNode];
      this.scratchPackedValue = new byte[config.packedBytesLength];
      this.nodeID = nodeID;
      this.rootNode = nodeID;
      this.level = level;
      this.maxPackedValue = maxPackedValue.clone();
      this.minPackedValue = minPackedValue.clone();
      int treeDepth = getTreeDepth(leafNodeOffset);
      splitDimValueStack = new byte[treeDepth + 1][];
      splitDims = new int[treeDepth + 1];
    }

    private int getTreeDepth(int numLeaves) {
      // First +1 because all the non-leave nodes makes another power
      // of 2; e.g. to have a fully balanced tree with 4 leaves you
      // need a depth=3 tree:

      // Second +1 because MathUtil.log computes floor of the logarithm; e.g.
      // with 5 leaves you need a depth=4 tree:
      return MathUtil.log(numLeaves, 2) + 2;
    }

    @Override
    public PointTree clone() {
      SimpleTextPointTree index =
          new SimpleTextPointTree(in.clone(), nodeID, level, minPackedValue, maxPackedValue);
      if (isLeafNode() == false) {
        // copy node data
        index.splitDims[level] = splitDims[level];
        index.splitDimValueStack[level] = splitDimValueStack[level];
      }
      return index;
    }

    @Override
    public boolean moveToChild() {
      if (isLeafNode()) {
        return false;
      }
      pushLeft();
      return true;
    }

    private void pushLeft() {
      int address = nodeID * bytesPerIndexEntry;
      // final int splitDimPos;
      if (config.numIndexDims == 1) {
        splitDims[level] = 0;
      } else {
        splitDims[level] = (splitPackedValues[address++] & 0xff);
      }
      final int splitDimPos = splitDims[level] * config.bytesPerDim;
      if (splitDimValueStack[level] == null) {
        splitDimValueStack[level] = new byte[config.bytesPerDim];
      }
      // save the dimension we are going to change
      System.arraycopy(
          maxPackedValue, splitDimPos, splitDimValueStack[level], 0, config.bytesPerDim);
      assert Arrays.compareUnsigned(
                  maxPackedValue,
                  splitDimPos,
                  splitDimPos + config.bytesPerDim,
                  splitPackedValues,
                  address,
                  address + config.bytesPerDim)
              >= 0
          : "config.bytesPerDim="
              + config.bytesPerDim
              + " splitDim="
              + splitDims[level]
              + " config.numIndexDims="
              + config.numIndexDims
              + " config.numDims="
              + config.numDims;
      nodeID *= 2;
      level++;
      // add the split dim value:
      System.arraycopy(splitPackedValues, address, maxPackedValue, splitDimPos, config.bytesPerDim);
    }

    @Override
    public boolean moveToSibling() {
      if (nodeID != rootNode && (nodeID & 1) == 0) {
        pop(true);
        pushRight();
        return true;
      }
      return false;
    }

    private void pushRight() {
      int address = nodeID * bytesPerIndexEntry;
      if (config.numIndexDims == 1) {
        splitDims[level] = 0;
      } else {
        splitDims[level] = (splitPackedValues[address++] & 0xff);
      }
      final int splitDimPos = splitDims[level] * config.bytesPerDim;
      // we should have already visit the left node
      assert splitDimValueStack[level] != null;
      // save the dimension we are going to change
      System.arraycopy(
          minPackedValue, splitDimPos, splitDimValueStack[level], 0, config.bytesPerDim);
      assert Arrays.compareUnsigned(
                  minPackedValue,
                  splitDimPos,
                  splitDimPos + config.bytesPerDim,
                  splitPackedValues,
                  address,
                  address + config.bytesPerDim)
              <= 0
          : "config.bytesPerDim="
              + config.bytesPerDim
              + " splitDim="
              + splitDims[level]
              + " config.numIndexDims="
              + config.numIndexDims
              + " config.numDims="
              + config.numDims;
      nodeID = 2 * nodeID + 1;
      level++;
      // add the split dim value:
      System.arraycopy(splitPackedValues, address, minPackedValue, splitDimPos, config.bytesPerDim);
    }

    @Override
    public boolean moveToParent() {
      if (nodeID == rootNode) {
        return false;
      }
      pop((nodeID & 1) == 0);
      return true;
    }

    private void pop(boolean isLeft) {
      nodeID /= 2;
      level--;
      // restore the split dimension
      if (isLeft) {
        System.arraycopy(
            splitDimValueStack[level],
            0,
            maxPackedValue,
            splitDims[level] * config.bytesPerDim,
            config.bytesPerDim);
      } else {

        System.arraycopy(
            splitDimValueStack[level],
            0,
            minPackedValue,
            splitDims[level] * config.bytesPerDim,
            config.bytesPerDim);
      }
    }

    @Override
    public byte[] getMinPackedValue() {
      return minPackedValue.clone();
    }

    @Override
    public byte[] getMaxPackedValue() {
      return maxPackedValue.clone();
    }

    @Override
    public long size() {
      int leftMostLeafNode = nodeID;
      while (leftMostLeafNode < leafNodeOffset) {
        leftMostLeafNode = leftMostLeafNode * 2;
      }
      int rightMostLeafNode = nodeID;
      while (rightMostLeafNode < leafNodeOffset) {
        rightMostLeafNode = rightMostLeafNode * 2 + 1;
      }
      final int numLeaves;
      if (rightMostLeafNode >= leftMostLeafNode) {
        // both are on the same level
        numLeaves = rightMostLeafNode - leftMostLeafNode + 1;
      } else {
        // left is one level deeper than right
        numLeaves = rightMostLeafNode - leftMostLeafNode + 1 + leafNodeOffset;
      }
      assert numLeaves == getNumLeavesSlow(nodeID) : numLeaves + " " + getNumLeavesSlow(nodeID);
      return sizeFromBalancedTree(leftMostLeafNode, rightMostLeafNode);
    }

    private long sizeFromBalancedTree(int leftMostLeafNode, int rightMostLeafNode) {
      // number of points that need to be distributed between leaves, one per leaf
      final int extraPoints =
          Math.toIntExact(((long) config.maxPointsInLeafNode * leafNodeOffset) - pointCount);
      assert extraPoints < leafNodeOffset : "point excess should be lower than leafNodeOffset";
      // offset where we stop adding one point to the leaves
      final int nodeOffset = leafNodeOffset - extraPoints;
      long count = 0;
      for (int node = leftMostLeafNode; node <= rightMostLeafNode; node++) {
        // offsetPosition provides which extra point will be added to this node
        if (balanceTreeNodePosition(0, leafNodeOffset, node - leafNodeOffset, 0, 0) < nodeOffset) {
          count += config.maxPointsInLeafNode;
        } else {
          count += config.maxPointsInLeafNode - 1;
        }
      }
      return count;
    }

    private int balanceTreeNodePosition(
        int minNode, int maxNode, int node, int position, int level) {
      if (maxNode - minNode == 1) {
        return position;
      }
      final int mid = (minNode + maxNode + 1) >>> 1;
      if (mid > node) {
        return balanceTreeNodePosition(minNode, mid, node, position, level + 1);
      } else {
        return balanceTreeNodePosition(mid, maxNode, node, position + (1 << level), level + 1);
      }
    }

    private int getNumLeavesSlow(int node) {
      if (node >= 2 * leafNodeOffset) {
        return 0;
      } else if (node >= leafNodeOffset) {
        return 1;
      } else {
        final int leftCount = getNumLeavesSlow(node * 2);
        final int rightCount = getNumLeavesSlow(node * 2 + 1);
        return leftCount + rightCount;
      }
    }

    @Override
    public void visitDocIDs(PointValues.IntersectVisitor visitor) throws IOException {
      addAll(visitor, false);
    }

    public void addAll(PointValues.IntersectVisitor visitor, boolean grown) throws IOException {
      if (grown == false) {
        final long size = size();
        if (size <= Integer.MAX_VALUE) {
          visitor.grow((int) size);
          grown = true;
        }
      }
      if (isLeafNode()) {
        // Leaf node
        BytesRefBuilder scratch = new BytesRefBuilder();
        in.seek(leafBlockFPs[nodeID - leafNodeOffset]);
        readLine(in, scratch);
        int count = parseInt(scratch, BLOCK_COUNT);
        for (int i = 0; i < count; i++) {
          readLine(in, scratch);
          visitor.visit(parseInt(scratch, BLOCK_DOC_ID));
        }
      } else {
        pushLeft();
        addAll(visitor, grown);
        pop(true);
        pushRight();
        addAll(visitor, grown);
        pop(false);
      }
    }

    @Override
    public void visitDocValues(PointValues.IntersectVisitor visitor) throws IOException {
      if (isLeafNode()) {
        // Leaf node
        int leafID = nodeID - leafNodeOffset;

        // Leaf node; scan and filter all points in this block:
        int count = readDocIDs(in, leafBlockFPs[leafID], scratchDocIDs);

        // Again, this time reading values and checking with the visitor
        visitor.grow(count);
        // NOTE: we don't do prefix coding, so we ignore commonPrefixLengths
        assert scratchPackedValue.length == config.packedBytesLength;
        BytesRefBuilder scratch = new BytesRefBuilder();
        for (int i = 0; i < count; i++) {
          readLine(in, scratch);
          assert startsWith(scratch, BLOCK_VALUE);
          BytesRef br = SimpleTextUtil.fromBytesRefString(stripPrefix(scratch, BLOCK_VALUE));
          assert br.length == config.packedBytesLength;
          System.arraycopy(br.bytes, br.offset, scratchPackedValue, 0, config.packedBytesLength);
          visitor.visit(scratchDocIDs[i], scratchPackedValue);
        }
      } else {
        pushLeft();
        visitDocValues(visitor);
        pop(true);
        pushRight();
        visitDocValues(visitor);
        pop(false);
      }
    }

    int readDocIDs(IndexInput in, long blockFP, int[] docIDs) throws IOException {
      BytesRefBuilder scratch = new BytesRefBuilder();
      in.seek(blockFP);
      readLine(in, scratch);
      int count = parseInt(scratch, BLOCK_COUNT);
      for (int i = 0; i < count; i++) {
        readLine(in, scratch);
        docIDs[i] = parseInt(scratch, BLOCK_DOC_ID);
      }
      return count;
    }

    public boolean isLeafNode() {
      return nodeID >= leafNodeOffset;
    }

    private int parseInt(BytesRefBuilder scratch, BytesRef prefix) {
      assert startsWith(scratch, prefix);
      return Integer.parseInt(stripPrefix(scratch, prefix));
    }

    private String stripPrefix(BytesRefBuilder scratch, BytesRef prefix) {
      return new String(
          scratch.bytes(), prefix.length, scratch.length() - prefix.length, StandardCharsets.UTF_8);
    }

    private boolean startsWith(BytesRefBuilder scratch, BytesRef prefix) {
      return StringHelper.startsWith(scratch.get(), prefix);
    }

    private void readLine(IndexInput in, BytesRefBuilder scratch) throws IOException {
      SimpleTextUtil.readLine(in, scratch);
    }
  }

  @Override
  public byte[] getMinPackedValue() {
    return minPackedValue.clone();
  }

  @Override
  public byte[] getMaxPackedValue() {
    return maxPackedValue.clone();
  }

  @Override
  public int getNumDimensions() throws IOException {
    return config.numDims;
  }

  @Override
  public int getNumIndexDimensions() throws IOException {
    return config.numIndexDims;
  }

  @Override
  public int getBytesPerDimension() throws IOException {
    return config.bytesPerDim;
  }

  @Override
  public long size() {
    return pointCount;
  }

  @Override
  public int getDocCount() {
    return docCount;
  }
}
