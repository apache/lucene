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
package org.apache.lucene.util.bkd;

import java.io.IOException;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.MathUtil;

/**
 * Handles reading a block KD-tree in byte[] space previously written with {@link BKDWriter}.
 *
 * @lucene.experimental
 */
public class BKDReader extends PointValues {

  final BKDConfig config;
  final int numLeaves;
  final IndexInput in;
  final byte[] minPackedValue;
  final byte[] maxPackedValue;
  final long pointCount;
  final int docCount;
  final int version;
  final long minLeafBlockFP;

  final IndexInput packedIndex;
  // if true, the tree is a legacy balanced tree
  private final boolean isTreeBalanced;

  /**
   * Caller must pre-seek the provided {@link IndexInput} to the index location that {@link
   * BKDWriter#finish} returned. BKD tree is always stored off-heap.
   */
  public BKDReader(IndexInput metaIn, IndexInput indexIn, IndexInput dataIn) throws IOException {
    version =
        CodecUtil.checkHeader(
            metaIn, BKDWriter.CODEC_NAME, BKDWriter.VERSION_START, BKDWriter.VERSION_CURRENT);
    final int numDims = metaIn.readVInt();
    final int numIndexDims;
    if (version >= BKDWriter.VERSION_SELECTIVE_INDEXING) {
      numIndexDims = metaIn.readVInt();
    } else {
      numIndexDims = numDims;
    }
    final int maxPointsInLeafNode = metaIn.readVInt();
    final int bytesPerDim = metaIn.readVInt();
    config = new BKDConfig(numDims, numIndexDims, bytesPerDim, maxPointsInLeafNode);

    // Read index:
    numLeaves = metaIn.readVInt();
    assert numLeaves > 0;

    minPackedValue = new byte[config.packedIndexBytesLength];
    maxPackedValue = new byte[config.packedIndexBytesLength];

    metaIn.readBytes(minPackedValue, 0, config.packedIndexBytesLength);
    metaIn.readBytes(maxPackedValue, 0, config.packedIndexBytesLength);
    final ArrayUtil.ByteArrayComparator comparator =
        ArrayUtil.getUnsignedComparator(config.bytesPerDim);
    for (int dim = 0; dim < config.numIndexDims; dim++) {
      if (comparator.compare(
              minPackedValue, dim * config.bytesPerDim, maxPackedValue, dim * config.bytesPerDim)
          > 0) {
        throw new CorruptIndexException(
            "minPackedValue "
                + new BytesRef(minPackedValue)
                + " is > maxPackedValue "
                + new BytesRef(maxPackedValue)
                + " for dim="
                + dim,
            metaIn);
      }
    }

    pointCount = metaIn.readVLong();
    docCount = metaIn.readVInt();

    int numIndexBytes = metaIn.readVInt();
    long indexStartPointer;
    if (version >= BKDWriter.VERSION_META_FILE) {
      minLeafBlockFP = metaIn.readLong();
      indexStartPointer = metaIn.readLong();
    } else {
      indexStartPointer = indexIn.getFilePointer();
      minLeafBlockFP = indexIn.readVLong();
      indexIn.seek(indexStartPointer);
    }
    this.packedIndex = indexIn.slice("packedIndex", indexStartPointer, numIndexBytes);
    this.in = dataIn;
    // for only one leaf, balanced and unbalanced trees can be handled the same way
    // we set it to unbalanced.
    this.isTreeBalanced = numLeaves != 1 && isTreeBalanced();
  }

  private boolean isTreeBalanced() throws IOException {
    if (version >= BKDWriter.VERSION_META_FILE) {
      // since lucene 8.6 all trees are unbalanced.
      return false;
    }
    if (config.numDims > 1) {
      // high dimensional tree in pre-8.6 indices are balanced.
      assert 1 << MathUtil.log(numLeaves, 2) == numLeaves;
      return true;
    }
    if (1 << MathUtil.log(numLeaves, 2) != numLeaves) {
      // if we don't have enough leaves to fill the last level then it is unbalanced
      return false;
    }
    // count of the last node for unbalanced trees
    final int lastLeafNodePointCount = Math.toIntExact(pointCount % config.maxPointsInLeafNode);
    // navigate to last node
    PointTree pointTree = getPointTree();
    do {
      while (pointTree.moveToSibling()) {}
    } while (pointTree.moveToChild());
    // count number of docs in the node
    final int[] count = new int[] {0};
    pointTree.visitDocIDs(
        new IntersectVisitor() {
          @Override
          public void visit(int docID) {
            count[0]++;
          }

          @Override
          public void visit(int docID, byte[] packedValue) {
            throw new AssertionError();
          }

          @Override
          public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            throw new AssertionError();
          }
        });
    return count[0] != lastLeafNodePointCount;
  }

  @Override
  public PointTree getPointTree() throws IOException {
    return new BKDPointTree(
        packedIndex.clone(),
        this.in.clone(),
        config,
        numLeaves,
        version,
        pointCount,
        minPackedValue,
        maxPackedValue,
        isTreeBalanced);
  }

  private static class BKDPointTree implements PointTree {
    private int nodeID;
    // during clone, the node root can be different to 1
    private final int nodeRoot;
    // level is 1-based so that we can do level-1 w/o checking each time:
    private int level;
    // used to read the packed tree off-heap
    private final IndexInput innerNodes;
    // used to read the packed leaves off-heap
    private final IndexInput leafNodes;
    // holds the minimum (left most) leaf block file pointer for each level we've recursed to:
    private final long[] leafBlockFPStack;
    // holds the address, in the off-heap index, after reading the node data of each level:
    private final int[] readNodeDataPositions;
    // holds the address, in the off-heap index, of the right-node of each level:
    private final int[] rightNodePositions;
    // holds the splitDim position for each level:
    private final int[] splitDimsPos;
    // true if the per-dim delta we read for the node at this level is a negative offset vs. the
    // last split on this dim; this is a packed
    // 2D array, i.e. to access array[level][dim] you read from negativeDeltas[level*numDims+dim].
    // this will be true if the last time we
    // split on this dimension, we next pushed to the left sub-tree:
    private final boolean[] negativeDeltas;
    // holds the packed per-level split values
    private final byte[][] splitValuesStack;
    // holds the min / max value of the current node.
    private final byte[] minPackedValue, maxPackedValue;
    // holds the previous value of the split dimension
    private final byte[][] splitDimValueStack;
    // tree parameters
    private final BKDConfig config;
    // number of leaves
    private final int leafNodeOffset;
    // version of the index
    private final int version;
    // total number of points
    final long pointCount;
    // last node might not be fully populated
    private final int lastLeafNodePointCount;
    // right most leaf node ID
    private final int rightMostLeafNode;
    // helper objects for reading doc values
    private final byte[] scratchDataPackedValue,
        scratchMinIndexPackedValue,
        scratchMaxIndexPackedValue;
    private final int[] commonPrefixLengths;
    private final BKDReaderDocIDSetIterator scratchIterator;
    private final DocIdsWriter docIdsWriter;
    // if true the tree is balanced, otherwise unbalanced
    private final boolean isTreeBalanced;

    private BKDPointTree(
        IndexInput innerNodes,
        IndexInput leafNodes,
        BKDConfig config,
        int numLeaves,
        int version,
        long pointCount,
        byte[] minPackedValue,
        byte[] maxPackedValue,
        boolean isTreeBalanced)
        throws IOException {
      this(
          innerNodes,
          leafNodes,
          config,
          numLeaves,
          version,
          pointCount,
          1,
          1,
          minPackedValue,
          maxPackedValue,
          new BKDReaderDocIDSetIterator(config.maxPointsInLeafNode),
          new byte[config.packedBytesLength],
          new byte[config.packedIndexBytesLength],
          new byte[config.packedIndexBytesLength],
          new int[config.numDims],
          isTreeBalanced);
      // read root node
      readNodeData(false);
    }

    private BKDPointTree(
        IndexInput innerNodes,
        IndexInput leafNodes,
        BKDConfig config,
        int numLeaves,
        int version,
        long pointCount,
        int nodeID,
        int level,
        byte[] minPackedValue,
        byte[] maxPackedValue,
        BKDReaderDocIDSetIterator scratchIterator,
        byte[] scratchDataPackedValue,
        byte[] scratchMinIndexPackedValue,
        byte[] scratchMaxIndexPackedValue,
        int[] commonPrefixLengths,
        boolean isTreeBalanced) {
      this.config = config;
      this.version = version;
      this.nodeID = nodeID;
      this.nodeRoot = nodeID;
      this.level = level;
      this.isTreeBalanced = isTreeBalanced;
      leafNodeOffset = numLeaves;
      this.innerNodes = innerNodes;
      this.leafNodes = leafNodes;
      this.minPackedValue = minPackedValue.clone();
      this.maxPackedValue = maxPackedValue.clone();
      // stack arrays that keep information at different levels
      int treeDepth = getTreeDepth(numLeaves);
      splitDimValueStack = new byte[treeDepth][];
      splitValuesStack = new byte[treeDepth][];
      splitValuesStack[0] = new byte[config.packedIndexBytesLength];
      leafBlockFPStack = new long[treeDepth + 1];
      readNodeDataPositions = new int[treeDepth + 1];
      rightNodePositions = new int[treeDepth];
      splitDimsPos = new int[treeDepth];
      negativeDeltas = new boolean[config.numIndexDims * treeDepth];
      // information about the unbalance of the tree so we can report the exact size below a node
      this.pointCount = pointCount;
      rightMostLeafNode = (1 << treeDepth - 1) - 1;
      int lastLeafNodePointCount = Math.toIntExact(pointCount % config.maxPointsInLeafNode);
      this.lastLeafNodePointCount =
          lastLeafNodePointCount == 0 ? config.maxPointsInLeafNode : lastLeafNodePointCount;
      // scratch objects, reused between clones so NN search are not creating those objects
      // in every clone.
      this.scratchIterator = scratchIterator;
      this.commonPrefixLengths = commonPrefixLengths;
      this.scratchDataPackedValue = scratchDataPackedValue;
      this.scratchMinIndexPackedValue = scratchMinIndexPackedValue;
      this.scratchMaxIndexPackedValue = scratchMaxIndexPackedValue;
      this.docIdsWriter = scratchIterator.docIdsWriter;
    }

    @Override
    public PointTree clone() {
      BKDPointTree index =
          new BKDPointTree(
              innerNodes.clone(),
              leafNodes.clone(),
              config,
              leafNodeOffset,
              version,
              pointCount,
              nodeID,
              level,
              minPackedValue,
              maxPackedValue,
              scratchIterator,
              scratchDataPackedValue,
              scratchMinIndexPackedValue,
              scratchMaxIndexPackedValue,
              commonPrefixLengths,
              isTreeBalanced);
      index.leafBlockFPStack[index.level] = leafBlockFPStack[level];
      if (isLeafNode() == false) {
        // copy node data
        index.rightNodePositions[index.level] = rightNodePositions[level];
        index.readNodeDataPositions[index.level] = readNodeDataPositions[level];
        index.splitValuesStack[index.level] = splitValuesStack[level].clone();
        System.arraycopy(
            negativeDeltas,
            level * config.numIndexDims,
            index.negativeDeltas,
            level * config.numIndexDims,
            config.numIndexDims);
        index.splitDimsPos[level] = splitDimsPos[level];
      }
      return index;
    }

    @Override
    public byte[] getMinPackedValue() {
      return minPackedValue;
    }

    @Override
    public byte[] getMaxPackedValue() {
      return maxPackedValue;
    }

    @Override
    public boolean moveToChild() throws IOException {
      if (isLeafNode()) {
        return false;
      }
      resetNodeDataPosition();
      pushBoundsLeft();
      pushLeft();
      return true;
    }

    private void resetNodeDataPosition() throws IOException {
      // move position of the inner nodes index to visit the first child
      assert readNodeDataPositions[level] <= innerNodes.getFilePointer();
      innerNodes.seek(readNodeDataPositions[level]);
    }

    private void pushBoundsLeft() {
      final int splitDimPos = splitDimsPos[level];
      if (splitDimValueStack[level] == null) {
        splitDimValueStack[level] = new byte[config.bytesPerDim];
      }
      // save the dimension we are going to change
      System.arraycopy(
          maxPackedValue, splitDimPos, splitDimValueStack[level], 0, config.bytesPerDim);
      assert ArrayUtil.getUnsignedComparator(config.bytesPerDim)
                  .compare(maxPackedValue, splitDimPos, splitValuesStack[level], splitDimPos)
              >= 0
          : "config.bytesPerDim="
              + config.bytesPerDim
              + " splitDimPos="
              + splitDimsPos[level]
              + " config.numIndexDims="
              + config.numIndexDims
              + " config.numDims="
              + config.numDims;
      // add the split dim value:
      System.arraycopy(
          splitValuesStack[level], splitDimPos, maxPackedValue, splitDimPos, config.bytesPerDim);
    }

    private void pushLeft() throws IOException {
      nodeID *= 2;
      level++;
      readNodeData(true);
    }

    private void pushBoundsRight() {
      final int splitDimPos = splitDimsPos[level];
      // we should have already visited the left node
      assert splitDimValueStack[level] != null;
      // save the dimension we are going to change
      System.arraycopy(
          minPackedValue, splitDimPos, splitDimValueStack[level], 0, config.bytesPerDim);
      assert ArrayUtil.getUnsignedComparator(config.bytesPerDim)
                  .compare(minPackedValue, splitDimPos, splitValuesStack[level], splitDimPos)
              <= 0
          : "config.bytesPerDim="
              + config.bytesPerDim
              + " splitDimPos="
              + splitDimsPos[level]
              + " config.numIndexDims="
              + config.numIndexDims
              + " config.numDims="
              + config.numDims;
      // add the split dim value:
      System.arraycopy(
          splitValuesStack[level], splitDimPos, minPackedValue, splitDimPos, config.bytesPerDim);
    }

    private void pushRight() throws IOException {
      final int nodePosition = rightNodePositions[level];
      assert nodePosition >= innerNodes.getFilePointer()
          : "nodePosition = " + nodePosition + " < currentPosition=" + innerNodes.getFilePointer();
      innerNodes.seek(nodePosition);
      nodeID = 2 * nodeID + 1;
      level++;
      readNodeData(false);
    }

    @Override
    public boolean moveToSibling() throws IOException {
      if (isLeftNode() == false || isRootNode()) {
        return false;
      }
      pop();
      popBounds(maxPackedValue);
      pushBoundsRight();
      pushRight();
      assert nodeExists();
      return true;
    }

    private void pop() {
      nodeID /= 2;
      level--;
    }

    private void popBounds(byte[] packedValue) {
      // restore the split dimension
      System.arraycopy(
          splitDimValueStack[level], 0, packedValue, splitDimsPos[level], config.bytesPerDim);
    }

    @Override
    public boolean moveToParent() {
      if (isRootNode()) {
        return false;
      }
      final byte[] packedValue = isLeftNode() ? maxPackedValue : minPackedValue;
      pop();
      popBounds(packedValue);
      return true;
    }

    private boolean isRootNode() {
      return nodeID == nodeRoot;
    }

    private boolean isLeftNode() {
      return (nodeID & 1) == 0;
    }

    private boolean isLeafNode() {
      return nodeID >= leafNodeOffset;
    }

    private boolean nodeExists() {
      return nodeID - leafNodeOffset < leafNodeOffset;
    }

    /** Only valid after pushLeft or pushRight, not pop! */
    private long getLeafBlockFP() {
      assert isLeafNode() : "nodeID=" + nodeID + " is not a leaf";
      return leafBlockFPStack[level];
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
      if (isTreeBalanced) {
        // before lucene 8.6, trees might have been constructed as fully balanced trees.
        return sizeFromBalancedTree(leftMostLeafNode, rightMostLeafNode);
      }
      // size for an unbalanced tree.
      return rightMostLeafNode == this.rightMostLeafNode
          ? (long) (numLeaves - 1) * config.maxPointsInLeafNode + lastLeafNodePointCount
          : (long) numLeaves * config.maxPointsInLeafNode;
    }

    private long sizeFromBalancedTree(int leftMostLeafNode, int rightMostLeafNode) {
      // number of points that need to be distributed between leaves, one per leaf
      final int extraPoints =
          Math.toIntExact(((long) config.maxPointsInLeafNode * this.leafNodeOffset) - pointCount);
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

    @Override
    public void visitDocIDs(PointValues.IntersectVisitor visitor) throws IOException {
      resetNodeDataPosition();
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
        leafNodes.seek(getLeafBlockFP());
        // How many points are stored in this leaf cell:
        int count = leafNodes.readVInt();
        // No need to call grow(), it has been called up-front
        docIdsWriter.readInts(leafNodes, count, visitor);
      } else {
        pushLeft();
        addAll(visitor, grown);
        pop();
        pushRight();
        addAll(visitor, grown);
        pop();
      }
    }

    @Override
    public void visitDocValues(PointValues.IntersectVisitor visitor) throws IOException {
      resetNodeDataPosition();
      visitLeavesOneByOne(visitor);
    }

    private void visitLeavesOneByOne(PointValues.IntersectVisitor visitor) throws IOException {
      if (isLeafNode()) {
        // Leaf node
        visitDocValues(visitor, getLeafBlockFP());
      } else {
        pushLeft();
        visitLeavesOneByOne(visitor);
        pop();
        pushRight();
        visitLeavesOneByOne(visitor);
        pop();
      }
    }

    private void visitDocValues(PointValues.IntersectVisitor visitor, long fp) throws IOException {
      // Leaf node; scan and filter all points in this block:
      int count = readDocIDs(leafNodes, fp, scratchIterator);
      if (version >= BKDWriter.VERSION_LOW_CARDINALITY_LEAVES) {
        visitDocValuesWithCardinality(
            commonPrefixLengths,
            scratchDataPackedValue,
            scratchMinIndexPackedValue,
            scratchMaxIndexPackedValue,
            leafNodes,
            scratchIterator,
            count,
            visitor);
      } else {
        visitDocValuesNoCardinality(
            commonPrefixLengths,
            scratchDataPackedValue,
            scratchMinIndexPackedValue,
            scratchMaxIndexPackedValue,
            leafNodes,
            scratchIterator,
            count,
            visitor);
      }
    }

    private int readDocIDs(IndexInput in, long blockFP, BKDReaderDocIDSetIterator iterator)
        throws IOException {
      in.seek(blockFP);
      // How many points are stored in this leaf cell:
      int count = in.readVInt();

      docIdsWriter.readInts(in, count, iterator.docIDs);

      return count;
    }

    // for assertions
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

    private void readNodeData(boolean isLeft) throws IOException {
      leafBlockFPStack[level] = leafBlockFPStack[level - 1];
      if (isLeft == false) {
        // read leaf block FP delta
        leafBlockFPStack[level] += innerNodes.readVLong();
      }

      if (isLeafNode() == false) {
        System.arraycopy(
            negativeDeltas,
            (level - 1) * config.numIndexDims,
            negativeDeltas,
            level * config.numIndexDims,
            config.numIndexDims);
        negativeDeltas[
                level * config.numIndexDims + (splitDimsPos[level - 1] / config.bytesPerDim)] =
            isLeft;

        if (splitValuesStack[level] == null) {
          splitValuesStack[level] = splitValuesStack[level - 1].clone();
        } else {
          System.arraycopy(
              splitValuesStack[level - 1],
              0,
              splitValuesStack[level],
              0,
              config.packedIndexBytesLength);
        }

        // read split dim, prefix, firstDiffByteDelta encoded as int:
        int code = innerNodes.readVInt();
        final int splitDim = code % config.numIndexDims;
        splitDimsPos[level] = splitDim * config.bytesPerDim;
        code /= config.numIndexDims;
        final int prefix = code % (1 + config.bytesPerDim);
        final int suffix = config.bytesPerDim - prefix;

        if (suffix > 0) {
          int firstDiffByteDelta = code / (1 + config.bytesPerDim);
          if (negativeDeltas[level * config.numIndexDims + splitDim]) {
            firstDiffByteDelta = -firstDiffByteDelta;
          }
          final int startPos = splitDimsPos[level] + prefix;
          final int oldByte = splitValuesStack[level][startPos] & 0xFF;
          splitValuesStack[level][startPos] = (byte) (oldByte + firstDiffByteDelta);
          innerNodes.readBytes(splitValuesStack[level], startPos + 1, suffix - 1);
        } else {
          // our split value is == last split value in this dim, which can happen when there are
          // many duplicate values
        }

        final int leftNumBytes;
        if (nodeID * 2 < leafNodeOffset) {
          leftNumBytes = innerNodes.readVInt();
        } else {
          leftNumBytes = 0;
        }
        rightNodePositions[level] = Math.toIntExact(innerNodes.getFilePointer()) + leftNumBytes;
        readNodeDataPositions[level] = Math.toIntExact(innerNodes.getFilePointer());
      }
    }

    private int getTreeDepth(int numLeaves) {
      // First +1 because all the non-leave nodes makes another power
      // of 2; e.g. to have a fully balanced tree with 4 leaves you
      // need a depth=3 tree:

      // Second +1 because MathUtil.log computes floor of the logarithm; e.g.
      // with 5 leaves you need a depth=4 tree:
      return MathUtil.log(numLeaves, 2) + 2;
    }

    private void visitDocValuesNoCardinality(
        int[] commonPrefixLengths,
        byte[] scratchDataPackedValue,
        byte[] scratchMinIndexPackedValue,
        byte[] scratchMaxIndexPackedValue,
        IndexInput in,
        BKDReaderDocIDSetIterator scratchIterator,
        int count,
        PointValues.IntersectVisitor visitor)
        throws IOException {
      readCommonPrefixes(commonPrefixLengths, scratchDataPackedValue, in);
      if (config.numIndexDims != 1 && version >= BKDWriter.VERSION_LEAF_STORES_BOUNDS) {
        byte[] minPackedValue = scratchMinIndexPackedValue;
        System.arraycopy(
            scratchDataPackedValue, 0, minPackedValue, 0, config.packedIndexBytesLength);
        byte[] maxPackedValue = scratchMaxIndexPackedValue;
        // Copy common prefixes before reading adjusted box
        System.arraycopy(minPackedValue, 0, maxPackedValue, 0, config.packedIndexBytesLength);
        readMinMax(commonPrefixLengths, minPackedValue, maxPackedValue, in);

        // The index gives us range of values for each dimension, but the actual range of values
        // might be much more narrow than what the index told us, so we double check the relation
        // here, which is cheap yet might help figure out that the block either entirely matches
        // or does not match at all. This is especially more likely in the case that there are
        // multiple dimensions that have correlation, ie. splitting on one dimension also
        // significantly changes the range of values in another dimension.
        PointValues.Relation r = visitor.compare(minPackedValue, maxPackedValue);
        if (r == PointValues.Relation.CELL_OUTSIDE_QUERY) {
          return;
        }
        visitor.grow(count);
        if (r == PointValues.Relation.CELL_INSIDE_QUERY) {
          for (int i = 0; i < count; ++i) {
            visitor.visit(scratchIterator.docIDs[i]);
          }
          return;
        }
      } else {
        visitor.grow(count);
      }

      int compressedDim = readCompressedDim(in);

      if (compressedDim == -1) {
        visitUniqueRawDocValues(scratchDataPackedValue, scratchIterator, count, visitor);
      } else {
        visitCompressedDocValues(
            commonPrefixLengths,
            scratchDataPackedValue,
            in,
            scratchIterator,
            count,
            visitor,
            compressedDim);
      }
    }

    private void visitDocValuesWithCardinality(
        int[] commonPrefixLengths,
        byte[] scratchDataPackedValue,
        byte[] scratchMinIndexPackedValue,
        byte[] scratchMaxIndexPackedValue,
        IndexInput in,
        BKDReaderDocIDSetIterator scratchIterator,
        int count,
        PointValues.IntersectVisitor visitor)
        throws IOException {

      readCommonPrefixes(commonPrefixLengths, scratchDataPackedValue, in);
      int compressedDim = readCompressedDim(in);
      if (compressedDim == -1) {
        // all values are the same
        visitor.grow(count);
        visitUniqueRawDocValues(scratchDataPackedValue, scratchIterator, count, visitor);
      } else {
        if (config.numIndexDims != 1) {
          byte[] minPackedValue = scratchMinIndexPackedValue;
          System.arraycopy(
              scratchDataPackedValue, 0, minPackedValue, 0, config.packedIndexBytesLength);
          byte[] maxPackedValue = scratchMaxIndexPackedValue;
          // Copy common prefixes before reading adjusted box
          System.arraycopy(minPackedValue, 0, maxPackedValue, 0, config.packedIndexBytesLength);
          readMinMax(commonPrefixLengths, minPackedValue, maxPackedValue, in);

          // The index gives us range of values for each dimension, but the actual range of values
          // might be much more narrow than what the index told us, so we double check the relation
          // here, which is cheap yet might help figure out that the block either entirely matches
          // or does not match at all. This is especially more likely in the case that there are
          // multiple dimensions that have correlation, ie. splitting on one dimension also
          // significantly changes the range of values in another dimension.
          PointValues.Relation r = visitor.compare(minPackedValue, maxPackedValue);
          if (r == PointValues.Relation.CELL_OUTSIDE_QUERY) {
            return;
          }
          visitor.grow(count);

          if (r == PointValues.Relation.CELL_INSIDE_QUERY) {
            for (int i = 0; i < count; ++i) {
              visitor.visit(scratchIterator.docIDs[i]);
            }
            return;
          }
        } else {
          visitor.grow(count);
        }

        if (compressedDim == -2) {
          // low cardinality values
          visitSparseRawDocValues(
              commonPrefixLengths, scratchDataPackedValue, in, scratchIterator, count, visitor);
        } else {
          // high cardinality
          visitCompressedDocValues(
              commonPrefixLengths,
              scratchDataPackedValue,
              in,
              scratchIterator,
              count,
              visitor,
              compressedDim);
        }
      }
    }

    private void readMinMax(
        int[] commonPrefixLengths, byte[] minPackedValue, byte[] maxPackedValue, IndexInput in)
        throws IOException {
      for (int dim = 0; dim < config.numIndexDims; dim++) {
        int prefix = commonPrefixLengths[dim];
        in.readBytes(
            minPackedValue, dim * config.bytesPerDim + prefix, config.bytesPerDim - prefix);
        in.readBytes(
            maxPackedValue, dim * config.bytesPerDim + prefix, config.bytesPerDim - prefix);
      }
    }

    // read cardinality and point
    private void visitSparseRawDocValues(
        int[] commonPrefixLengths,
        byte[] scratchPackedValue,
        IndexInput in,
        BKDReaderDocIDSetIterator scratchIterator,
        int count,
        PointValues.IntersectVisitor visitor)
        throws IOException {
      int i;
      for (i = 0; i < count; ) {
        int length = in.readVInt();
        for (int dim = 0; dim < config.numDims; dim++) {
          int prefix = commonPrefixLengths[dim];
          in.readBytes(
              scratchPackedValue, dim * config.bytesPerDim + prefix, config.bytesPerDim - prefix);
        }
        scratchIterator.reset(i, length);
        visitor.visit(scratchIterator, scratchPackedValue);
        i += length;
      }
      if (i != count) {
        throw new CorruptIndexException(
            "Sub blocks do not add up to the expected count: " + count + " != " + i, in);
      }
    }

    // point is under commonPrefix
    private void visitUniqueRawDocValues(
        byte[] scratchPackedValue,
        BKDReaderDocIDSetIterator scratchIterator,
        int count,
        PointValues.IntersectVisitor visitor)
        throws IOException {
      scratchIterator.reset(0, count);
      visitor.visit(scratchIterator, scratchPackedValue);
    }

    private void visitCompressedDocValues(
        int[] commonPrefixLengths,
        byte[] scratchPackedValue,
        IndexInput in,
        BKDReaderDocIDSetIterator scratchIterator,
        int count,
        PointValues.IntersectVisitor visitor,
        int compressedDim)
        throws IOException {
      // the byte at `compressedByteOffset` is compressed using run-length compression,
      // other suffix bytes are stored verbatim
      final int compressedByteOffset =
          compressedDim * config.bytesPerDim + commonPrefixLengths[compressedDim];
      commonPrefixLengths[compressedDim]++;
      int i;
      for (i = 0; i < count; ) {
        scratchPackedValue[compressedByteOffset] = in.readByte();
        final int runLen = Byte.toUnsignedInt(in.readByte());
        for (int j = 0; j < runLen; ++j) {
          for (int dim = 0; dim < config.numDims; dim++) {
            int prefix = commonPrefixLengths[dim];
            in.readBytes(
                scratchPackedValue, dim * config.bytesPerDim + prefix, config.bytesPerDim - prefix);
          }
          visitor.visit(scratchIterator.docIDs[i + j], scratchPackedValue);
        }
        i += runLen;
      }
      if (i != count) {
        throw new CorruptIndexException(
            "Sub blocks do not add up to the expected count: " + count + " != " + i, in);
      }
    }

    private int readCompressedDim(IndexInput in) throws IOException {
      int compressedDim = in.readByte();
      if (compressedDim < -2
          || compressedDim >= config.numDims
          || (version < BKDWriter.VERSION_LOW_CARDINALITY_LEAVES && compressedDim == -2)) {
        throw new CorruptIndexException("Got compressedDim=" + compressedDim, in);
      }
      return compressedDim;
    }

    private void readCommonPrefixes(
        int[] commonPrefixLengths, byte[] scratchPackedValue, IndexInput in) throws IOException {
      for (int dim = 0; dim < config.numDims; dim++) {
        int prefix = in.readVInt();
        commonPrefixLengths[dim] = prefix;
        if (prefix > 0) {
          in.readBytes(scratchPackedValue, dim * config.bytesPerDim, prefix);
        }
        // System.out.println("R: " + dim + " of " + numDims + " prefix=" + prefix);
      }
    }

    @Override
    public String toString() {
      return "nodeID=" + nodeID;
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

  /** Reusable {@link DocIdSetIterator} to handle low cardinality leaves. */
  private static class BKDReaderDocIDSetIterator extends DocIdSetIterator {

    private int idx;
    private int length;
    private int offset;
    private int docID;
    final int[] docIDs;
    private final DocIdsWriter docIdsWriter;

    public BKDReaderDocIDSetIterator(int maxPointsInLeafNode) {
      this.docIDs = new int[maxPointsInLeafNode];
      this.docIdsWriter = new DocIdsWriter(maxPointsInLeafNode);
    }

    @Override
    public int docID() {
      return docID;
    }

    private void reset(int offset, int length) {
      this.offset = offset;
      this.length = length;
      assert offset + length <= docIDs.length;
      this.docID = -1;
      this.idx = 0;
    }

    @Override
    public int nextDoc() throws IOException {
      if (idx == length) {
        docID = DocIdSetIterator.NO_MORE_DOCS;
      } else {
        docID = docIDs[offset + idx];
        idx++;
      }
      return docID;
    }

    @Override
    public int advance(int target) throws IOException {
      return slowAdvance(target);
    }

    @Override
    public long cost() {
      return length;
    }
  }
}
