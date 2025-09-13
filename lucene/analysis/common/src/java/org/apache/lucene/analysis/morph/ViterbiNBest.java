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
package org.apache.lucene.analysis.morph;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.internal.hppc.IntCursor;
import org.apache.lucene.internal.hppc.IntIntHashMap;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.fst.FST;

/** {@link Viterbi} subclass for n-best path calculation. */
public abstract class ViterbiNBest<T extends Token, U extends MorphData>
    extends Viterbi<T, ViterbiNBest.PositionNBest> {

  protected final EnumMap<TokenType, Dictionary<? extends U>> dictionaryMap =
      new EnumMap<>(TokenType.class);

  // Allowable cost difference for N-best output:
  private int nBestCost = 0;

  protected Lattice<U> lattice = null;

  protected ViterbiNBest(
      TokenInfoFST fst,
      FST.BytesReader fstReader,
      BinaryDictionary<? extends MorphData> dictionary,
      TokenInfoFST userFST,
      FST.BytesReader userFSTReader,
      Dictionary<? extends MorphData> userDictionary,
      ConnectionCosts costs) {
    super(
        fst,
        fstReader,
        dictionary,
        userFST,
        userFSTReader,
        userDictionary,
        costs,
        ViterbiNBest.PositionNBest.class);
  }

  @Override
  protected final void backtraceNBest(final Position endPosData, final boolean useEOS)
      throws IOException {
    if (lattice == null) {
      lattice = new Lattice<>();
    }

    final int endPos = endPosData.getPos();
    char[] fragment = buffer.get(lastBackTracePos, endPos - lastBackTracePos);
    lattice.setup(fragment, dictionaryMap, positions, lastBackTracePos, endPos, useEOS);
    lattice.markUnreachable();
    lattice.calcLeftCost(costs);
    lattice.calcRightCost(costs);

    int bestCost = lattice.bestCost();
    if (VERBOSE) {
      System.out.printf("DEBUG: 1-BEST COST: %d\n", bestCost);
    }
    for (IntCursor node : lattice.bestPathNodeList()) {
      registerNode(node.value, fragment);
    }

    for (int n = 2; ; ++n) {
      IntArrayList nbest = lattice.nBestNodeList(n);
      if (nbest.isEmpty()) {
        break;
      }
      int cost = lattice.cost(nbest.get(0));
      if (VERBOSE) {
        System.out.printf("DEBUG: %d-BEST COST: %d\n", n, cost);
      }
      if (bestCost + nBestCost < cost) {
        break;
      }
      for (IntCursor node : nbest) {
        registerNode(node.value, fragment);
      }
    }
    if (VERBOSE) {
      lattice.debugPrint();
    }
  }

  /** Add n-best tokens to the pending list. */
  protected abstract void registerNode(int node, char[] fragment);

  @Override
  protected final void fixupPendingList() {
    // Sort for removing same tokens.
    // USER token should be ahead from normal one.
    Collections.sort(
        pending,
        (a, b) -> {
          int aOff = a.getOffset();
          int bOff = b.getOffset();
          if (aOff != bOff) {
            return aOff - bOff;
          }
          int aLen = a.getLength();
          int bLen = b.getLength();
          if (aLen != bLen) {
            return aLen - bLen;
          }
          // order of Type is KNOWN, UNKNOWN, USER,
          // so we use reversed comparison here.
          return b.getType().ordinal() - a.getType().ordinal();
        });

    // Remove same token.
    for (int i = 1; i < pending.size(); ++i) {
      Token a = pending.get(i - 1);
      Token b = pending.get(i);
      if (a.getOffset() == b.getOffset() && a.getLength() == b.getLength()) {
        pending.remove(i);
        // It is important to decrement "i" here, because a next may be removed.
        --i;
      }
    }

    // offset=>position map
    IntIntHashMap map = new IntIntHashMap();
    for (Token t : pending) {
      map.put(t.getOffset(), 0);
      map.put(t.getOffset() + t.getLength(), 0);
    }

    // Get unique and sorted list of all edge position of tokens.
    int[] offsets = map.keys().toArray();
    Arrays.sort(offsets);

    // setup all value of map.  It specifies N-th position from begin.
    for (int i = 0; i < offsets.length; ++i) {
      map.put(offsets[i], i);
    }

    // We got all position length now.
    for (Token t : pending) {
      t.setPositionLength(map.get(t.getOffset() + t.getLength()) - map.get(t.getOffset()));
    }

    // Make PENDING to be reversed order to fit its usage.
    // If you would like to speedup, you can try reversed order sort
    // at first of this function.
    Collections.reverse(pending);
  }

  protected void setNBestCost(int value) {
    nBestCost = value;
    outputNBest = 0 < nBestCost;
  }

  protected int getNBestCost() {
    return nBestCost;
  }

  public int getLatticeRootBase() {
    return lattice.getRootBase();
  }

  public int probeDelta(int start, int end) {
    return lattice.probeDelta(start, end);
  }

  /**
   * {@link Viterbi.Position} extension; this holds all forward pointers to calculate n-best path.
   */
  public static final class PositionNBest extends Viterbi.Position {
    // Only used when finding 2nd best segmentation under a
    // too-long token:
    int forwardCount;
    int[] forwardPos = new int[8];
    int[] forwardID = new int[8];
    int[] forwardIndex = new int[8];
    TokenType[] forwardType = new TokenType[8];

    private void growForward() {
      forwardPos = ArrayUtil.grow(forwardPos, 1 + forwardCount);
      forwardID = ArrayUtil.grow(forwardID, 1 + forwardCount);
      forwardIndex = ArrayUtil.grow(forwardIndex, 1 + forwardCount);

      // NOTE: sneaky: grow separately because
      // ArrayUtil.grow will otherwise pick a different
      // length than the int[]s we just grew:
      final TokenType[] newForwardType = new TokenType[forwardPos.length];
      System.arraycopy(forwardType, 0, newForwardType, 0, forwardType.length);
      forwardType = newForwardType;
    }

    public void addForward(int forwardPos, int forwardIndex, int forwardID, TokenType forwardType) {
      if (forwardCount == this.forwardID.length) {
        growForward();
      }
      this.forwardPos[forwardCount] = forwardPos;
      this.forwardIndex[forwardCount] = forwardIndex;
      this.forwardID[forwardCount] = forwardID;
      this.forwardType[forwardCount] = forwardType;
      forwardCount++;
    }

    @Override
    public void reset() {
      super.reset();
      // forwardCount naturally resets after it runs:
      assert forwardCount == 0 : "pos=" + getPos() + " forwardCount=" + forwardCount;
    }

    public int getForwardCount() {
      return forwardCount;
    }

    public void setForwardCount(int forwardCount) {
      this.forwardCount = forwardCount;
    }

    public TokenType getForwardType(int index) {
      return forwardType[index];
    }

    public int getForwardID(int index) {
      return forwardID[index];
    }

    public int getForwardPos(int index) {
      return forwardPos[index];
    }
  }

  /** Yet another lattice data structure for keeping n-best path. */
  protected static final class Lattice<U extends MorphData> {
    private char[] fragment;
    private EnumMap<TokenType, Dictionary<? extends U>> dictionaryMap;
    private boolean useEOS;

    private int rootCapacity = 0;
    private int rootSize = 0;
    private int rootBase = 0;

    // root pointers of node chain by leftChain_ that have same start offset.
    private int[] lRoot;
    // root pointers of node chain by rightChain_ that have same end offset.
    private int[] rRoot;

    private int capacity = 0;
    private int nodeCount = 0;

    // The variables below are elements of lattice node that indexed by node number.
    private TokenType[] nodeDicType;
    private int[] nodeWordID;
    // nodeMark - -1:excluded, 0:unused, 1:bestpath, 2:2-best-path, ... N:N-best-path
    private int[] nodeMark;
    private int[] nodeLeftID;
    private int[] nodeRightID;
    private int[] nodeWordCost;
    private int[] nodeLeftCost;
    private int[] nodeRightCost;
    // nodeLeftNode, nodeRightNode - are left/right node number with minimum cost path.
    private int[] nodeLeftNode;
    private int[] nodeRightNode;
    // nodeLeft, nodeRight - start/end offset
    private int[] nodeLeft;
    private int[] nodeRight;
    private int[] nodeLeftChain;
    private int[] nodeRightChain;

    public int getNodeLeft(int node) {
      return nodeLeft[node];
    }

    public int getNodeRight(int node) {
      return nodeRight[node];
    }

    public TokenType getNodeDicType(int node) {
      return nodeDicType[node];
    }

    public int getNodeWordID(int node) {
      return nodeWordID[node];
    }

    public int getRootBase() {
      return rootBase;
    }

    private void setupRoot(int baseOffset, int lastOffset) {
      assert baseOffset <= lastOffset;
      int size = lastOffset - baseOffset + 1;
      if (rootCapacity < size) {
        int oversize = ArrayUtil.oversize(size, Integer.BYTES);
        lRoot = new int[oversize];
        rRoot = new int[oversize];
        rootCapacity = oversize;
      }
      Arrays.fill(lRoot, 0, size, -1);
      Arrays.fill(rRoot, 0, size, -1);
      rootSize = size;
      rootBase = baseOffset;
    }

    // Reserve at least N nodes.
    private void reserve(int n) {
      if (capacity < n) {
        int oversize = ArrayUtil.oversize(n, Integer.BYTES);
        nodeDicType = new TokenType[oversize];
        nodeWordID = new int[oversize];
        nodeMark = new int[oversize];
        nodeLeftID = new int[oversize];
        nodeRightID = new int[oversize];
        nodeWordCost = new int[oversize];
        nodeLeftCost = new int[oversize];
        nodeRightCost = new int[oversize];
        nodeLeftNode = new int[oversize];
        nodeRightNode = new int[oversize];
        nodeLeft = new int[oversize];
        nodeRight = new int[oversize];
        nodeLeftChain = new int[oversize];
        nodeRightChain = new int[oversize];
        capacity = oversize;
      }
    }

    private void setupNodePool(int n) {
      reserve(n);
      nodeCount = 0;
      if (VERBOSE) {
        System.out.printf("DEBUG: setupNodePool: n = %d\n", n);
        System.out.printf("DEBUG: setupNodePool: lattice.capacity = %d\n", capacity);
      }
    }

    private int addNode(TokenType dicType, int wordID, int left, int right) {
      if (VERBOSE) {
        System.out.printf(
            "DEBUG: addNode: dicType=%s, wordID=%d, left=%d, right=%d, str=%s\n",
            dicType.toString(),
            wordID,
            left,
            right,
            left == -1 ? "BOS" : right == -1 ? "EOS" : new String(fragment, left, right - left));
      }
      assert nodeCount < capacity;
      assert left == -1 || right == -1 || left < right;
      assert left == -1 || (0 <= left && left < rootSize);
      assert right == -1 || (0 <= right && right < rootSize);

      int node = nodeCount++;

      if (VERBOSE) {
        System.out.printf("DEBUG: addNode: node=%d\n", node);
      }

      nodeDicType[node] = dicType;
      nodeWordID[node] = wordID;
      nodeMark[node] = 0;

      if (wordID < 0) {
        nodeWordCost[node] = 0;
        nodeLeftCost[node] = 0;
        nodeRightCost[node] = 0;
        nodeLeftID[node] = 0;
        nodeRightID[node] = 0;
      } else {
        Dictionary<? extends MorphData> dic = dictionaryMap.get(dicType);
        nodeWordCost[node] = dic.getWordCost(wordID);
        nodeLeftID[node] = dic.getLeftId(wordID);
        nodeRightID[node] = dic.getRightId(wordID);
      }

      if (VERBOSE) {
        System.out.printf(
            "DEBUG: addNode: wordCost=%d, leftID=%d, rightID=%d\n",
            nodeWordCost[node], nodeLeftID[node], nodeRightID[node]);
      }

      nodeLeft[node] = left;
      nodeRight[node] = right;
      if (0 <= left) {
        nodeLeftChain[node] = lRoot[left];
        lRoot[left] = node;
      } else {
        nodeLeftChain[node] = -1;
      }
      if (0 <= right) {
        nodeRightChain[node] = rRoot[right];
        rRoot[right] = node;
      } else {
        nodeRightChain[node] = -1;
      }
      return node;
    }

    // Sum of positions.get(i).count in [beg, end) range.
    // using stream:
    //   return IntStream.range(beg, end).map(i -> positions.get(i).count).sum();
    private int positionCount(WrappedPositionArray<PositionNBest> positions, int beg, int end) {
      int count = 0;
      for (int i = beg; i < end; ++i) {
        count += positions.get(i).getCount();
      }
      return count;
    }

    void setup(
        char[] fragment,
        EnumMap<TokenType, Dictionary<? extends U>> dictionaryMap,
        WrappedPositionArray<PositionNBest> positions,
        int prevOffset,
        int endOffset,
        boolean useEOS) {
      assert positions.get(prevOffset).getCount() == 1;
      if (VERBOSE) {
        System.out.printf("DEBUG: setup: prevOffset=%d, endOffset=%d\n", prevOffset, endOffset);
      }

      this.fragment = fragment;
      this.dictionaryMap = dictionaryMap;
      this.useEOS = useEOS;

      // Initialize lRoot and rRoot.
      setupRoot(prevOffset, endOffset);

      // "+ 2" for first/last record.
      setupNodePool(positionCount(positions, prevOffset + 1, endOffset + 1) + 2);

      // substitute for BOS = 0
      Position first = positions.get(prevOffset);
      if (addNode(first.getBackType(0), first.getBackID(0), -1, 0) != 0) {
        assert false;
      }

      // EOS = 1
      if (addNode(TokenType.KNOWN, -1, endOffset - rootBase, -1) != 1) {
        assert false;
      }

      for (int offset = endOffset; prevOffset < offset; --offset) {
        int right = offset - rootBase;
        // optimize: exclude disconnected nodes.
        if (0 <= lRoot[right]) {
          Position pos = positions.get(offset);
          for (int i = 0; i < pos.getCount(); ++i) {
            addNode(pos.getBackType(i), pos.getBackID(i), pos.getBackPos(i) - rootBase, right);
          }
        }
      }
    }

    // set mark = -1 for unreachable nodes.
    void markUnreachable() {
      for (int index = 1; index < rootSize - 1; ++index) {
        if (rRoot[index] < 0) {
          for (int node = lRoot[index]; 0 <= node; node = nodeLeftChain[node]) {
            if (VERBOSE) {
              System.out.printf("DEBUG: markUnreachable: node=%d\n", node);
            }
            nodeMark[node] = -1;
          }
        }
      }
    }

    private int connectionCost(ConnectionCosts costs, int left, int right) {
      int leftID = nodeLeftID[right];
      return ((leftID == 0 && !useEOS) ? 0 : costs.get(nodeRightID[left], leftID));
    }

    void calcLeftCost(ConnectionCosts costs) {
      for (int index = 0; index < rootSize; ++index) {
        for (int node = lRoot[index]; 0 <= node; node = nodeLeftChain[node]) {
          if (0 <= nodeMark[node]) {
            int leastNode = -1;
            int leastCost = Integer.MAX_VALUE;
            for (int leftNode = rRoot[index]; 0 <= leftNode; leftNode = nodeRightChain[leftNode]) {
              if (0 <= nodeMark[leftNode]) {
                int cost =
                    nodeLeftCost[leftNode]
                        + nodeWordCost[leftNode]
                        + connectionCost(costs, leftNode, node);
                if (cost < leastCost) {
                  leastCost = cost;
                  leastNode = leftNode;
                }
              }
            }
            assert 0 <= leastNode;
            nodeLeftNode[node] = leastNode;
            nodeLeftCost[node] = leastCost;
            if (VERBOSE) {
              System.out.printf(
                  "DEBUG: calcLeftCost: node=%d, leftNode=%d, leftCost=%d\n",
                  node, nodeLeftNode[node], nodeLeftCost[node]);
            }
          }
        }
      }
    }

    void calcRightCost(ConnectionCosts costs) {
      for (int index = rootSize - 1; 0 <= index; --index) {
        for (int node = rRoot[index]; 0 <= node; node = nodeRightChain[node]) {
          if (0 <= nodeMark[node]) {
            int leastNode = -1;
            int leastCost = Integer.MAX_VALUE;
            for (int rightNode = lRoot[index];
                0 <= rightNode;
                rightNode = nodeLeftChain[rightNode]) {
              if (0 <= nodeMark[rightNode]) {
                int cost =
                    nodeRightCost[rightNode]
                        + nodeWordCost[rightNode]
                        + connectionCost(costs, node, rightNode);
                if (cost < leastCost) {
                  leastCost = cost;
                  leastNode = rightNode;
                }
              }
            }
            assert 0 <= leastNode;
            nodeRightNode[node] = leastNode;
            nodeRightCost[node] = leastCost;
            if (VERBOSE) {
              System.out.printf(
                  "DEBUG: calcRightCost: node=%d, rightNode=%d, rightCost=%d\n",
                  node, nodeRightNode[node], nodeRightCost[node]);
            }
          }
        }
      }
    }

    // Mark all nodes that have same text and different par-of-speech or reading.
    void markSameSpanNode(int refNode, int value) {
      int left = nodeLeft[refNode];
      int right = nodeRight[refNode];
      for (int node = lRoot[left]; 0 <= node; node = nodeLeftChain[node]) {
        if (nodeRight[node] == right) {
          nodeMark[node] = value;
        }
      }
    }

    IntArrayList bestPathNodeList() {
      IntArrayList list = new IntArrayList();
      for (int node = nodeRightNode[0]; node != 1; node = nodeRightNode[node]) {
        list.add(node);
        markSameSpanNode(node, 1);
      }
      return list;
    }

    private int cost(int node) {
      return nodeLeftCost[node] + nodeWordCost[node] + nodeRightCost[node];
    }

    IntArrayList nBestNodeList(int N) {
      IntArrayList list = new IntArrayList();
      int leastCost = Integer.MAX_VALUE;
      int leastLeft = -1;
      int leastRight = -1;
      for (int node = 2; node < nodeCount; ++node) {
        if (nodeMark[node] == 0) {
          int cost = cost(node);
          if (cost < leastCost) {
            leastCost = cost;
            leastLeft = nodeLeft[node];
            leastRight = nodeRight[node];
            list.clear();
            list.add(node);
          } else if (cost == leastCost
              && (nodeLeft[node] != leastLeft || nodeRight[node] != leastRight)) {
            list.add(node);
          }
        }
      }
      for (IntCursor node : list) {
        markSameSpanNode(node.value, N);
      }
      return list;
    }

    int bestCost() {
      return nodeLeftCost[1];
    }

    int probeDelta(int start, int end) {
      int left = start - rootBase;
      int right = end - rootBase;
      if (left < 0 || rootSize < right) {
        return Integer.MAX_VALUE;
      }
      int probedCost = Integer.MAX_VALUE;
      for (int node = lRoot[left]; 0 <= node; node = nodeLeftChain[node]) {
        if (nodeRight[node] == right) {
          probedCost = Math.min(probedCost, cost(node));
        }
      }
      return probedCost - bestCost();
    }

    void debugPrint() {
      if (VERBOSE) {
        for (int node = 0; node < nodeCount; ++node) {
          System.out.printf(
              "DEBUG NODE: node=%d, mark=%d, cost=%d, left=%d, right=%d\n",
              node, nodeMark[node], cost(node), nodeLeft[node], nodeRight[node]);
        }
      }
    }
  }
}
