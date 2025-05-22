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
package org.apache.lucene.codecs.lucene103.blocktree;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.function.BiConsumer;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

/**
 * A builder to build prefix tree (trie) as the index of block tree, and can be saved to disk.
 *
 * <p>TODO make this trie builder a more memory efficient structure.
 */
class TrieBuilder {

  static final int SIGN_NO_CHILDREN = 0x00;
  static final int SIGN_SINGLE_CHILD_WITH_OUTPUT = 0x01;
  static final int SIGN_SINGLE_CHILD_WITHOUT_OUTPUT = 0x02;
  static final int SIGN_MULTI_CHILDREN = 0x03;

  static final int LEAF_NODE_HAS_TERMS = 1 << 5;
  static final int LEAF_NODE_HAS_FLOOR = 1 << 6;
  static final long NON_LEAF_NODE_HAS_TERMS = 1L << 1;
  static final long NON_LEAF_NODE_HAS_FLOOR = 1L << 0;

  /**
   * The output describing the term block the prefix point to.
   *
   * @param fp the file pointer to the on-disk terms block which a trie node points to.
   * @param hasTerms false if this on-disk block consists entirely of pointers to child blocks.
   * @param floorData will be non-null when a large block of terms sharing a single trie prefix is
   *     split into multiple on-disk blocks.
   */
  record Output(long fp, boolean hasTerms, BytesRef floorData) {}

  private enum Status {
    BUILDING,
    SAVED,
    DESTROYED
  }

  private static class Node {

    // The utf8 digit that leads to this Node, 0 for root node
    private final int label;
    // The output of this node.
    private Output output;
    // The number of children of this node.
    private int childrenNum;
    // Pointers to relative nodes
    private Node next;
    private Node firstChild;
    private Node lastChild;

    // Vars used during saving:

    // The file pointer point to where the node saved. -1 means the node has not been saved.
    private long fp = -1;
    // The latest child that have been saved. null means no child has been saved.
    private Node savedTo;

    Node(int label, Output output) {
      this.label = label;
      this.output = output;
    }
  }

  private final Node root = new Node(0, null);
  private final BytesRef minKey;
  private BytesRef maxKey;
  private Status status = Status.BUILDING;

  static TrieBuilder bytesRefToTrie(BytesRef k, Output v) {
    return new TrieBuilder(k, v);
  }

  private TrieBuilder(BytesRef k, Output v) {
    minKey = maxKey = BytesRef.deepCopyOf(k);
    if (k.length == 0) {
      root.output = v;
      return;
    }
    Node parent = root;
    for (int i = 0; i < k.length; i++) {
      int b = k.bytes[i + k.offset] & 0xFF;
      Output output = i == k.length - 1 ? v : null;
      Node node = new Node(b, output);
      parent.firstChild = parent.lastChild = node;
      parent.childrenNum = 1;
      parent = node;
    }
  }

  /**
   * Append all (K, V) pairs from the given trie into this one. The given trie builder need to
   * ensure its keys greater or equals than max key of this one.
   *
   * <p>Note: the given trie will be destroyed after appending.
   */
  void append(TrieBuilder trieBuilder) {
    if (status != Status.BUILDING || trieBuilder.status != Status.BUILDING) {
      throw new IllegalStateException(
          "tries have wrong status, got this: " + status + ", append: " + trieBuilder.status);
    }
    assert this.maxKey.compareTo(trieBuilder.minKey) < 0;

    int mismatch =
        Arrays.mismatch(
            this.maxKey.bytes,
            this.maxKey.offset,
            this.maxKey.offset + this.maxKey.length,
            trieBuilder.minKey.bytes,
            trieBuilder.minKey.offset,
            trieBuilder.minKey.offset + trieBuilder.minKey.length);
    Node a = this.root;
    Node b = trieBuilder.root;

    for (int i = 0; i < mismatch; i++) {
      final Node aLast = a.lastChild;
      final Node bFirst = b.firstChild;
      assert aLast.label == bFirst.label;

      if (b.childrenNum > 1) {
        aLast.next = bFirst.next;
        a.childrenNum += b.childrenNum - 1;
        a.lastChild = b.lastChild;
        assert assertChildrenLabelInOrder(a);
      }

      a = aLast;
      b = bFirst;
    }

    assert b.childrenNum > 0;
    if (a.childrenNum == 0) {
      a.firstChild = b.firstChild;
      a.lastChild = b.lastChild;
      a.childrenNum = b.childrenNum;
    } else {
      assert a.lastChild.label < b.firstChild.label;
      a.lastChild.next = b.firstChild;
      a.lastChild = b.lastChild;
      a.childrenNum += b.childrenNum;
    }
    assert assertChildrenLabelInOrder(a);

    this.maxKey = trieBuilder.maxKey;
    trieBuilder.status = Status.DESTROYED;
  }

  Output getEmptyOutput() {
    return root.output;
  }

  /**
   * Used for tests only. The recursive impl need to be avoided if someone plans to use for
   * production one day.
   */
  void visit(BiConsumer<BytesRef, Output> consumer) {
    assert status == Status.BUILDING;
    if (root.output != null) {
      consumer.accept(new BytesRef(), root.output);
    }
    visit(root.firstChild, new BytesRefBuilder(), consumer);
  }

  private void visit(Node first, BytesRefBuilder key, BiConsumer<BytesRef, Output> consumer) {
    while (first != null) {
      key.append((byte) first.label);
      if (first.output != null) {
        consumer.accept(key.toBytesRef(), first.output);
      }
      visit(first.firstChild, key, consumer);
      key.setLength(key.length() - 1);
      first = first.next;
    }
  }

  void save(DataOutput meta, IndexOutput index) throws IOException {
    if (status != Status.BUILDING) {
      throw new IllegalStateException("only unsaved trie can be saved, got: " + status);
    }
    meta.writeVLong(index.getFilePointer());
    saveNodes(index);
    meta.writeVLong(root.fp);
    index.writeLong(0L); // additional 8 bytes for over-reading
    meta.writeVLong(index.getFilePointer());
    status = Status.SAVED;
  }

  void saveNodes(IndexOutput index) throws IOException {
    final long startFP = index.getFilePointer();
    Deque<Node> stack = new ArrayDeque<>();
    stack.push(root);

    // Visit and save nodes of this trie in a post-order depth-first traversal.
    while (stack.isEmpty() == false) {
      Node node = stack.peek();
      assert node.fp == -1;
      assert assertChildrenLabelInOrder(node);

      final int childrenNum = node.childrenNum;

      if (childrenNum == 0) { // leaf node
        assert node.output != null : "leaf nodes should have output.";

        node.fp = index.getFilePointer() - startFP;
        stack.pop();

        // [n bytes] floor data
        // [n bytes] output fp
        // [1bit] x | [1bit] has floor | [1bit] has terms | [3bit] output fp bytes | [2bit] sign

        Output output = node.output;
        int outputFpBytes = bytesRequiredVLong(output.fp);
        int header =
            SIGN_NO_CHILDREN
                | ((outputFpBytes - 1) << 2)
                | (output.hasTerms ? LEAF_NODE_HAS_TERMS : 0)
                | (output.floorData != null ? LEAF_NODE_HAS_FLOOR : 0);
        index.writeByte(((byte) header));
        writeLongNBytes(output.fp, outputFpBytes, index);
        if (output.floorData != null) {
          index.writeBytes(
              output.floorData.bytes, output.floorData.offset, output.floorData.length);
        }
        continue;
      }

      // If there are any children have not been saved, push the first one into stack and continue.
      // We want to ensure saving children before parent.

      if (node.savedTo == null) {
        node.savedTo = node.firstChild;
        stack.push(node.savedTo);
        continue;
      }
      if (node.savedTo.next != null) {
        assert node.savedTo.fp >= 0;
        node.savedTo = node.savedTo.next;
        stack.push(node.savedTo);
        continue;
      }

      // All children have been written, now it's time to write the parent!

      assert assertNonLeafNodePreparingSaving(node);
      node.fp = index.getFilePointer() - startFP;
      stack.pop();

      if (childrenNum == 1) {

        // [n bytes] floor data
        // [n bytes] encoded output fp | [n bytes] child fp | [1 byte] label
        // [3bit] encoded output fp bytes | [3bit] child fp bytes | [2bit] sign

        long childDeltaFp = node.fp - node.firstChild.fp;
        assert childDeltaFp > 0 : "parent node is always written after children: " + childDeltaFp;
        int childFpBytes = bytesRequiredVLong(childDeltaFp);
        int encodedOutputFpBytes =
            node.output == null ? 0 : bytesRequiredVLong(node.output.fp << 2);

        // TODO if we have only one child and no output, we can store child labels in this node.
        // E.g. for a single term trie [foobar], we can save only two nodes [fooba] and [r]

        int sign =
            node.output != null ? SIGN_SINGLE_CHILD_WITH_OUTPUT : SIGN_SINGLE_CHILD_WITHOUT_OUTPUT;
        int header = sign | ((childFpBytes - 1) << 2) | ((encodedOutputFpBytes - 1) << 5);
        index.writeByte((byte) header);
        index.writeByte((byte) node.firstChild.label);
        writeLongNBytes(childDeltaFp, childFpBytes, index);

        if (node.output != null) {
          Output output = node.output;
          long encodedFp = encodeFP(output);
          writeLongNBytes(encodedFp, encodedOutputFpBytes, index);
          if (output.floorData != null) {
            index.writeBytes(
                output.floorData.bytes, output.floorData.offset, output.floorData.length);
          }
        }
      } else {

        // [n bytes] floor data
        // [n bytes] children fps | [n bytes] strategy data
        // [1 byte] children count (if floor data) | [n bytes] encoded output fp | [1 byte] label
        // [5bit] strategy bytes | 2bit children strategy | [3bit] encoded output fp bytes
        // [1bit] has output | [3bit] children fp bytes | [2bit] sign

        final int minLabel = node.firstChild.label;
        final int maxLabel = node.lastChild.label;
        assert maxLabel > minLabel;
        ChildSaveStrategy childSaveStrategy =
            ChildSaveStrategy.choose(minLabel, maxLabel, childrenNum);
        int strategyBytes = childSaveStrategy.needBytes(minLabel, maxLabel, childrenNum);
        assert strategyBytes > 0 && strategyBytes <= 32;

        // children fps are in order, so the first child's fp is min, then delta is max.
        long maxChildDeltaFp = node.fp - node.firstChild.fp;
        assert maxChildDeltaFp > 0 : "parent always written after all children";

        int childrenFpBytes = bytesRequiredVLong(maxChildDeltaFp);
        int encodedOutputFpBytes =
            node.output == null ? 1 : bytesRequiredVLong(node.output.fp << 2);
        int header =
            SIGN_MULTI_CHILDREN
                | ((childrenFpBytes - 1) << 2)
                | ((node.output != null ? 1 : 0) << 5)
                | ((encodedOutputFpBytes - 1) << 6)
                | (childSaveStrategy.code << 9)
                | ((strategyBytes - 1) << 11)
                | (minLabel << 16);

        writeLongNBytes(header, 3, index);

        if (node.output != null) {
          Output output = node.output;
          long encodedFp = encodeFP(output);
          writeLongNBytes(encodedFp, encodedOutputFpBytes, index);
          if (output.floorData != null) {
            // We need this childrenNum to compute where the floor data start.
            index.writeByte((byte) (childrenNum - 1));
          }
        }

        long strategyStartFp = index.getFilePointer();
        childSaveStrategy.save(node, childrenNum, strategyBytes, index);
        assert index.getFilePointer() == strategyStartFp + strategyBytes
            : childSaveStrategy.name()
                + " strategy bytes compute error, computed: "
                + strategyBytes
                + " actual: "
                + (index.getFilePointer() - strategyStartFp);

        for (Node child = node.firstChild; child != null; child = child.next) {
          assert node.fp > child.fp : "parent always written after all children";
          writeLongNBytes(node.fp - child.fp, childrenFpBytes, index);
        }

        if (node.output != null && node.output.floorData != null) {
          BytesRef floorData = node.output.floorData;
          index.writeBytes(floorData.bytes, floorData.offset, floorData.length);
        }
      }
    }
  }

  private long encodeFP(Output output) {
    assert output.fp < 1L << 62;
    return (output.floorData != null ? NON_LEAF_NODE_HAS_FLOOR : 0)
        | (output.hasTerms ? NON_LEAF_NODE_HAS_TERMS : 0)
        | (output.fp << 2);
  }

  private static int bytesRequiredVLong(long v) {
    return Long.BYTES - (Long.numberOfLeadingZeros(v | 1) >>> 3);
  }

  /**
   * Write the first (LSB order) n bytes of the given long v into the DataOutput.
   *
   * <p>This differs from writeVLong because it can write more bytes than would be needed for vLong
   * when the incoming int n is larger.
   */
  private static void writeLongNBytes(long v, int n, DataOutput out) throws IOException {
    for (int i = 0; i < n; i++) {
      // Note that we sometimes write trailing 0 bytes here, when the incoming int n is bigger than
      // would be required for a "normal" vLong
      out.writeByte((byte) v);
      v >>>= 8;
    }
    assert v == 0;
  }

  private static boolean assertChildrenLabelInOrder(Node node) {
    if (node.childrenNum == 0) {
      assert node.firstChild == null;
      assert node.lastChild == null;
    } else if (node.childrenNum == 1) {
      assert node.firstChild == node.lastChild;
      assert node.firstChild.next == null;
    } else if (node.childrenNum > 1) {
      int n = 0;
      for (Node child = node.firstChild; child != null; child = child.next) {
        n++;
        assert child.next == null || child.label < child.next.label
            : " the label of children nodes should always be in strictly increasing order.";
      }
      assert node.childrenNum == n;
    }
    return true;
  }

  private static boolean assertNonLeafNodePreparingSaving(Node node) {
    assert assertChildrenLabelInOrder(node);
    assert node.childrenNum != 0;
    if (node.childrenNum == 1) {
      assert node.firstChild == node.lastChild;
      assert node.firstChild.next == null;
      assert node.savedTo == node.firstChild;
      assert node.firstChild.fp >= 0;
    } else {
      int n = 0;
      for (Node child = node.firstChild; child != null; child = child.next) {
        n++;
        assert child.fp >= 0;
        assert child.next == null || child.fp < child.next.fp
            : " the fp or children nodes should always be in order.";
      }
      assert node.childrenNum == n;
      assert node.lastChild == node.savedTo;
      assert node.savedTo.next == null;
    }
    return true;
  }

  enum ChildSaveStrategy {

    /**
     * Store children labels in a bitset, this is likely the most efficient storage as we can
     * compute position with bitCount instruction, so we give it the highest priority.
     */
    BITS(2) {
      @Override
      int needBytes(int minLabel, int maxLabel, int labelCnt) {
        int byteDistance = maxLabel - minLabel + 1;
        return (byteDistance + 7) >>> 3;
      }

      @Override
      void save(Node parent, int labelCnt, int strategyBytes, IndexOutput output)
          throws IOException {
        byte presenceBits = 1; // The first arc is always present.
        int presenceIndex = 0;
        int previousLabel = parent.firstChild.label;
        for (Node child = parent.firstChild.next; child != null; child = child.next) {
          int label = child.label;
          assert label > previousLabel;
          presenceIndex += label - previousLabel;
          while (presenceIndex >= Byte.SIZE) {
            output.writeByte(presenceBits);
            presenceBits = 0;
            presenceIndex -= Byte.SIZE;
          }
          // Set the bit at presenceIndex to flag that the corresponding arc is present.
          presenceBits |= 1 << presenceIndex;
          previousLabel = label;
        }
        assert presenceIndex == (parent.lastChild.label - parent.firstChild.label) % 8;
        assert presenceBits != 0; // The last byte is not 0.
        assert (presenceBits & (1 << presenceIndex)) != 0; // The last arc is always present.
        output.writeByte(presenceBits);
      }

      @Override
      int lookup(
          int targetLabel, RandomAccessInput in, long offset, int strategyBytes, int minLabel)
          throws IOException {
        int bitIndex = targetLabel - minLabel;
        if (bitIndex >= (strategyBytes << 3)) {
          return -1;
        }
        int wordIndex = bitIndex >>> 6;
        long wordFp = offset + (wordIndex << 3);
        long word = in.readLong(wordFp);
        long mask = 1L << bitIndex;
        if ((word & mask) == 0) {
          return -1;
        }
        int pos = 0;
        for (long fp = offset; fp < wordFp; fp += 8L) {
          pos += Long.bitCount(in.readLong(fp));
        }
        pos += Long.bitCount(word & (mask - 1));
        return pos;
      }
    },

    /**
     * Store labels in an array and lookup with binary search.
     *
     * <p>TODO: Can we use VectorAPI to speed up the lookup? we can check 64 labels once on AVX512!
     */
    ARRAY(1) {
      @Override
      int needBytes(int minLabel, int maxLabel, int labelCnt) {
        return labelCnt - 1; // min label saved
      }

      @Override
      void save(Node parent, int labelCnt, int strategyBytes, IndexOutput output)
          throws IOException {
        for (Node child = parent.firstChild.next; child != null; child = child.next) {
          output.writeByte((byte) child.label);
        }
      }

      @Override
      int lookup(
          int targetLabel, RandomAccessInput in, long offset, int strategyBytes, int minLabel)
          throws IOException {
        int low = 0;
        int high = strategyBytes - 1;
        while (low <= high) {
          int mid = (low + high) >>> 1;
          int midLabel = in.readByte(offset + mid) & 0xFF;
          if (midLabel < targetLabel) {
            low = mid + 1;
          } else if (midLabel > targetLabel) {
            high = mid - 1;
          } else {
            return mid + 1; // min label not included, plus 1
          }
        }
        return -1;
      }
    },

    /**
     * Store labels that not existing within the range. E.g. store 10(max label) and 3, 5(absent
     * label) for [1, 2, 4, 6, 7, 8, 9, 10].
     *
     * <p>TODO: Can we use VectorAPI to speed up the lookup? we can check 64 labels once on AVX512!
     */
    REVERSE_ARRAY(0) {

      @Override
      int needBytes(int minLabel, int maxLabel, int labelCnt) {
        int byteDistance = maxLabel - minLabel + 1;
        return byteDistance - labelCnt + 1;
      }

      @Override
      void save(Node parent, int labelCnt, int strategyBytes, IndexOutput output)
          throws IOException {
        output.writeByte((byte) parent.lastChild.label);
        int lastLabel = parent.firstChild.label;
        for (Node child = parent.firstChild.next; child != null; child = child.next) {
          while (++lastLabel < child.label) {
            output.writeByte((byte) lastLabel);
          }
        }
      }

      @Override
      int lookup(
          int targetLabel, RandomAccessInput in, long offset, int strategyBytes, int minLabel)
          throws IOException {
        int maxLabel = in.readByte(offset++) & 0xFF;
        if (targetLabel >= maxLabel) {
          return targetLabel == maxLabel ? maxLabel - minLabel - strategyBytes + 1 : -1;
        }
        if (strategyBytes == 1) {
          return targetLabel - minLabel;
        }

        int low = 0;
        int high = strategyBytes - 2;
        while (low <= high) {
          int mid = (low + high) >>> 1;
          int midLabel = in.readByte(offset + mid) & 0xFF;
          if (midLabel < targetLabel) {
            low = mid + 1;
          } else if (midLabel > targetLabel) {
            high = mid - 1;
          } else {
            return -1;
          }
        }
        return targetLabel - minLabel - low;
      }
    };

    private static final ChildSaveStrategy[] STRATEGIES_IN_PRIORITY_ORDER =
        new ChildSaveStrategy[] {BITS, ARRAY, REVERSE_ARRAY};
    private static final ChildSaveStrategy[] STRATEGIES_BY_CODE;

    static {
      STRATEGIES_BY_CODE = new ChildSaveStrategy[ChildSaveStrategy.values().length];
      for (ChildSaveStrategy strategy : ChildSaveStrategy.values()) {
        assert STRATEGIES_BY_CODE[strategy.code] == null;
        STRATEGIES_BY_CODE[strategy.code] = strategy;
      }
    }

    final int code;

    ChildSaveStrategy(int code) {
      this.code = code;
    }

    abstract int needBytes(int minLabel, int maxLabel, int labelCnt);

    abstract void save(Node parent, int labelCnt, int strategyBytes, IndexOutput output)
        throws IOException;

    abstract int lookup(
        int targetLabel, RandomAccessInput in, long offset, int strategyBytes, int minLabel)
        throws IOException;

    static ChildSaveStrategy byCode(int code) {
      return STRATEGIES_BY_CODE[code];
    }

    static ChildSaveStrategy choose(int minLabel, int maxLabel, int labelCnt) {
      ChildSaveStrategy childSaveStrategy = null;
      int strategyBytes = Integer.MAX_VALUE;
      for (ChildSaveStrategy strategy : ChildSaveStrategy.STRATEGIES_IN_PRIORITY_ORDER) {
        int strategyCost = strategy.needBytes(minLabel, maxLabel, labelCnt);
        if (strategyCost < strategyBytes) {
          childSaveStrategy = strategy;
          strategyBytes = strategyCost;
        }
      }
      assert childSaveStrategy != null;
      assert strategyBytes > 0 && strategyBytes <= 32;
      return childSaveStrategy;
    }
  }
}
