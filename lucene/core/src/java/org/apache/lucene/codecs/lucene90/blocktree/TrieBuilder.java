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
package org.apache.lucene.codecs.lucene90.blocktree;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.function.BiConsumer;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

/** TODO make it a more memory efficient structure */
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
   * @param fp describes the on-disk terms block which a trie node points to.
   * @param hasTerms A boolean which will be false if this on-disk block consists entirely of
   *     pointers to child blocks.
   * @param floorData A {@link BytesRef} which will be non-null when a large block of terms sharing
   *     a single trie prefix is split into multiple on-disk blocks.
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
    // The children listed in order by their utf8 label
    private final LinkedList<Node> children;
    // The output of this node.
    private Output output;

    // Vars used during saving:

    // The file pointer point to where the node saved. -1 means the node has not been saved.
    private long fp = -1;
    // The iterator whose next() point to the first child has not been saved.
    private Iterator<Node> childrenIterator;

    Node(int label, Output output, LinkedList<Node> children) {
      this.label = label;
      this.output = output;
      this.children = children;
    }
  }

  private Status status = Status.BUILDING;
  final Node root = new Node(0, null, new LinkedList<>());

  static TrieBuilder bytesRefToTrie(BytesRef k, Output v) {
    return new TrieBuilder(k, v);
  }

  private TrieBuilder(BytesRef k, Output v) {
    if (k.length == 0) {
      root.output = v;
      return;
    }
    Node parent = root;
    for (int i = 0; i < k.length; i++) {
      int b = k.bytes[i + k.offset] & 0xFF;
      Output output = i == k.length - 1 ? v : null;
      Node node = new Node(b, output, new LinkedList<>());
      parent.children.add(node);
      parent = node;
    }
  }

  /**
   * Put all (K, V) pair from the given trie into this one. Output of current trie will be
   * overwritten if the given trie has the same K.
   *
   * <p>Note: the given trie will be destroyed after absorbing.
   */
  void absorb(TrieBuilder trieBuilder) {
    if (status != Status.BUILDING || trieBuilder.status != Status.BUILDING) {
      throw new IllegalStateException("tries should be unsaved");
    }
    // Use a simple stack to avoid recursion.
    Deque<Runnable> stack = new ArrayDeque<>();
    stack.add(() -> absorb(this.root, trieBuilder.root, stack));
    while (!stack.isEmpty()) {
      stack.pop().run();
    }
    trieBuilder.status = Status.DESTROYED;
  }

  private static void absorb(Node n, Node add, Deque<Runnable> stack) {
    assert n.label == add.label;
    if (add.output != null) {
      if (n.output != null) {
        throw new IllegalStateException("duplicate key found");
      }
      n.output = add.output;
    }
    ListIterator<Node> iter = n.children.listIterator();
    // TODO we can do more efficient if there is no intersection, block tree always do that
    outer:
    for (Node addChild : add.children) {
      while (iter.hasNext()) {
        Node nChild = iter.next();
        if (nChild.label == addChild.label) {
          stack.push(() -> absorb(nChild, addChild, stack));
          continue outer;
        }
        if (nChild.label > addChild.label) {
          iter.previous(); // move back
          iter.add(addChild);
          continue outer;
        }
      }
      iter.add(addChild);
    }
  }

  Output getEmptyOutput() {
    return root.output;
  }

  /** Used for tests only. */
  void visit(BiConsumer<BytesRef, Output> consumer) {
    assert status == Status.BUILDING;
    if (root.output != null) {
      consumer.accept(new BytesRef(), root.output);
    }
    visit(root.children, new BytesRefBuilder(), consumer);
  }

  private void visit(List<Node> nodes, BytesRefBuilder key, BiConsumer<BytesRef, Output> consumer) {
    for (Node node : nodes) {
      key.append((byte) node.label);
      if (node.output != null) {
        consumer.accept(key.toBytesRef(), node.output);
      }
      visit(node.children, key, consumer);
      key.setLength(key.length() - 1);
    }
  }

  void save(DataOutput meta, IndexOutput index) throws IOException {
    if (status != Status.BUILDING) {
      throw new IllegalStateException("only unsaved trie can be saved");
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

    // Visit and save nodes of this trie in a post-order traversal.

    while (stack.isEmpty() == false) {
      Node node = stack.peek();
      assert node.fp == -1;
      final int childrenNum = node.children.size();

      if (childrenNum == 0) {
        assert node.output != null;

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

      if (node.childrenIterator == null) {
        node.childrenIterator = node.children.iterator();
      }
      if (node.childrenIterator.hasNext()) {
        stack.push(node.childrenIterator.next());
        continue;
      }

      // All children have been written, now it's time to write the parent!

      node.fp = index.getFilePointer() - startFP;
      stack.pop();

      if (childrenNum == 1) {

        // [n bytes] floor data
        // [n bytes] encoded output fp | [n bytes] child fp | [1 byte] label
        // [3bit] encoded output fp bytes | [3bit] child fp bytes | [2bit] sign

        long childDeltaFp = node.fp - node.children.getFirst().fp;
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
        index.writeByte((byte) node.children.getFirst().label);
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
        // [n bytes] children fps | [n bytes] position data
        // [1 byte] children count (if floor data) | [n bytes] encoded output fp | [1 byte] label
        // [5bit] position bytes | 2bit children strategy | [3bit] encoded output fp bytes
        // [1bit] has output | [3bit] children fp bytes | [2bit] sign

        final int minLabel = node.children.getFirst().label;
        final int maxLabel = node.children.getLast().label;
        PositionStrategy positionStrategy = null;
        int positionBytes = Integer.MAX_VALUE;
        for (PositionStrategy strategy : PositionStrategy.values()) {
          int strategyCost = strategy.positionBytes(minLabel, maxLabel, childrenNum);
          if (strategyCost < positionBytes) {
            positionStrategy = strategy;
            positionBytes = strategyCost;
          } else if (positionStrategy != null
              && strategyCost == positionBytes
              && strategy.priority > positionStrategy.priority) {
            positionStrategy = strategy;
          }
        }

        assert positionStrategy != null;
        assert positionBytes > 0 && positionBytes <= 32;

        long maxChildDeltaFp = node.fp - node.children.getFirst().fp;
        assert maxChildDeltaFp > 0;
        int childrenFpBytes = bytesRequiredVLong(maxChildDeltaFp);
        int encodedOutputFpBytes =
            node.output == null ? 1 : bytesRequiredVLong(node.output.fp << 2);
        int header =
            SIGN_MULTI_CHILDREN
                | ((childrenFpBytes - 1) << 2)
                | ((node.output != null ? 1 : 0) << 5)
                | ((encodedOutputFpBytes - 1) << 6)
                | (positionStrategy.priority << 9)
                | ((positionBytes - 1) << 11)
                | (minLabel << 16);

        writeLongNBytes(header, 3, index);

        if (node.output != null) {
          Output output = node.output;
          long encodedFp = encodeFP(output);
          writeLongNBytes(encodedFp, encodedOutputFpBytes, index);
          if (output.floorData != null) {
            index.writeByte((byte) (childrenNum - 1));
          }
        }

        long positionStartFp = index.getFilePointer();
        positionStrategy.save(node.children, childrenNum, positionBytes, index);
        assert index.getFilePointer() == positionStartFp + positionBytes
            : positionStrategy.name()
                + " position bytes compute error, computed: "
                + positionBytes
                + " actual: "
                + (index.getFilePointer() - positionStartFp);

        for (Node child : node.children) {
          assert node.fp > child.fp;
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

  private static void writeLongNBytes(long v, int n, DataOutput out) throws IOException {
    for (int i = 0; i < n; i++) {
      out.writeByte((byte) v);
      v >>= 8;
    }
  }

  enum PositionStrategy {

    /**
     * Store children labels in a bitset, this is likely the most efficient storage as we can
     * compute position with bitCount instruction, so we give it the highest priority.
     */
    BITS(2) {
      @Override
      int positionBytes(int minLabel, int maxLabel, int labelCnt) {
        int byteDistance = maxLabel - minLabel + 1;
        return (byteDistance + 7) >>> 3;
      }

      @Override
      void save(List<Node> children, int labelCnt, int positionBytes, IndexOutput output)
          throws IOException {
        byte presenceBits = 1; // The first arc is always present.
        int presenceIndex = 0;
        int previousLabel = children.getFirst().label;
        for (int arcIdx = 1; arcIdx < children.size(); arcIdx++) {
          int label = children.get(arcIdx).label;
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
        assert presenceIndex == (children.getLast().label - children.getFirst().label) % 8;
        assert presenceBits != 0; // The last byte is not 0.
        assert (presenceBits & (1 << presenceIndex)) != 0; // The last arc is always present.
        output.writeByte(presenceBits);
      }

      @Override
      int lookup(
          int targetLabel, RandomAccessInput in, long offset, int positionBytes, int minLabel)
          throws IOException {
        int bitIndex = targetLabel - minLabel;
        if (bitIndex >= (positionBytes << 3)) {
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
      int positionBytes(int minLabel, int maxLabel, int labelCnt) {
        return labelCnt - 1; // min label saved
      }

      @Override
      void save(List<Node> children, int labelCnt, int positionBytes, IndexOutput output)
          throws IOException {
        for (int i = 1; i < labelCnt; i++) {
          output.writeByte((byte) children.get(i).label);
        }
      }

      @Override
      int lookup(
          int targetLabel, RandomAccessInput in, long offset, int positionBytes, int minLabel)
          throws IOException {
        int low = 0;
        int high = positionBytes - 1;
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
     * label) for [1, 2, 4, 6, 7, 8, 9, 10]
     *
     * <p>TODO: Can we use VectorAPI to speed up the lookup? we can check 64 labels once on AVX512!
     */
    REVERSE_ARRAY(0) {

      @Override
      int positionBytes(int minLabel, int maxLabel, int labelCnt) {
        int byteDistance = maxLabel - minLabel + 1;
        return byteDistance - labelCnt + 1;
      }

      @Override
      void save(List<Node> children, int labelCnt, int positionBytes, IndexOutput output)
          throws IOException {
        output.writeByte((byte) children.getLast().label);
        int lastLabel = children.getFirst().label;
        for (int i = 1; i < labelCnt; i++) {
          Node node = children.get(i);
          while (++lastLabel < node.label) {
            output.writeByte((byte) lastLabel);
          }
        }
      }

      @Override
      int lookup(
          int targetLabel, RandomAccessInput in, long offset, int positionBytes, int minLabel)
          throws IOException {
        int maxLabel = in.readByte(offset++) & 0xFF;
        if (targetLabel >= maxLabel) {
          return targetLabel == maxLabel ? maxLabel - minLabel - positionBytes + 1 : -1;
        }
        if (positionBytes == 1) {
          return targetLabel - minLabel;
        }

        int low = 0;
        int high = positionBytes - 2;
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

    private static final PositionStrategy[] STRATEGIES = new PositionStrategy[3];

    static {
      for (PositionStrategy strategy : PositionStrategy.values()) {
        STRATEGIES[strategy.priority] = strategy;
      }
    }

    final int priority;

    PositionStrategy(int priority) {
      this.priority = priority;
    }

    abstract int positionBytes(int minLabel, int maxLabel, int labelCnt);

    abstract void save(List<Node> children, int labelCnt, int positionBytes, IndexOutput output)
        throws IOException;

    abstract int lookup(
        int targetLabel, RandomAccessInput in, long offset, int positionBytes, int minLabel)
        throws IOException;

    static PositionStrategy byCode(int code) {
      return STRATEGIES[code];
    }
  }
}
