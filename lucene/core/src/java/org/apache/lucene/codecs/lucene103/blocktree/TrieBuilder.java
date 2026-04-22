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
import java.util.Arrays;
import java.util.function.BiConsumer;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

/**
 * A builder to build prefix tree (trie) as the index of block tree, and can be saved to disk.
 *
 * <p>This implementation stores entries in a compact prefix-coded byte buffer during building, and
 * reconstructs the trie structure on-the-fly during save using a frontier-based approach. The first
 * non-empty-key entry (minKey) is stored separately so that {@link #append} can re-encode only that
 * one entry and then bulk-copy the remaining bytes with zero per-entry overhead.
 *
 * <p>Memory usage is O(total encoded bytes) during building and O(max key depth) during save,
 * compared to the previous tree-based approach which was O(total nodes * ~120 bytes per node).
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

  // ====== Prefix-coded buffer entry format ======
  // Each entry is encoded as:
  //   [prefixLen: vInt] [suffixLen: vInt] [suffix: bytes]
  //   [fp: vLong] [hasTerms: byte (0/1)] [floorDataLen: vInt] [floorData: bytes]
  //
  // The first non-empty-key entry (minKey) is stored separately in minKey/minOutput
  // and NOT written to the buffer. Buffer entries start from the second entry onward,
  // with their prefixLen computed relative to the preceding entry's full key.

  /** Output for the empty ("") key, if any. */
  private Output emptyOutput;

  /** The first non-empty key, stored separately from the buffer. */
  private final BytesRef minKey;

  /** Output for minKey. Null if there is no non-empty key entry. */
  private Output minOutput;

  /** Buffer holding entries from the second onward, prefix-encoded. */
  private final ByteBuffersDataOutput buffer = new ByteBuffersDataOutput();

  /**
   * The last key appended. Since entries are always appended in sorted order, this also serves as
   * the lexicographically largest key (maxKey) for ordered-append assertions. The backing byte[]
   * may be over-allocated; only bytes [0, length) are meaningful.
   */
  private final BytesRef lastKey;

  /** The maximum key length across all entries, used for frontier array allocation. */
  private int maxKeyDepth;

  private Status status = Status.BUILDING;

  static TrieBuilder bytesRefToTrie(BytesRef k, Output v) {
    return new TrieBuilder(k, v);
  }

  private TrieBuilder(BytesRef k, Output v) {
    minKey = BytesRef.deepCopyOf(k);
    maxKeyDepth = k.length;

    if (k.length == 0) {
      emptyOutput = v;
      lastKey = new BytesRef();
    } else {
      minOutput = v;
      lastKey = BytesRef.deepCopyOf(k);
    }
  }

  /**
   * Encode and append a single (key, output) entry into the buffer. The entry is prefix-encoded
   * relative to {@link #lastKey}, and lastKey is updated to the given key afterward.
   */
  private void appendEntry(BytesRef key, int prefixLen, Output v) {
    try {
      int suffixLen = key.length - prefixLen;
      buffer.writeVInt(prefixLen);
      buffer.writeVInt(suffixLen);
      buffer.writeBytes(key.bytes, key.offset + prefixLen, suffixLen);
      buffer.writeVLong(v.fp);
      buffer.writeByte((byte) (v.hasTerms ? 1 : 0));
      if (v.floorData != null) {
        buffer.writeVInt(v.floorData.length);
        buffer.writeBytes(v.floorData.bytes, v.floorData.offset, v.floorData.length);
      } else {
        buffer.writeVInt(0);
      }
    } catch (IOException e) {
      throw new RuntimeException("should not happen on in-memory buffer", e);
    }
    lastKey.bytes = ArrayUtil.growNoCopy(lastKey.bytes, key.length);
    System.arraycopy(key.bytes, key.offset, lastKey.bytes, 0, key.length);
    lastKey.offset = 0;
    lastKey.length = key.length;
    maxKeyDepth = Math.max(maxKeyDepth, key.length);
  }

  /**
   * Append all (K, V) pairs from the given trie into this one. The given trie builder need to
   * ensure its keys greater or equals than max key of this one.
   *
   * <p>Note: the given trie will be destroyed after appending.
   */
  void append(TrieBuilder other) {
    if (status != Status.BUILDING || other.status != Status.BUILDING) {
      throw new IllegalStateException(
          "tries have wrong status, got this: " + status + ", append: " + other.status);
    }
    assert this.lastKey.compareTo(other.minKey) < 0;

    if (other.emptyOutput != null && this.emptyOutput == null) {
      this.emptyOutput = other.emptyOutput;
    }

    if (other.minOutput != null) {
      // Re-encode other's first entry (minKey) relative to our lastKey.
      int mismatch =
          Arrays.mismatch(
              lastKey.bytes,
              lastKey.offset,
              lastKey.offset + lastKey.length,
              other.minKey.bytes,
              other.minKey.offset,
              other.minKey.offset + other.minKey.length);
      if (mismatch == -1) {
        mismatch = Math.min(lastKey.length, other.minKey.length);
      }
      appendEntry(other.minKey, mismatch, other.minOutput);

      // After appendEntry, lastKey == other.minKey. The remaining buffer entries in 'other'
      // are prefix-encoded relative to other.minKey, which is now identical to our lastKey,
      // so we can bulk-copy them directly without any re-encoding.
      if (other.buffer.size() > 0) {
        try {
          ByteBuffersDataInput otherIn = other.buffer.toDataInput();
          buffer.copyBytes(otherIn, otherIn.length());
        } catch (IOException e) {
          throw new RuntimeException("should not happen on in-memory buffer", e);
        }
        // Update lastKey to other's lastKey (the last entry in the bulk-copied data).
        lastKey.bytes = ArrayUtil.growNoCopy(lastKey.bytes, other.lastKey.length);
        System.arraycopy(
            other.lastKey.bytes, other.lastKey.offset, lastKey.bytes, 0, other.lastKey.length);
        lastKey.offset = 0;
        lastKey.length = other.lastKey.length;
      }
    }

    this.maxKeyDepth = Math.max(this.maxKeyDepth, other.maxKeyDepth);
    other.status = Status.DESTROYED;
  }

  Output getEmptyOutput() {
    return emptyOutput;
  }

  // ====== Entry iteration (shared by visit and saveNodes) ======

  /**
   * Iterates over all non-empty-key entries: first the separately-stored minKey entry, then the
   * prefix-coded buffer entries. The iterator maintains a reusable key buffer; callers that need the
   * previous key (e.g. for common-prefix computation) must copy it before calling {@link #next()}.
   *
   * <p>This is a non-static inner class so it can directly access the enclosing TrieBuilder's
   * {@link #minKey}, {@link #minOutput}, {@link #buffer}, and {@link #maxKeyDepth}.
   */
  private class EntryIterator {
    private final ByteBuffersDataInput in;
    private boolean minKeyConsumed;
    byte[] key;
    int keyLen;
    Output output;

    EntryIterator() {
      key = new byte[Math.max(maxKeyDepth, 1)];
      // Pre-populate key buffer with minKey. When minOutput != null, the first next()
      // returns it directly. Either way the buffer is seeded for prefix-decoding
      // subsequent entries (which are encoded relative to minKey).
      System.arraycopy(minKey.bytes, minKey.offset, key, 0, minKey.length);
      keyLen = minKey.length;
      minKeyConsumed = (minOutput == null);
      in = buffer.size() > 0 ? buffer.toDataInput() : null;
    }

    boolean hasNext() {
      if (!minKeyConsumed) return true;
      return in != null && in.position() < in.length();
    }

    void next() throws IOException {
      if (!minKeyConsumed) {
        // key/keyLen are already set from constructor.
        output = minOutput;
        minKeyConsumed = true;
        return;
      }
      int prefixLen = in.readVInt();
      int suffixLen = in.readVInt();
      keyLen = prefixLen + suffixLen;
      key = ArrayUtil.grow(key, keyLen); // must preserve [0, prefixLen)
      in.readBytes(key, prefixLen, suffixLen);

      long fp = in.readVLong();
      boolean hasTerms = in.readByte() == 1;
      int floorDataLen = in.readVInt();
      BytesRef floorData = null;
      if (floorDataLen > 0) {
        byte[] fd = new byte[floorDataLen];
        in.readBytes(fd, 0, floorDataLen);
        floorData = new BytesRef(fd);
      }
      output = new Output(fp, hasTerms, floorData);
    }
  }

  /**
   * Used for tests only. The recursive impl need to be avoided if someone plans to use for
   * production one day.
   */
  void visit(BiConsumer<BytesRef, Output> consumer) {
    assert status == Status.BUILDING;
    if (emptyOutput != null) {
      consumer.accept(new BytesRef(), emptyOutput);
    }
    try {
      EntryIterator iter = new EntryIterator();
      while (iter.hasNext()) {
        iter.next();
        consumer.accept(
            new BytesRef(ArrayUtil.copyOfSubArray(iter.key, 0, iter.keyLen)), iter.output);
      }
    } catch (IOException e) {
      throw new RuntimeException("should not happen on in-memory buffer", e);
    }
  }

  void save(DataOutput meta, IndexOutput index) throws IOException {
    if (status != Status.BUILDING) {
      throw new IllegalStateException("only unsaved trie can be saved, got: " + status);
    }
    meta.writeVLong(index.getFilePointer());
    meta.writeVLong(saveNodes(index));
    index.writeLong(0L); // additional 8 bytes for over-reading
    meta.writeVLong(index.getFilePointer());
    status = Status.SAVED;
  }

  // ====== Frontier-based save ======

  /**
   * A frontier slot for one depth level. During save, frontier[d] represents the trie node at
   * depth d on the path from root to the last inserted key. Only the current path is live; deeper
   * slots are frozen (serialized) and reset as new keys arrive.
   */
  private static class FrontierNode {
    Output output;
    int childrenNum;
    int[] childLabels;
    long[] childFps;

    FrontierNode() {
      childLabels = new int[4];
      childFps = new long[4];
    }

    void reset() {
      output = null;
      childrenNum = 0;
    }

    void addChild(int label, long fp) {
      childLabels = ArrayUtil.grow(childLabels, childrenNum + 1);
      childFps = ArrayUtil.grow(childFps, childrenNum + 1);
      childLabels[childrenNum] = label;
      childFps[childrenNum] = fp;
      childrenNum++;
    }
  }

  long saveNodes(IndexOutput index) throws IOException {
    final long startFP = index.getFilePointer();

    FrontierNode[] frontier = new FrontierNode[maxKeyDepth + 1];
    for (int i = 0; i <= maxKeyDepth; i++) {
      frontier[i] = new FrontierNode();
    }
    frontier[0].output = emptyOutput;

    byte[] prevKey = new byte[Math.max(maxKeyDepth, 1)];
    int prevKeyLen = 0;
    boolean firstEntry = true;

    EntryIterator iter = new EntryIterator();
    while (iter.hasNext()) {
      iter.next();

      if (!firstEntry) {
        int commonPrefix =
            Arrays.mismatch(prevKey, 0, prevKeyLen, iter.key, 0, iter.keyLen);
        if (commonPrefix == -1) {
          commonPrefix = Math.min(prevKeyLen, iter.keyLen);
        }
        freezeFrom(prevKey, prevKeyLen, commonPrefix, frontier, startFP, index);
      }
      firstEntry = false;

      frontier[iter.keyLen].output = iter.output;

      prevKey = ArrayUtil.growNoCopy(prevKey, iter.keyLen);
      System.arraycopy(iter.key, 0, prevKey, 0, iter.keyLen);
      prevKeyLen = iter.keyLen;
    }

    // Freeze all remaining frontier nodes down to depth 1.
    if (!firstEntry) {
      freezeFrom(prevKey, prevKeyLen, 0, frontier, startFP, index);
    }

    // Freeze root.
    return freezeNode(frontier[0], startFP, index);
  }

  /**
   * Freeze frontier nodes from depth {@code keyLen} down to depth {@code toDepth + 1}. Each frozen
   * node's fp is registered as a child of its parent (one level up).
   */
  private void freezeFrom(
      byte[] key, int keyLen, int toDepth, FrontierNode[] frontier, long startFP, IndexOutput index)
      throws IOException {
    for (int d = keyLen; d > toDepth; d--) {
      long fp = freezeNode(frontier[d], startFP, index);
      frontier[d - 1].addChild(key[d - 1] & 0xFF, fp);
      frontier[d].reset();
    }
  }

  /**
   * Serialize a single frontier node to the index output and return its fp (relative to startFP).
   */
  private long freezeNode(FrontierNode node, long startFP, IndexOutput index) throws IOException {
    int childrenNum = node.childrenNum;

    if (childrenNum == 0) {
      assert node.output != null : "leaf nodes should have output.";
      long bottomFp = index.getFilePointer() - startFP;
      writeLeafNode(node.output, index);
      return bottomFp;
    }

    if (childrenNum == 1) {
      long bottomFp = index.getFilePointer() - startFP;
      long childDeltaFp = bottomFp - node.childFps[0];
      assert childDeltaFp > 0 : "parent node is always written after children: " + childDeltaFp;
      int childFpBytes = bytesRequiredVLong(childDeltaFp);
      int encodedOutputFpBytes =
          node.output == null ? 0 : bytesRequiredVLong(node.output.fp << 2);

      int sign =
          node.output != null ? SIGN_SINGLE_CHILD_WITH_OUTPUT : SIGN_SINGLE_CHILD_WITHOUT_OUTPUT;
      int header = sign | ((childFpBytes - 1) << 2) | ((encodedOutputFpBytes - 1) << 5);
      index.writeByte((byte) header);
      index.writeByte((byte) node.childLabels[0]);
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
      return bottomFp;
    }

    // Multi-children
    long bottomFp = index.getFilePointer() - startFP;

    final int minLabel = node.childLabels[0];
    final int maxLabel = node.childLabels[childrenNum - 1];
    assert maxLabel > minLabel;
    ChildSaveStrategy childSaveStrategy =
        ChildSaveStrategy.choose(minLabel, maxLabel, childrenNum);
    int strategyBytes = childSaveStrategy.needBytes(minLabel, maxLabel, childrenNum);
    assert strategyBytes > 0 && strategyBytes <= 32;

    long maxChildDeltaFp = bottomFp - node.childFps[0];
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
        index.writeByte((byte) (childrenNum - 1));
      }
    }

    long strategyStartFp = index.getFilePointer();
    childSaveStrategy.save(node.childLabels, childrenNum, strategyBytes, index);
    assert index.getFilePointer() == strategyStartFp + strategyBytes
        : childSaveStrategy.name()
        + " strategy bytes compute error, computed: "
        + strategyBytes
        + " actual: "
        + (index.getFilePointer() - strategyStartFp);

    for (int i = 0; i < childrenNum; i++) {
      assert bottomFp > node.childFps[i] : "parent always written after all children";
      writeLongNBytes(bottomFp - node.childFps[i], childrenFpBytes, index);
    }

    if (node.output != null && node.output.floorData != null) {
      BytesRef floorData = node.output.floorData;
      index.writeBytes(floorData.bytes, floorData.offset, floorData.length);
    }

    return bottomFp;
  }

  private void writeLeafNode(Output output, IndexOutput index) throws IOException {
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
      out.writeByte((byte) v);
      v >>>= 8;
    }
    assert v == 0;
  }

  // ====== ChildSaveStrategy (lookup methods unchanged from original) ======

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
      void save(int[] childLabels, int labelCnt, int strategyBytes, IndexOutput output)
          throws IOException {
        byte presenceBits = 1; // The first arc is always present.
        int presenceIndex = 0;
        int previousLabel = childLabels[0];
        for (int i = 1; i < labelCnt; i++) {
          int label = childLabels[i];
          assert label > previousLabel;
          presenceIndex += label - previousLabel;
          while (presenceIndex >= Byte.SIZE) {
            output.writeByte(presenceBits);
            presenceBits = 0;
            presenceIndex -= Byte.SIZE;
          }
          presenceBits |= 1 << presenceIndex;
          previousLabel = label;
        }
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
      void save(int[] childLabels, int labelCnt, int strategyBytes, IndexOutput output)
          throws IOException {
        for (int i = 1; i < labelCnt; i++) {
          output.writeByte((byte) childLabels[i]);
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
      void save(int[] childLabels, int labelCnt, int strategyBytes, IndexOutput output)
          throws IOException {
        output.writeByte((byte) childLabels[labelCnt - 1]); // max label
        int lastLabel = childLabels[0];
        for (int i = 1; i < labelCnt; i++) {
          while (++lastLabel < childLabels[i]) {
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

    abstract void save(int[] childLabels, int labelCnt, int strategyBytes, IndexOutput output)
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
