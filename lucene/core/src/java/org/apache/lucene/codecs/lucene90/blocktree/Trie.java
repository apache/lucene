package org.apache.lucene.codecs.lucene90.blocktree;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.function.BiConsumer;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

class Trie {

  static class Node {
    final int label;
    final LinkedList<Node> children;
    BytesRef output;

    Node(int label, BytesRef output, LinkedList<Node> children) {
      this.label = label;
      this.output = output;
      this.children = children;
    }
  }

  final Node root = new Node(0, null, new LinkedList<>());

  Trie(BytesRef k, BytesRef v) {
    if (k.length == 0) {
      root.output = v;
      return;
    }
    Node parent = root;
    for (int i = 0; i < k.length; i++) {
      int b = k.bytes[i + k.offset] & 0xFF;
      BytesRef output = i == k.length - 1 ? v : null;
      Node node = new Node(b, output, new LinkedList<>());
      parent.children.add(node);
      parent = node;
    }
  }

  void putAll(Trie trie) {
    putAll(this.root, trie.root);
  }

  private static void putAll(Node n, Node add) {
    assert n.label == add.label;
    if (add.output != null) {
      n.output = BytesRef.deepCopyOf(add.output);
    }
    ListIterator<Node> iter = n.children.listIterator();
    outer:
    for (Node addChild : add.children) {
      while (iter.hasNext()) {
        Node nChild = iter.next();
        if (nChild.label == addChild.label) {
          putAll(nChild, addChild);
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

  BytesRef getEmptyOutput() {
    return root.output;
  }

  void forEach(BiConsumer<BytesRef, BytesRef> consumer) {
    if (root.output != null) {
      consumer.accept(new BytesRef(), root.output);
    }
    intersect(root.children, new BytesRefBuilder(), consumer);
  }

  private void intersect(
      List<Node> nodes, BytesRefBuilder key, BiConsumer<BytesRef, BytesRef> consumer) {
    for (Node node : nodes) {
      key.append((byte) node.label);
      if (node.output != null) consumer.accept(key.toBytesRef(), node.output);
      intersect(node.children, key, consumer);
      key.setLength(key.length() - 1);
    }
  }

  void save(DataOutput meta, IndexOutput index) throws IOException {
    ByteBuffersDataOutput outputBuffer = new ByteBuffersDataOutput();
    meta.writeVLong(index.getFilePointer()); // index start fp
    meta.writeVLong(saveArcs(root, index, outputBuffer, index.getFilePointer())); // root code
    index.writeLong(0L); // additional 8 bytes for probably over-read in BIT strategy
    meta.writeVLong(index.getFilePointer()); // index end, output start fp
    outputBuffer.copyTo(index);
    meta.writeVLong(index.getFilePointer()); // output end fp
  }

  long saveArcs(Node node, IndexOutput index, ByteBuffersDataOutput outputsBuffer, long startFP)
      throws IOException {
    final int childrenNum = node.children.size();

    if (childrenNum == 0) {
      assert node.output != null;
      long code = (outputsBuffer.size() << 1) | 1L;
      outputsBuffer.writeVLong(0L);
      outputsBuffer.writeBytes(node.output.bytes, node.output.offset, node.output.length);
      return code;
    }

    long[] codeBuffer = new long[childrenNum];
    long maxCode = 0;
    for (int i = 0; i < childrenNum; i++) {
      Node child = node.children.get(i);
      codeBuffer[i] = saveArcs(child, index, outputsBuffer, startFP);
      maxCode = Math.max(maxCode, codeBuffer[i]);
    }

    final int minLabel = node.children.get(0).label;
    final int maxLabel = node.children.get(node.children.size() - 1).label;
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

    long fp = index.getFilePointer();
    if (fp == startFP) {
      // relative fp == 0 has special meaning.
      index.writeByte((byte) 0);
      fp = index.getFilePointer();
    }

    assert positionStrategy != null;
    assert positionBytes >= 0 && positionBytes <= 32;

    int sign =
        (positionStrategy.priority << 14) // 2bit
            | (positionBytes << 8) // 6 bit
            | minLabel; // 8bit

    index.writeShort((short) sign);
    int codeBytes = Math.max(1, Long.BYTES - (Long.numberOfLeadingZeros(maxCode) >>> 3));
    index.writeByte((byte) codeBytes);
    positionStrategy.save(node.children, childrenNum, positionBytes, index);
    assert index.getFilePointer() == fp + positionBytes + 3
        : positionStrategy.name()
            + " position bytes compute error, computed: "
            + positionBytes
            + " actual: "
            + (index.getFilePointer() - fp - 3);

    for (int i = 0; i < childrenNum; i++) {
      long code = codeBuffer[i];
      for (int j = 0; j < codeBytes; j++) {
        index.writeByte((byte) code);
        code >>= 8;
      }
    }

    long relativeFP = fp - startFP;
    if (node.output == null) {
      return relativeFP << 1;
    } else {
      long code = (outputsBuffer.size() << 1) | 1L;
      outputsBuffer.writeVLong(relativeFP);
      outputsBuffer.writeBytes(node.output.bytes, node.output.offset, node.output.length);
      return code;
    }
  }

  enum PositionStrategy {
    ARRAY(2) {
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
      int lookup(int targetLabel, RandomAccessInput in, long offset, int positionBytes, int minLabel) throws IOException {
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

    REVERSE_ARRAY(1) {
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
      int lookup(int targetLabel, RandomAccessInput in, long offset, int positionBytes, int minLabel) throws IOException {
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
    },

    BITS(0) {
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
        int previousLabel = children.get(0).label;
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
        assert presenceIndex
            == (children.get(children.size() - 1).label - children.get(0).label) % 8;
        assert presenceBits != 0; // The last byte is not 0.
        assert (presenceBits & (1 << presenceIndex)) != 0; // The last arc is always present.
        output.writeByte(presenceBits);
      }

      @Override
      int lookup(int targetLabel, RandomAccessInput in, long offset, int positionBytes, int minLabel) throws IOException {
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

    abstract int lookup(int targetLabel, RandomAccessInput in, long offset, int positionBytes, int minLabel)
        throws IOException;

    static PositionStrategy byCode(int code) {
      return STRATEGIES[code];
    }
  }
}
