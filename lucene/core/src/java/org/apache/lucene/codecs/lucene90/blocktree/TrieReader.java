package org.apache.lucene.codecs.lucene90.blocktree;

import static org.apache.lucene.codecs.lucene90.blocktree.Trie.PositionStrategy.ARRAY;
import static org.apache.lucene.codecs.lucene90.blocktree.Trie.PositionStrategy.BITS;
import static org.apache.lucene.codecs.lucene90.blocktree.Trie.PositionStrategy.REVERSE_ARRAY;

import java.io.IOException;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;

class TrieReader {

  private static final long META_BYTES = 3L;
  private static final long NO_OUTPUT = -1;
  static final int HAS_OUTPUT = 1 << 6;
  static final int SINGLE_CHILD = 1 << 7;

  static class Node {
    private long positionFp;
    private long outputFp;
    private boolean isLeaf;
    private int childrenStrategy;
    private int positionBytes;
    private int minChildrenLabel;
    private int childrenCodesBytes;
    private long minChildrenCode;

    int label;

    boolean hasOutput() {
      return outputFp != NO_OUTPUT;
    }

    IndexInput output(TrieReader reader) throws IOException {
      assert hasOutput();
      reader.outputsIn.seek(outputFp);
      return reader.outputsIn;
    }
  }

  final RandomAccessInput nodesIn;
  final IndexInput outputsIn;
  final Node root;

  TrieReader(RandomAccessInput nodesIn, IndexInput outputsIn, long rootFP) throws IOException {
    this.nodesIn = nodesIn;
    this.outputsIn = outputsIn;
    this.root = new Node();
    load(root, rootFP);
  }

  private void load(Node node, long code) throws IOException {
    long tail = code & 0x01L;
    if (tail == 0x01L) {
      node.isLeaf = true;
      node.outputFp = code >>> 1;
      return;
    }

    node.isLeaf = false;
    long fp = code >>> 1;
    final int header = nodesIn.readInt(fp);
    if ((header & SINGLE_CHILD) != 0) {
      node.childrenCodesBytes = header & 0x07;
      node.childrenStrategy = ARRAY.priority;
      node.positionBytes = 0;
      node.minChildrenLabel = (header >>> 8) & 0xFF;
      node.minChildrenCode = 0L;
      fp += 2;
      if ((header & HAS_OUTPUT) != 0) {
        int fpBits = header & 0x38;
        long mask = (1L << fpBits) - 1L;
        node.outputFp = nodesIn.readLong(fp) & mask;
        node.positionFp = fp + (fpBits >> 3);
      } else {
        node.outputFp = NO_OUTPUT;
        node.positionFp = fp;
      }
      return;
    }

    node.childrenCodesBytes = header & 0x07;
    node.childrenStrategy = (header >>> 22) & 0x03;
    node.positionBytes = (header >>> 16) & 0x3F;
    node.minChildrenLabel = (header >>> 8) & 0xFF;
    fp += 3;

    final int fpBits = header & 0x38;
    if (fpBits == 0) {
      node.minChildrenCode = 0L;
      node.outputFp = NO_OUTPUT;
      node.positionFp = fp;
    } else {
      long mask = (1L << fpBits) - 1L;
      node.minChildrenCode = nodesIn.readLong(fp) & mask;
      int fpBytes = fpBits >>> 3;
      fp += fpBytes;
      if ((header & HAS_OUTPUT) != 0) {
        node.outputFp = nodesIn.readLong(fp) & mask;
        node.positionFp = fp + fpBytes;
      } else {
        node.outputFp = NO_OUTPUT;
        node.positionFp = fp;
      }
    }
  }

  Node lookupChild(int targetLabel, Node parent, Node child) throws IOException {
    if (parent.isLeaf) {
      return null;
    }

    final long positionBytesStartFp = parent.positionFp;
    final int minLabel = parent.minChildrenLabel;
    final int positionBytes = parent.positionBytes;

    int position;
    if (targetLabel < minLabel) {
      position = -1;
    } else if (targetLabel == minLabel) {
      position = 0;
    } else {
      int strategy = parent.childrenStrategy;
      // Use if else here - avoiding virtual call seems help performance
      if (strategy == BITS.priority) {
        position = BITS.lookup(targetLabel, nodesIn, positionBytesStartFp, positionBytes, minLabel);
      } else if (strategy == ARRAY.priority) {
        position =
            ARRAY.lookup(targetLabel, nodesIn, positionBytesStartFp, positionBytes, minLabel);
      } else if (strategy == REVERSE_ARRAY.priority) {
        position =
            REVERSE_ARRAY.lookup(
                targetLabel, nodesIn, positionBytesStartFp, positionBytes, minLabel);
      } else {
        throw new CorruptIndexException("unknown strategy: " + strategy, "trie nodesIn");
      }
    }

    if (position < 0) {
      return null;
    }

    final long codeBytes = parent.childrenCodesBytes;
    final long pos = positionBytesStartFp + positionBytes + codeBytes * position;
    final long mask = (1L << (codeBytes << 3)) - 1L;
    final long code = (nodesIn.readLong(pos) & mask) + parent.minChildrenCode;
    child.label = targetLabel;
    load(child, code);

    return child;
  }
}
