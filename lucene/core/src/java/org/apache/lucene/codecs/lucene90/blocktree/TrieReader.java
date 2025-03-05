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
    final int sign = nodesIn.readInt(fp);
    final int header = sign >>> 16;
    node.childrenCodesBytes = header & 0x07;
    node.childrenStrategy = (sign >>> 14) & 0x03;
    node.positionBytes = (sign >>> 8) & 0x3F;
    node.minChildrenLabel = sign & 0xFF;
    fp += META_BYTES;

    final int fpBits = header & 0x38;
    final long mask = (1L << fpBits) - 1L;
    node.minChildrenCode = nodesIn.readLong(fp) & mask;
    final int fpBytes = fpBits >>> 3;
    fp += fpBytes;

    if ((header & (1 << 6)) != 0) {
      node.outputFp = nodesIn.readLong(fp) & mask;
      node.positionFp = fp + fpBytes;
    } else {
      node.outputFp = NO_OUTPUT;
      node.positionFp = fp;
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
