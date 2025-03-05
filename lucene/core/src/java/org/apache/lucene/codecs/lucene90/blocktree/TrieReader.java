package org.apache.lucene.codecs.lucene90.blocktree;

import java.io.IOException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;

class TrieReader {

  private static final long META_BYTES = 3L;
  private static final long NO_OUTPUT = -1;

  static class Node {
    Node parent;
    long positionFp;
    long outputFp;
    int label;
    boolean isLeaf;
    Trie.PositionStrategy childrenStrategy;
    int positionBytes;
    int minChildrenLabel;
    int childrenCodesBytes;

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
    load(root, rootFP, null);
  }

  private void load(Node node, long code, Node parent) throws IOException {
    node.parent = parent;
    long tail = code & 0x3L;
    if (tail == 0x1L) {
      node.isLeaf = true;
      node.outputFp = code >>> 2;
      return;
    }

    node.isLeaf = false;
    long fp = code >>> 2;
    final int sign = nodesIn.readInt(fp);
    final int bytes = sign >>> 16;
    node.childrenCodesBytes = bytes & 0x7;
    node.childrenStrategy = Trie.PositionStrategy.byCode((sign >>> 14) & 0x03);
    node.positionBytes = (sign >>> 8) & 0x3F;
    node.minChildrenLabel = sign & 0xFF;
    fp += META_BYTES;
    if (tail == 0x3L) {
      int shift = bytes & 0x38;
      long mask = (1L << shift) - 1L;
      node.outputFp = nodesIn.readLong(fp) & mask;
      node.positionFp = fp + (shift >> 3);
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
      position =
          parent.childrenStrategy.lookup(
              targetLabel, nodesIn, positionBytesStartFp, positionBytes, minLabel);
    }

    if (position < 0) {
      return null;
    }

    final long codeBytes = parent.childrenCodesBytes;
    final long pos = positionBytesStartFp + positionBytes + codeBytes * position;
    final long mask = (1L << (codeBytes << 3)) - 1L;
    final long code = nodesIn.readLong(pos) & mask;
    child.label = targetLabel;
    load(child, code, parent);

    return child;
  }
}
