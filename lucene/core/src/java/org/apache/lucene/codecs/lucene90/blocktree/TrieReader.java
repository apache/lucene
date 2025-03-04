package org.apache.lucene.codecs.lucene90.blocktree;

import java.io.IOException;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;

class TrieReader {

  private static final long META_BYTES = 3L;
  private static final long NO_OUTPUT = -1;

  static class Node {
    Node parent;
    long fp;
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

  final RandomAccessInput arcsIn;
  final IndexInput outputsIn;
  final Node root;

  TrieReader(IndexInput meta, IndexInput index) throws IOException {
    long arcInStart = meta.readVLong();
    long rootFP = meta.readVLong();
    long arcInEnd = meta.readVLong();
    arcsIn = index.randomAccessSlice(arcInStart, arcInEnd - arcInStart);
    long outputEnd = meta.readVLong();
    outputsIn = index.slice("outputs", arcInEnd, outputEnd - arcInEnd);
    root = new Node();
    load(root, rootFP, null);
  }

  private void load(Node node, long code, Node parent) throws IOException {
    node.parent = parent;
    if ((code & 0x1) == 0) {
      node.fp = code >>> 1;
      node.outputFp = NO_OUTPUT;
    } else {
      long outputFP = code >>> 1;
      outputsIn.seek(outputFP);
      node.fp = outputsIn.readVLong();
      node.outputFp = outputsIn.getFilePointer();
      if (node.fp == 0) {
        node.isLeaf = true;
        return;
      }
    }

    int sign = arcsIn.readInt(node.fp);
    node.childrenCodesBytes = (sign >>> 16) & 0xFF;;
    node.childrenStrategy = Trie.PositionStrategy.byCode((sign >>> 14) & 0x03);
    node.positionBytes = (sign >>> 8) & 0x3F;
    node.minChildrenLabel = sign & 0xFF;
  }

  Node lookupChild(int targetLabel, Node parent, Node child) throws IOException {
    if (parent.isLeaf) {
      return null;
    }

    final long positionBytesFp = parent.fp + META_BYTES;
    final int minLabel = parent.minChildrenLabel;
    final int positionBytes = parent.positionBytes;

    int position;
    if (targetLabel < minLabel) {
      position = -1;
    } else if (targetLabel == minLabel) {
      position = 0;
    } else {
      position =
          parent.childrenStrategy.lookup(targetLabel, arcsIn, positionBytesFp, positionBytes, minLabel);
    }

    if (position < 0) {
      return null;
    }

    final long codeBytes = parent.childrenCodesBytes;
    final long pos = positionBytesFp + positionBytes + codeBytes * position;
    final long mask = (1L << (codeBytes << 3)) - 1;
    final long code = arcsIn.readLong(pos) & mask;

    child.label = targetLabel;
    load(child, code, parent);

    return child;
  }
}
