package org.apache.lucene.codecs.lucene103.blocktree.art;


import org.apache.lucene.util.BytesRef;

public class LeafNode extends Node {


  public static final int LEAF_NODE_KEY_LENGTH_IN_BYTES = 6;

  /**
   * constructor
   *
   * @param key the 48 bit
   * @param output the corresponding container index
   */
  public LeafNode(BytesRef key, Output output) {
    super(NodeType.LEAF_NODE, 0);
//    byte[] bytes = new byte[key.bytes.length];
//    System.arraycopy(key, 0, bytes, 0, LEAF_NODE_KEY_LENGTH_IN_BYTES);
    this.key = key;
    this.output = output;
  }

  @Override
  public int getChildPos(byte k) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte getChildKey(int pos) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Node getChild(int pos) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void replaceNode(int pos, Node freshOne) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getMinPos() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getNextLargerPos(int pos) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getMaxPos() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getNextSmallerPos(int pos) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Node remove(int pos) {
    throw new UnsupportedOperationException();
  }

}
