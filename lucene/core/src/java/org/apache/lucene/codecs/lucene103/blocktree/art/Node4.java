package org.apache.lucene.codecs.lucene103.blocktree.art;

public class Node4 extends Node {

  int childIndex = 0;
  Node[] children = new Node[4];
  Output output;

  public Node4(int compressedPrefixSize) {
    super(NodeType.NODE4, compressedPrefixSize);
  }

  @Override
  public int getChildPos(byte k) {
    for (int i = 0; i < count; i++) {
      int shiftLeftLen = (3 - i) * 8;
      byte v = (byte) (childIndex >> shiftLeftLen);
      if (v == k) {
        return i;
      }
    }
    return ILLEGAL_IDX;
  }

  @Override
  public byte getChildKey(int pos) {
    int shiftLeftLen = (3 - pos) * 8;
    byte v = (byte) (childIndex >> shiftLeftLen);
    return v;
  }

  @Override
  public Node getChild(int pos) {
    return children[pos];
  }

  @Override
  public void replaceNode(int pos, Node freshOne) {
    children[pos] = freshOne;
  }

  @Override
  public int getMinPos() {
    return 0;
  }

  @Override
  public int getNextLargerPos(int pos) {
    if (pos == ILLEGAL_IDX) {
      return 0;
    }
    pos++;
    return pos < count ? pos : ILLEGAL_IDX;
  }

  @Override
  public int getMaxPos() {
    return count - 1;
  }

  @Override
  public int getNextSmallerPos(int pos) {
    if (pos == ILLEGAL_IDX) {
      return count - 1;
    }
    pos--;
    return pos >= 0 ? pos : ILLEGAL_IDX;
  }

  /**
   * insert the child node into the node4 with the key byte
   *
   * @param node the node4 to insert into
   * @param childNode the child node
   * @param key the key byte
   * @return the input node4 or an adaptive generated node16
   */
  public static Node insert(Node node, Node childNode, byte key) {
    Node4 current = (Node4) node;
    if (current.count < 4) {
      //insert leaf into current node
      current.childIndex = IntegerUtil.setByte(current.childIndex, key, current.count);
      current.children[current.count] = childNode;
      current.count++;
      return current;
    } else {
      //grow to Node16
      Node16 node16 = new Node16(current.prefixLength);
      node16.count = 4;
      node16.firstChildIndex = LongUtils.initWithFirst4Byte(current.childIndex);
      System.arraycopy(current.children, 0, node16.children, 0, 4);
      copyPrefix(current, node16);
      Node freshOne = Node16.insert(node16, childNode, key);
      return freshOne;
    }
  }

  @Override
  public Node remove(int pos) {
    assert pos < count;
    children[pos] = null;
    count--;
    childIndex = IntegerUtil.shiftLeftFromSpecifiedPosition(childIndex, pos, (4 - pos - 1));
    for (; pos < count; pos++) {
      children[pos] = children[pos + 1];
    }
    if (count == 1) {
      //shrink to leaf node
      Node child = children[0];
      byte newLength = (byte) (child.prefixLength + this.prefixLength + 1);
      byte[] newPrefix = new byte[newLength];
      System.arraycopy(this.prefix, 0, newPrefix, 0, this.prefixLength);
      newPrefix[this.prefixLength] = IntegerUtil.firstByte(childIndex);
      System.arraycopy(child.prefix, 0, newPrefix, this.prefixLength + 1, child.prefixLength);
      child.prefixLength = newLength;
      child.prefix = newPrefix;
      return child;
    }
    return this;
  }
}
