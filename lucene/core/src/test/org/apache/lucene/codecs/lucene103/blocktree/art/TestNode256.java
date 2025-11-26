package org.apache.lucene.codecs.lucene103.blocktree.art;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

public class TestNode256 extends LuceneTestCase {
  Node256 node = new Node256(0);

  public void setUp() throws Exception {
    super.setUp();
    int childrenCount = random().nextInt(49, 256);
    for (int i = 0; i < childrenCount; i++) {
      byte b = (byte) random().nextInt();
      Node256.insert(node, new LeafNode(new BytesRef(new byte[] {b}), null), b);
    }
  }

  public void testPosition() {
    int currentPos = Node.ILLEGAL_IDX;
    int nextPos;
    while ((nextPos = node.getNextLargerPos(currentPos)) != Node.ILLEGAL_IDX) {
      int calculatePos = nextPos - node.numberOfNullChildren(nextPos);
      assert calculatePos == realPos(nextPos)
          : "next Pos: "
              + nextPos
              + ", calculate Pos: "
              + calculatePos
              + ", real Pos:"
              + realPos(nextPos);
      currentPos = nextPos;
    }
  }

  private int realPos(int pos) {
    Node[] children = node.children;
    assert children[pos] != null;
    int realPos = pos;

    for (int i = 0; i <= pos; i++) {
      if (children[i] == null) {
        realPos--;
      }
    }
    return realPos;
  }
}
