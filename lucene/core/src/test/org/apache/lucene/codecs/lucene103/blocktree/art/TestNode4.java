package org.apache.lucene.codecs.lucene103.blocktree.art;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

public class TestNode4 extends LuceneTestCase {
  Node4 node = new Node4(0);

  public void setUp() throws Exception {
    super.setUp();
    int childrenCount = random().nextInt(1, 5);
    //    int childrenCount = 4;
    for (int i = 0; i < childrenCount; i++) {
      byte b = (byte) random().nextInt();
      Node4.insert(node, new LeafNode(new BytesRef(new byte[] {b}), null), b);
    }
  }

  public void testPosition() {
    assert node.getNextLargerPos(Node.ILLEGAL_IDX) == 0;
    int currentPos = Node.ILLEGAL_IDX;
    int nextPos;
    while ((nextPos = node.getNextLargerPos(currentPos)) != Node.ILLEGAL_IDX) {
      assert nextPos - currentPos == 1;
      currentPos = nextPos;
    }
  }
}
