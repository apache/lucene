package org.apache.lucene.codecs.lucene103.blocktree.art;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

public class TestNode16 extends LuceneTestCase {
  Node16 node = new Node16(0);

  public void setUp() throws Exception {
    super.setUp();
    int childrenCount = random().nextInt(5, 17);
    for (int i = 0; i < childrenCount; i++) {
      byte b = (byte) random().nextInt();
      Node16.insert(node, new LeafNode(new BytesRef(new byte[] {b}), null), b);
    }
  }

  public void testPosition() {
    int currentPos = Node.ILLEGAL_IDX;
    int nextPos;
    while ((nextPos = node.getNextLargerPos(currentPos)) != Node.ILLEGAL_IDX) {
      assert nextPos - currentPos == 1;
      currentPos = nextPos;
    }
  }
}
