package org.apache.lucene.codecs.lucene103.blocktree.art;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

public class TestNode48 extends LuceneTestCase {
  public void test() {
    Node48 node48 = new Node48(0);
    for (int i = 100; i < 148; i++) {
      Node48.insert(node48, new LeafNode(new BytesRef(new byte[] {(byte) i}), null), (byte) i);
    }

    int pos = 0;
    node48.getNextLargerPos(100);
    for (int i = 100; i < 148; i++) {
      assertEquals(pos, node48.getChildIndex((byte) i));
      pos++;
    }
  }
}
