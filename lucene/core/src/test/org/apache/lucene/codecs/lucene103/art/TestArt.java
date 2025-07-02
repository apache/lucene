package org.apache.lucene.codecs.lucene103.art;

import org.apache.lucene.codecs.lucene103.blocktree.art.Art;
import org.apache.lucene.codecs.lucene103.blocktree.art.NodeType;
import org.apache.lucene.codecs.lucene103.blocktree.art.Output;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

public class TestArt extends LuceneTestCase {

  public void testNode4() {
    Art art = new Art();
    art.insert(new BytesRef("abc1".getBytes()), new Output(0, false, new BytesRef("abc1")));
    art.insert(new BytesRef("abc10".getBytes()), new Output(0, false, new BytesRef("abc10")));
    art.insert(new BytesRef("abc100".getBytes()), new Output(0, false, new BytesRef("abc100")));
    art.insert(new BytesRef("abc2".getBytes()), new Output(0, false, new BytesRef("abc2")));
    art.insert(new BytesRef("abc234".getBytes()), new Output(0, false, new BytesRef("abc234")));
    art.insert(new BytesRef("abc3".getBytes()), new Output(0, false, new BytesRef("abc3")));


    assertEquals(3, art.root.prefixLength);
    assertEquals(NodeType.NODE4, art.root.nodeType);
  }

  public void testNode16() {
    Art art = new Art();
    // Add a null child.
    art.insert(new BytesRef(("abc").getBytes()), new Output(0, false, new BytesRef(("abc"))));
    for (int i = 0; i < 10; i++) {
      art.insert(new BytesRef(("abc" + i).getBytes()), new Output(0, false, new BytesRef(("abc" + i))));
    }
    assertEquals(NodeType.NODE16, art.root.nodeType);
  }

  public void testNode48() {
    Art art = new Art();
    // Add a null child.
    art.insert(new BytesRef(("abc").getBytes()), new Output(0, false, new BytesRef(("abc"))));
    for (byte i = 65; i < 91; i++) {
      byte[] bytes = {97, 98, 99, i};
      art.insert(new BytesRef(bytes), new Output(0, false, new BytesRef(bytes)));
    }
    assertEquals(NodeType.NODE48, art.root.nodeType);
  }

  public void testNode256() {
    Art art = new Art();
    // Add a null child.
    art.insert(new BytesRef(("abc").getBytes()), new Output(0, false, new BytesRef(("abc"))));
    for (byte i = -128; i < 127; i++) {
      byte[] bytes = {97, 98, 99, i};
      art.insert(new BytesRef(bytes), new Output(0, false, new BytesRef(bytes)));
    }
    assertEquals(NodeType.NODE256, art.root.nodeType);
  }
}
