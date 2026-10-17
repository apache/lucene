/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.codecs.lucene103.blocktree.art;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

public class TestNode48 extends LuceneTestCase {
  Node48 node = new Node48(0);

  @Override
  public void setUp() throws Exception {
    super.setUp();
    int childrenCount = random().nextInt(17, 49);
    HashSet<Byte> bytes = new HashSet<>();
    for (int i = 0; i < childrenCount; i++) {
      byte b = (byte) random().nextInt();
      // Use unique byte to insert.
      while (bytes.contains(b)) {
        b = (byte) random().nextInt();
      }
      bytes.add(b);
      node.insert(new LeafNode(new BytesRef(new byte[] {b}), null), b);
    }
  }

  public void testMaskAll() {
    int mask = 0;
    for (int i = 0; i < 32; i++) {
      mask |= 1 << i;
    }
    for (int i = 0; i < 32; i++) {
      assert ((mask >>> i) & 1) == 1 : "i: " + i + " is: " + ((mask >>> i) & 1);
    }
  }

  public void testMaskRandom() {
    int size = random().nextInt(32);
    List<Integer> offs = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      int off = random().nextInt(32);
      while (offs.contains(off)) {
        off = random().nextInt(32);
      }
      offs.add(off);
    }

    int mask = 0;
    for (int i = 0; i < 32; i++) {
      if (offs.contains(i) == false) {
        mask |= 1 << i;
      }
    }
    for (int i = 0; i < 32; i++) {
      if (offs.contains(i)) {
        assert ((mask >>> i) & 1) == 0 : "i: " + i + " is: " + ((mask >>> i) & 1);
      } else {
        assert ((mask >>> i) & 1) == 1 : "i: " + i + " is: " + ((mask >>> i) & 1);
      }
    }
  }

  // For Node48, position is the key byte, we can use this key to calculate child index with
  // #getChildIndex
  public void testPosition() {
    assert node.nodeType.equals(NodeType.NODE48);
    for (int i = 0; i < node.childrenCount; i++) {
      Node child = node.children[i];
      assert child != null;
      // For this test case, we set child's key equals the insert key, so we can use this key as a
      // key byte to get child.
      byte keyByte = child.key.bytes[0];
      assert node.getChildIndex(keyByte) == i
          : "child index: " + node.getChildIndex(keyByte) + ", real index: " + i;
    }
  }

  public void test() {
    Node48 node48 = new Node48(0);
    for (int i = 100; i < 148; i++) {
      node48.insert(new LeafNode(new BytesRef(new byte[] {(byte) i}), null), (byte) i);
    }

    int pos = 0;
    node48.getNextLargerPos(100);
    for (int i = 100; i < 148; i++) {
      assertEquals(pos, node48.getChildIndex((byte) i));
      pos++;
    }
  }
}
