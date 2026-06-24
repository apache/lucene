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

import java.util.HashSet;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

public class TestNode256 extends LuceneTestCase {
  Node256 node = new Node256(0);

  @Override
  public void setUp() throws Exception {
    super.setUp();
    int childrenCount = random().nextInt(49, 256);
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

  public void testPosition() {
    assert node.nodeType.equals(NodeType.NODE256);

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
