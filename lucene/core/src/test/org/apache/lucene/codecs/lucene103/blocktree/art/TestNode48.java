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
