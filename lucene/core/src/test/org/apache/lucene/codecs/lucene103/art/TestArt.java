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
package org.apache.lucene.codecs.lucene103.art;

import org.apache.lucene.codecs.lucene103.blocktree.art.ARTBuilder;
import org.apache.lucene.codecs.lucene103.blocktree.art.ARTReader;
import org.apache.lucene.codecs.lucene103.blocktree.art.NodeType;
import org.apache.lucene.codecs.lucene103.blocktree.art.Output;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

public class TestArt extends LuceneTestCase {

  public void testNode4() {
    // Build.
    ARTBuilder artBuilder = new ARTBuilder();
    artBuilder.insert(new BytesRef("abc1".getBytes()), new Output(0, false, new BytesRef("abc1")));
    artBuilder.insert(
        new BytesRef("abc10".getBytes()), new Output(0, false, new BytesRef("abc10")));
    artBuilder.insert(
        new BytesRef("abc100".getBytes()), new Output(0, false, new BytesRef("abc100")));
    artBuilder.insert(new BytesRef("abc2".getBytes()), new Output(0, false, new BytesRef("abc2")));
    artBuilder.insert(
        new BytesRef("abc234".getBytes()), new Output(0, false, new BytesRef("abc234")));
    artBuilder.insert(new BytesRef("abc3".getBytes()), new Output(0, false, new BytesRef("abc3")));

    assertEquals(3, artBuilder.root.prefixLength);
    assertEquals(NodeType.NODE4, artBuilder.root.nodeType);

    // Search.
    ARTReader artReader = new ARTReader(artBuilder.root);
    assertEquals(new Output(0, false, new BytesRef("abc1")), artReader.find(new BytesRef("abc1")));
    assertEquals(
        new Output(0, false, new BytesRef("abc10")), artReader.find(new BytesRef("abc10")));
    assertEquals(
        new Output(0, false, new BytesRef("abc100")), artReader.find(new BytesRef("abc100")));
    assertEquals(new Output(0, false, new BytesRef("abc2")), artReader.find(new BytesRef("abc2")));
    assertEquals(
        new Output(0, false, new BytesRef("abc234")), artReader.find(new BytesRef("abc234")));
    assertEquals(new Output(0, false, new BytesRef("abc3")), artReader.find(new BytesRef("abc3")));
    assertNull(artReader.find(new BytesRef("abc33")));
  }

  public void testNode16() {
    // Build.
    ARTBuilder artBuilder = new ARTBuilder();
    // Add a null child.
    artBuilder.insert(
        new BytesRef(("abc").getBytes()), new Output(0, false, new BytesRef(("abc"))));
    for (int i = 0; i < 10; i++) {
      artBuilder.insert(
          new BytesRef(("abc" + i).getBytes()), new Output(0, false, new BytesRef(("abc" + i))));
    }
    assertEquals(NodeType.NODE16, artBuilder.root.nodeType);

    // Search.
    ARTReader artReader = new ARTReader(artBuilder.root);
    assertEquals(new Output(0, false, new BytesRef("abc")), artReader.find(new BytesRef("abc")));
    for (int i = 0; i < 10; i++) {
      assertEquals(
          new Output(0, false, new BytesRef("abc" + i)), artReader.find(new BytesRef("abc" + i)));
    }
  }

  public void testNode48() {
    // Build.
    ARTBuilder artBuilder = new ARTBuilder();
    // Add a null child.
    artBuilder.insert(
        new BytesRef(("abc").getBytes()), new Output(0, false, new BytesRef(("abc"))));
    for (byte i = 65; i < 91; i++) {
      byte[] bytes = {97, 98, 99, i};
      artBuilder.insert(new BytesRef(bytes), new Output(0, false, new BytesRef(bytes)));
    }
    assertEquals(NodeType.NODE48, artBuilder.root.nodeType);

    // Search.
    ARTReader artReader = new ARTReader(artBuilder.root);
    assertEquals(new Output(0, false, new BytesRef("abc")), artReader.find(new BytesRef("abc")));
    for (byte i = 65; i < 91; i++) {
      byte[] bytes = {97, 98, 99, i};
      assertEquals(new Output(0, false, new BytesRef(bytes)), artReader.find(new BytesRef(bytes)));
    }
  }

  public void testNode256() {
    // Build.
    ARTBuilder artBuilder = new ARTBuilder();
    // Add a null child.
    artBuilder.insert(
        new BytesRef(("abc").getBytes()), new Output(0, false, new BytesRef(("abc"))));
    for (byte i = -128; i < 127; i++) {
      byte[] bytes = {97, 98, 99, i};
      artBuilder.insert(new BytesRef(bytes), new Output(0, false, new BytesRef(bytes)));
    }
    assertEquals(NodeType.NODE256, artBuilder.root.nodeType);

    // Search.
    ARTReader artReader = new ARTReader(artBuilder.root);
    assertEquals(new Output(0, false, new BytesRef("abc")), artReader.find(new BytesRef("abc")));
    for (byte i = -128; i < 127; i++) {
      byte[] bytes = {97, 98, 99, i};
      assertEquals(new Output(0, false, new BytesRef(bytes)), artReader.find(new BytesRef(bytes)));
    }
  }
}
