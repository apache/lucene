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

import java.io.IOException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

public class TestART extends LuceneTestCase {

  public void testNode4() throws IOException {
    // Build.
    ARTBuilder artBuilder = new ARTBuilder();
    // Add a null child.
    artBuilder.insert(new BytesRef(("abc")), new Output(0, false, new BytesRef(("abc"))));
    artBuilder.insert(new BytesRef("abc1"), new Output(0, false, new BytesRef("abc1")));
    artBuilder.insert(new BytesRef("abc10"), new Output(0, false, new BytesRef("abc10")));
    artBuilder.insert(new BytesRef("abc100"), new Output(0, false, new BytesRef("abc100")));
    artBuilder.insert(new BytesRef("abc2"), new Output(0, false, new BytesRef("abc2")));
    artBuilder.insert(new BytesRef("abc234"), new Output(0, false, new BytesRef("abc234")));
    artBuilder.insert(new BytesRef("abc3"), new Output(0, false, new BytesRef("abc3")));

    assertEquals(3, artBuilder.root.prefixLength);
    assertEquals(NodeType.NODE4, artBuilder.root.nodeType);

    // Search.
    ARTReader artReader = new ARTReader(artBuilder.root);
    assertEquals(new Output(0, false, new BytesRef("abc")), artReader.find(new BytesRef("abc")));
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

    // Load.
    try (Directory directory = newDirectory()) {
      try (IndexOutput index = directory.createOutput("index", IOContext.DEFAULT);
          IndexOutput meta = directory.createOutput("meta", IOContext.DEFAULT)) {
        artBuilder.save(meta, index);
      }

      try (IndexInput indexIn = directory.openInput("index", IOContext.DEFAULT);
          IndexInput metaIn = directory.openInput("meta", IOContext.DEFAULT)) {
        long start = metaIn.readVLong();
        long end = metaIn.readVLong();
        ARTReader artReader1 = new ARTReader(indexIn.slice("outputs", start, end - start));
        assertEquals(artReader.getRoot(), artReader1.getRoot());
      }
    }
  }

  public void testNode16() throws IOException {
    // Build.
    ARTBuilder artBuilder = new ARTBuilder();
    // Add a null child.
    artBuilder.insert(new BytesRef(("abc")), new Output(0, false, new BytesRef(("abc"))));
    for (int i = 0; i < 10; i++) {
      artBuilder.insert(new BytesRef(("abc" + i)), new Output(0, false, new BytesRef(("abc" + i))));
    }
    assertEquals(NodeType.NODE16, artBuilder.root.nodeType);

    // Search.
    ARTReader artReader = new ARTReader(artBuilder.root);
    assertEquals(new Output(0, false, new BytesRef("abc")), artReader.find(new BytesRef("abc")));
    for (int i = 0; i < 10; i++) {
      assertEquals(
          new Output(0, false, new BytesRef("abc" + i)), artReader.find(new BytesRef("abc" + i)));
    }

    // Load.
    try (Directory directory = newDirectory()) {
      try (IndexOutput index = directory.createOutput("index", IOContext.DEFAULT);
          IndexOutput meta = directory.createOutput("meta", IOContext.DEFAULT)) {
        artBuilder.save(meta, index);
      }

      try (IndexInput indexIn = directory.openInput("index", IOContext.DEFAULT);
          IndexInput metaIn = directory.openInput("meta", IOContext.DEFAULT)) {
        long start = metaIn.readVLong();
        long end = metaIn.readVLong();
        ARTReader artReader1 = new ARTReader(indexIn.slice("outputs", start, end - start));
        assertEquals(artReader.getRoot(), artReader1.getRoot());
      }
    }
  }

  public void testNode48() throws IOException {
    // Build.
    ARTBuilder artBuilder = new ARTBuilder();
    // Add a null child.
    artBuilder.insert(new BytesRef(("abc")), new Output(0, false, new BytesRef(("abc"))));
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

    // Load.
    try (Directory directory = newDirectory()) {
      try (IndexOutput index = directory.createOutput("index", IOContext.DEFAULT);
          IndexOutput meta = directory.createOutput("meta", IOContext.DEFAULT)) {
        artBuilder.save(meta, index);
      }

      try (IndexInput indexIn = directory.openInput("index", IOContext.DEFAULT);
          IndexInput metaIn = directory.openInput("meta", IOContext.DEFAULT)) {
        long start = metaIn.readVLong();
        long end = metaIn.readVLong();
        ARTReader artReader1 = new ARTReader(indexIn.slice("outputs", start, end - start));
        assertEquals(artReader.getRoot(), artReader1.getRoot());
      }
    }
  }

  public void testNode256() throws IOException {
    // Build.
    ARTBuilder artBuilder = new ARTBuilder();
    // Add a null child.
    artBuilder.insert(new BytesRef(("abc")), new Output(0, false, new BytesRef(("abc"))));
    for (int i = -128; i <= 127; i++) {
      byte[] bytes = {97, 98, 99, (byte) i};
      artBuilder.insert(new BytesRef(bytes), new Output(0, false, new BytesRef(bytes)));
    }
    assertEquals(NodeType.NODE256, artBuilder.root.nodeType);

    // Search.
    ARTReader artReader = new ARTReader(artBuilder.root);
    assertEquals(new Output(0, false, new BytesRef("abc")), artReader.find(new BytesRef("abc")));
    for (int i = -128; i <= 127; i++) {
      byte[] bytes = {97, 98, 99, (byte) i};
      assertEquals(new Output(0, false, new BytesRef(bytes)), artReader.find(new BytesRef(bytes)));
    }

    // Load.
    try (Directory directory = newDirectory()) {
      try (IndexOutput index = directory.createOutput("index", IOContext.DEFAULT);
          IndexOutput meta = directory.createOutput("meta", IOContext.DEFAULT)) {
        artBuilder.save(meta, index);
      }

      try (IndexInput indexIn = directory.openInput("index", IOContext.DEFAULT);
          IndexInput metaIn = directory.openInput("meta", IOContext.DEFAULT)) {
        long start = metaIn.readVLong();
        long end = metaIn.readVLong();
        ARTReader artReader1 = new ARTReader(indexIn.slice("outputs", start, end - start));
        assertEquals(artReader.getRoot(), artReader1.getRoot());
      }
    }
  }
}
