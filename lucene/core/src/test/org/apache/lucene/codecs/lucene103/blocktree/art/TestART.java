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
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

/** Test ART for save/load node, output, floor data by fp version. */
public class TestART extends LuceneTestCase {

  public void testRandomTerms() throws Exception {
    Supplier<byte[]> supplier = TestART::randomBytes;
    testARTBuilder(supplier, atLeast(1000));
    testARTLookup(supplier, 12);
  }

  public void testVeryLongTerms() throws Exception {
    Supplier<byte[]> supplier =
        () -> {
          byte[] bytes = new byte[65535];
          for (int i = 1; i < bytes.length; i++) {
            bytes[i] = (byte) random().nextInt(i / 256 + 1);
          }
          return bytes;
        };
    testARTLookup(supplier, 5);
  }

  // Node48, Node256 is unusual, testRandomTerms almost can't test them, but this one can.
  public void testOneByteTerms() throws Exception {
    // heavily test single byte terms to generate various label distribution.
    Supplier<byte[]> supplier = () -> new byte[] {(byte) random().nextInt()};
    int round = atLeast(50);
    for (int i = 0; i < round; i++) {
      testARTLookup(supplier, 10);
    }
  }

  private void testARTBuilder(Supplier<byte[]> randomBytesSupplier, int count) {
    Map<BytesRef, Output> kvs = new TreeMap<>();
    // Since we modify bytes when inserting (by ARTBuilder#updateNodeBytes), expected is a copy for
    // original bytes.
    Map<BytesRef, Output> expected = new TreeMap<>();
    kvs.put(new BytesRef(""), new Output(0L, false, new BytesRef("emptyOutput")));
    expected.put(new BytesRef(""), new Output(0L, false, new BytesRef("emptyOutput")));
    for (int i = 0; i < count; i++) {
      BytesRef key = new BytesRef(randomBytesSupplier.get());
      Output value =
          new Output(
              random().nextLong(1L << 62),
              random().nextBoolean(),
              new BytesRef(randomBytesSupplier.get()));
      kvs.put(key, value);
      expected.put(new BytesRef(key.bytes), value);
    }

    // Build.
    ARTBuilder artBuilder = new ARTBuilder();
    artBuilder.insert(new BytesRef(""), new Output(0L, false, new BytesRef("emptyOutput")));
    for (var entry : kvs.entrySet()) {
      if (entry.getKey().equals(new BytesRef(""))) {
        continue;
      }
      artBuilder.insert(entry.getKey(), entry.getValue());
    }

    Map<BytesRef, Output> actual = new TreeMap<>();

    // Visit.
    artBuilder.visit(actual::put);
    assertEquals(expected, actual);
  }

  private void testARTLookup(Supplier<byte[]> randomBytesSupplier, int round) throws IOException {
    for (int iter = 1; iter <= round; iter++) {
      Map<BytesRef, Output> kvs = new TreeMap<>();
      // Since we modify bytes when inserting (by ARTBuilder#updateNodeBytes), expected is a copy
      // for
      // original bytes.
      Map<BytesRef, Output> expected = new TreeMap<>();
      kvs.put(new BytesRef(""), new Output(0L, false, new BytesRef("emptyOutput")));
      expected.put(new BytesRef(""), new Output(0L, false, new BytesRef("emptyOutput")));
      int n = 1 << iter;
      for (int i = 0; i < n; i++) {
        BytesRef key = new BytesRef(randomBytesSupplier.get());
        Output value =
            new Output(
                random().nextLong(1L << 62),
                random().nextBoolean(),
                random().nextBoolean() ? null : new BytesRef(randomBytesSupplier.get()));
        kvs.put(key, value);
        expected.put(new BytesRef(key.bytes), value);
      }

      // Build.
      ARTBuilder artBuilder = new ARTBuilder();

      artBuilder.insert(new BytesRef(""), new Output(0L, false, new BytesRef("emptyOutput")));

      for (var entry : kvs.entrySet()) {
        if (entry.getKey().equals(new BytesRef(""))) {
          continue;
        }
        artBuilder.insert(entry.getKey(), entry.getValue());
      }

      try (Directory directory = newDirectory()) {
        try (IndexOutput index = directory.createOutput("index", IOContext.DEFAULT);
            IndexOutput meta = directory.createOutput("meta", IOContext.DEFAULT)) {
          artBuilder.save(meta, index);
        }

        try (IndexInput indexIn = directory.openInput("index", IOContext.DEFAULT);
            IndexInput metaIn = directory.openInput("meta", IOContext.DEFAULT)) {
          long start = metaIn.readVLong();
          long rootFP = metaIn.readVLong();
          long end = metaIn.readVLong();

          ARTReader artReader = new ARTReader(indexIn.slice("outputs", start, end - start), rootFP);

          for (Map.Entry<BytesRef, Output> entry : expected.entrySet()) {
            assertResultStepByNode(artReader, entry.getKey(), entry.getValue());
          }
          // TODO: test not found.
        }
      }
    }
  }

  //  private static void assertResultStepByNodeLazily(ARTReader reader, BytesRef term, Output
  // expected)
  //      throws IOException {
  //    Node node = reader.getRoot();
  //    Node targetNode = null;
  //    while ((targetNode = reader.lookupChildLazily(term, node)) != null) {
  //      if (node == targetNode) {
  //        break;
  //      }
  //      node = targetNode;
  //    }
  //    if (targetNode == null) {
  //      assertEquals(expected.fp(), node.outputFp);
  //      assertEquals(expected.hasTerms(), node.hasTerms);
  //
  //      if (node.floorDataLen > 0) {
  //        byte[] floorData = new byte[node.floorDataLen];
  //        reader.access.readBytes(node.floorDataFp, floorData, 0, floorData.length);
  //        assertEquals(expected.floorData(), new BytesRef(floorData));
  //      }
  //      return;
  //    }
  //    // Not match.
  //    assertEquals(expected, null);
  //  }

  private static void assertResultStepByNode(ARTReader reader, BytesRef term, Output expected)
      throws IOException {
    Node node = reader.getRoot();
    Node targetNode = null;
    while ((targetNode = reader.lookupChild(term, node)) != null) {
      if (node == targetNode) {
        break;
      }
      node = targetNode;
    }
    if (targetNode == null) {
      assertEquals(expected.fp(), node.outputFp);
      assertEquals(expected.hasTerms(), node.hasTerms);

      if (expected.floorData() != null) {
        byte[] floorData = new byte[expected.floorData().length];
        reader.access.readBytes(node.floorDataFp, floorData, 0, floorData.length);
        assertEquals(expected.floorData(), new BytesRef(floorData));
      }
      return;
    }
    // Not match.
    assertEquals(expected, null);
  }

  private static byte[] randomBytes() {
    byte[] bytes = new byte[random().nextInt(256) + 1];
    for (int i = 1; i < bytes.length; i++) {
      bytes[i] = (byte) random().nextInt(1 << (i % 9));
    }
    return bytes;
  }
}
