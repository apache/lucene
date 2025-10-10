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
package org.apache.lucene.codecs.lucene103.blocktree;

import static org.apache.lucene.codecs.lucene103.blocktree.TrieBuilder.ChildSaveStrategy;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.junit.Assert;

public class TestTrie extends LuceneTestCase {

  public void testStrategyChoose() {
    // bits use 32 bytes while reverse_array use 31 bytes, choose reverse_array
    assertSame(ChildSaveStrategy.REVERSE_ARRAY, ChildSaveStrategy.choose(0, 255, 226));
    // bits use 32 bytes while array use 31 bytes, choose array
    assertSame(ChildSaveStrategy.ARRAY, ChildSaveStrategy.choose(0, 255, 32));
    // array and bits both use 32 position bytes, we choose bits.
    assertSame(ChildSaveStrategy.BITS, ChildSaveStrategy.choose(0, 255, 33));
    // reverse_array and bits both use 32 position bytes, we choose bits.
    assertSame(ChildSaveStrategy.BITS, ChildSaveStrategy.choose(0, 255, 225));
  }

  public void testRandomTerms() throws Exception {
    Supplier<byte[]> supplier = TestTrie::randomBytes;
    testTrieBuilder(supplier, atLeast(1000));
    testTrieLookup(supplier, 12);
  }

  // TODO: incredibly slow
  @Nightly
  public void testVeryLongTerms() throws Exception {
    Supplier<byte[]> supplier =
        () -> {
          byte[] bytes = new byte[65535];
          for (int i = 1; i < bytes.length; i++) {
            bytes[i] = (byte) random().nextInt(i / 256 + 1);
          }
          return bytes;
        };
    testTrieLookup(supplier, 5);
  }

  public void testOneByteTerms() throws Exception {
    // heavily test single byte terms to generate various label distribution.
    Supplier<byte[]> supplier = () -> new byte[] {(byte) random().nextInt()};
    int round = atLeast(5);
    for (int i = 0; i < round; i++) {
      testTrieLookup(supplier, 10);
    }
  }

  private void testTrieBuilder(Supplier<byte[]> randomBytesSupplier, int count) {
    Map<BytesRef, TrieBuilder.Output> expected = new TreeMap<>();
    expected.put(new BytesRef(""), new TrieBuilder.Output(0L, false, new BytesRef("emptyOutput")));
    for (int i = 0; i < count; i++) {
      BytesRef key = new BytesRef(randomBytesSupplier.get());
      TrieBuilder.Output value =
          new TrieBuilder.Output(
              random().nextLong(1L << 62),
              random().nextBoolean(),
              new BytesRef(randomBytesSupplier.get()));
      expected.put(key, value);
    }

    TrieBuilder trieBuilder =
        TrieBuilder.bytesRefToTrie(
            new BytesRef(""), new TrieBuilder.Output(0L, false, new BytesRef("emptyOutput")));
    for (var entry : expected.entrySet()) {
      if (entry.getKey().equals(new BytesRef(""))) {
        continue;
      }
      TrieBuilder add = TrieBuilder.bytesRefToTrie(entry.getKey(), entry.getValue());
      trieBuilder.append(add);
      Assert.assertThrows(IllegalStateException.class, () -> add.append(trieBuilder));
      Assert.assertThrows(IllegalStateException.class, () -> trieBuilder.append(add));
    }
    Map<BytesRef, TrieBuilder.Output> actual = new TreeMap<>();
    trieBuilder.visit(actual::put);
    assertEquals(expected, actual);
  }

  private void testTrieLookup(Supplier<byte[]> randomBytesSupplier, int round) throws IOException {
    for (int iter = 1; iter <= round; iter++) {
      Map<BytesRef, TrieBuilder.Output> expected = new TreeMap<>();
      expected.put(
          new BytesRef(""), new TrieBuilder.Output(0L, false, new BytesRef("emptyOutput")));
      int n = 1 << iter;
      for (int i = 0; i < n; i++) {
        BytesRef key = new BytesRef(randomBytesSupplier.get());
        TrieBuilder.Output value =
            new TrieBuilder.Output(
                random().nextLong(1L << 62),
                random().nextBoolean(),
                random().nextBoolean() ? null : new BytesRef(randomBytesSupplier.get()));
        expected.put(key, value);
      }

      TrieBuilder trieBuilder =
          TrieBuilder.bytesRefToTrie(
              new BytesRef(""), new TrieBuilder.Output(0L, false, new BytesRef("emptyOutput")));
      for (var entry : expected.entrySet()) {
        if (entry.getKey().equals(new BytesRef(""))) {
          continue;
        }
        TrieBuilder add = TrieBuilder.bytesRefToTrie(entry.getKey(), entry.getValue());
        trieBuilder.append(add);
        Assert.assertThrows(IllegalStateException.class, () -> add.append(trieBuilder));
        Assert.assertThrows(IllegalStateException.class, () -> trieBuilder.append(add));
      }

      try (Directory directory = newDirectory()) {
        try (IndexOutput index = directory.createOutput("index", IOContext.DEFAULT);
            IndexOutput meta = directory.createOutput("meta", IOContext.DEFAULT)) {
          trieBuilder.save(meta, index);
          assertThrows(IllegalStateException.class, () -> trieBuilder.save(meta, index));
          assertThrows(
              IllegalStateException.class,
              () ->
                  trieBuilder.append(
                      TrieBuilder.bytesRefToTrie(
                          new BytesRef(), new TrieBuilder.Output(0L, true, null))));
        }

        try (IndexInput indexIn = directory.openInput("index", IOContext.DEFAULT);
            IndexInput metaIn = directory.openInput("meta", IOContext.DEFAULT)) {
          long start = metaIn.readVLong();
          long rootFP = metaIn.readVLong();
          long end = metaIn.readVLong();
          TrieReader reader = new TrieReader(indexIn.slice("outputs", start, end - start), rootFP);

          for (Map.Entry<BytesRef, TrieBuilder.Output> entry : expected.entrySet()) {
            assertResult(reader, entry.getKey(), entry.getValue());
          }

          int testNotFound = atLeast(100);
          for (int i = 0; i < testNotFound; i++) {
            BytesRef key = new BytesRef(randomBytes());
            while (expected.containsKey(key)) {
              key = new BytesRef(randomBytes());
            }
            BytesRef lastK = new BytesRef();
            for (BytesRef k : expected.keySet()) {
              if (k.compareTo(key) > 0) {
                assert lastK.compareTo(key) < 0;
                int mismatch1 =
                    Arrays.mismatch(
                        lastK.bytes,
                        lastK.offset,
                        lastK.offset + lastK.length,
                        key.bytes,
                        key.offset,
                        key.offset + key.length);
                int mismatch2 =
                    Arrays.mismatch(
                        k.bytes,
                        k.offset,
                        k.offset + k.length,
                        key.bytes,
                        key.offset,
                        key.offset + key.length);
                assertNotFoundOnLevelN(reader, key, Math.max(mismatch1, mismatch2));
                break;
              }
              lastK = k;
            }
          }
        }
      }
    }
  }

  private static byte[] randomBytes() {
    byte[] bytes = new byte[random().nextInt(256) + 1];
    for (int i = 1; i < bytes.length; i++) {
      bytes[i] = (byte) random().nextInt(1 << (i % 9));
    }
    return bytes;
  }

  private static void assertResult(TrieReader reader, BytesRef term, TrieBuilder.Output expected)
      throws IOException {
    TrieReader.Node parent = reader.root;
    TrieReader.Node child = new TrieReader.Node();
    for (int i = 0; i < term.length; i++) {
      TrieReader.Node found = reader.lookupChild(term.bytes[i + term.offset] & 0xFF, parent, child);
      Assert.assertNotNull(found);
      parent = child;
      child = new TrieReader.Node();
    }
    assertTrue(parent.hasOutput());
    assertEquals(expected.fp(), parent.outputFp);
    assertEquals(expected.hasTerms(), parent.hasTerms);
    if (expected.floorData() == null) {
      assertFalse(parent.isFloor());
    } else {
      byte[] bytes = new byte[expected.floorData().length];
      parent.floorData(reader).readBytes(bytes, 0, bytes.length);
      assertArrayEquals(BytesRef.deepCopyOf(expected.floorData()).bytes, bytes);
    }
  }

  private static void assertNotFoundOnLevelN(TrieReader reader, BytesRef term, int n)
      throws IOException {
    TrieReader.Node parent = reader.root;
    TrieReader.Node child = new TrieReader.Node();
    for (int i = 0; i < term.length; i++) {
      TrieReader.Node found = reader.lookupChild(term.bytes[i + term.offset] & 0xFF, parent, child);
      if (i == n) {
        assertNull(found);
        break;
      }
      Assert.assertNotNull(found);
      parent = child;
      child = new TrieReader.Node();
    }
  }
}
