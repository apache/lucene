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
package org.apache.lucene.codecs.lucene90.blocktree;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.junit.Assert;

public class TestTrie extends LuceneTestCase {

  public void testTrie() {
    Map<BytesRef, Trie.Output> actual = new TreeMap<>();
    Map<BytesRef, Trie.Output> expected = new TreeMap<>();

    expected.put(new BytesRef(""), new Trie.Output(0L, false, new BytesRef("emptyOutput")));
    Trie trie = new Trie(new BytesRef(""), new Trie.Output(0L, false, new BytesRef("emptyOutput")));

    int n = random().nextInt(10000);
    for (int i = 0; i < n; i++) {
      BytesRef key = new BytesRef(randomBytes());
      Trie.Output value =
          new Trie.Output(
              random().nextLong(1L << 62), random().nextBoolean(), new BytesRef(randomBytes()));
      expected.put(key, value);
      trie.putAll(new Trie(key, value));
    }
    trie.forEach(actual::put);
    assertEquals(expected, actual);
  }

  public void testTrieLookup() throws IOException {

    for (int iter = 1; iter <= 12; iter++) {
      Map<BytesRef, Trie.Output> expected = new TreeMap<>();

      expected.put(new BytesRef(""), new Trie.Output(0L, false, new BytesRef("emptyOutput")));
      Trie trie =
          new Trie(new BytesRef(""), new Trie.Output(0L, false, new BytesRef("emptyOutput")));

      int n = 1 << iter;
      for (int i = 0; i < n; i++) {
        BytesRef key = new BytesRef(randomBytes());
        Trie.Output value =
            new Trie.Output(
                random().nextLong(1L << 62),
                random().nextBoolean(),
                random().nextBoolean() ? null : new BytesRef(randomBytes()));
        expected.put(key, value);
        trie.putAll(new Trie(key, value));
      }

      try (Directory directory = newDirectory()) {
        try (IndexOutput index = directory.createOutput("index", IOContext.DEFAULT);
            IndexOutput meta = directory.createOutput("meta", IOContext.DEFAULT)) {
          trie.save(meta, index);
        }

        try (IndexInput indexIn = directory.openInput("index", IOContext.DEFAULT);
            IndexInput metaIn = directory.openInput("meta", IOContext.DEFAULT)) {
          long start = metaIn.readVLong();
          long rootFP = metaIn.readVLong();
          long end = metaIn.readVLong();
          TrieReader reader = new TrieReader(indexIn.slice("outputs", start, end - start), rootFP);

          for (Map.Entry<BytesRef, Trie.Output> entry : expected.entrySet()) {
            assertResult(reader, entry.getKey(), entry.getValue());
          }

          int x = atLeast(100);
          for (int i = 0; i < x; i++) {
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
    byte[] bytes = new byte[random().nextInt(256)];
    for (int i = 1; i < bytes.length; i++) {
      bytes[i] = (byte) random().nextInt(1 << (i % 9));
    }
    return bytes;
  }

  private static void assertResult(TrieReader reader, BytesRef term, Trie.Output expected)
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
    assertEquals(term.toString(), expected.fp(), parent.outputFp);
    assertEquals(term.toString(), expected.hasTerms(), parent.hasTerms);
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
