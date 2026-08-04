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
package org.apache.lucene.util.fst;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.RamUsageTester;
import org.apache.lucene.util.IntsRefBuilder;

public class TestDirectBufferFSTStore extends LuceneTestCase {

  private static final int MAX_BLOCK_BITS = 30;

  public void testEquivalenceWithOnHeapStore() throws IOException {
    SavedFST saved = buildSavedFst();
    FST<Long> onHeapFst = loadOnHeap(saved);
    FST<Long> directFst = loadDirect(saved);

    IntsRefFSTEnum<Long> onHeapEnum = new IntsRefFSTEnum<>(onHeapFst);
    IntsRefFSTEnum<Long> directEnum = new IntsRefFSTEnum<>(directFst);
    IntsRefFSTEnum.InputOutput<Long> onHeapEntry;
    IntsRefFSTEnum.InputOutput<Long> directEntry;
    while ((onHeapEntry = onHeapEnum.next()) != null) {
      directEntry = directEnum.next();
      assertNotNull(directEntry);
      assertEquals(onHeapEntry.input, directEntry.input);
      assertEquals(onHeapEntry.output, directEntry.output);
    }
    assertNull(directEnum.next());
  }

  public void testRamBytesUsed() throws IOException {
    SavedFST saved = buildSavedFst();
    DirectBufferFSTStore directStore =
        new DirectBufferFSTStore(saved.dataInput(), saved.metadata.getNumBytes());
    OnHeapFSTStore onHeapStore =
        new OnHeapFSTStore(MAX_BLOCK_BITS, saved.dataInput(), saved.metadata.getNumBytes());

    assertTrue(directStore.ramBytesUsed() < 1_000);
    assertTrue(onHeapStore.ramBytesUsed() > directStore.ramBytesUsed() + 100);
    assertTrue(RamUsageTester.ramUsed(directStore) < RamUsageTester.ramUsed(onHeapStore));
    assertNoLargeByteArray(directStore);
  }

  public void testBytesReader() throws IOException {
    SavedFST saved = buildSavedFst();
    FST<Long> fst = loadDirect(saved);
    FST.BytesReader reader = fst.getBytesReader();
    byte[] data = saved.dataBytes;
    reader.setPosition(data.length - 1);
    assertEquals(data[data.length - 1], reader.readByte());
    byte[] scratch = new byte[4];
    reader.setPosition(3);
    reader.readBytes(scratch, 0, 4);
    for (int i = 0; i < 4; i++) {
      assertEquals(data[3 - i], scratch[i]);
    }
  }

  public void testCopyFromFSTReader() throws IOException {
    SavedFST saved = buildSavedFst();
    OnHeapFSTStore source =
        new OnHeapFSTStore(MAX_BLOCK_BITS, saved.dataInput(), saved.metadata.getNumBytes());
    DirectBufferFSTStore copied = new DirectBufferFSTStore(source, saved.metadata.getNumBytes());

    ByteArrayOutputStream out = new ByteArrayOutputStream(saved.dataBytes.length);
    copied.writeTo(new OutputStreamDataOutput(out));
    assertArrayEquals(saved.dataBytes, out.toByteArray());
    assertNoLargeByteArray(copied);
  }

  private static FST<Long> loadOnHeap(SavedFST saved) throws IOException {
    return new FST<>(saved.metadata, saved.dataInput());
  }

  private static FST<Long> loadDirect(SavedFST saved) throws IOException {
    return FST.loadDirect(saved.metadata, saved.dataInput());
  }

  private static SavedFST buildSavedFst() throws IOException {
    PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
    FSTCompiler<Long> compiler =
        new FSTCompiler.Builder<>(FST.INPUT_TYPE.BYTE1, outputs).build();
    IntsRefBuilder scratch = new IntsRefBuilder();
    for (int i = 0; i < 256; i++) {
      scratch.clear();
      scratch.append(i);
      compiler.add(scratch.get(), (long) i);
    }
    FST.FSTMetadata<Long> metadata = compiler.compile();
    FST<Long> fst = FST.fromFSTReader(metadata, compiler.getFSTReader());

    ByteArrayOutputStream metaBytes = new ByteArrayOutputStream();
    ByteArrayOutputStream dataBytes = new ByteArrayOutputStream();
    fst.save(new OutputStreamDataOutput(metaBytes), new OutputStreamDataOutput(dataBytes));
    return new SavedFST(
        metaBytes.toByteArray(), dataBytes.toByteArray(), outputs);
  }

  private static void assertNoLargeByteArray(Object root) {
    List<byte[]> largeArrays = new ArrayList<>();
    RamUsageTester.ramUsed(
        root,
        new RamUsageTester.Accumulator() {
          @Override
          public long accumulateArray(
              Object array, long shallowSize, List<Object> values, Collection<Object> queue) {
            if (array instanceof byte[] bytes && bytes.length > 1_000_000) {
              largeArrays.add(bytes);
            }
            return super.accumulateArray(array, shallowSize, values, queue);
          }
        });
    assertEquals(0, largeArrays.size());
  }

  private static final class SavedFST {
    final byte[] metaBytes;
    final byte[] dataBytes;
    final FST.FSTMetadata<Long> metadata;

    SavedFST(byte[] metaBytes, byte[] dataBytes, PositiveIntOutputs outputs)
        throws IOException {
      this.metaBytes = metaBytes;
      this.dataBytes = dataBytes;
      this.metadata = FST.readMetadata(new ByteArrayDataInput(metaBytes), outputs);
    }

    DataInput dataInput() {
      return new ByteArrayDataInput(dataBytes);
    }
  }
}
