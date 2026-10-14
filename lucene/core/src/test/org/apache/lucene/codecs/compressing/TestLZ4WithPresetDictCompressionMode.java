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
package org.apache.lucene.codecs.compressing;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.codecs.lucene90.LZ4WithPresetDictCompressionMode;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.BytesRef;

public class TestLZ4WithPresetDictCompressionMode extends AbstractTestCompressionMode {

  @Override
  public void setUp() throws Exception {
    super.setUp();
    mode = new LZ4WithPresetDictCompressionMode();
  }

  public void testReuseDictionary() throws IOException {
    byte[] original = randomArray(random(), 8192, 15);
    byte[] compressed = compress(original, 0, original.length);
    Decompressor decompressor = mode.newDecompressor();
    BytesRef bytes = new BytesRef();
    CountingInput first = new CountingInput(compressed);
    decompressor.decompress(first, original.length, 4096, 512, bytes, true);
    assertEquals(new BytesRef(original, 4096, 512), bytes);
    CountingInput second = new CountingInput(compressed);
    decompressor.decompress(second, original.length, 4096, 512, bytes, true);
    assertEquals(new BytesRef(original, 4096, 512), bytes);

    ByteArrayDataInput header = new ByteArrayDataInput(compressed);
    header.readVInt(); // dictionary length
    header.readVInt(); // block length
    int compressedDictionaryLength = header.readVInt();
    assertTrue(compressedDictionaryLength > 0);
    assertEquals(compressedDictionaryLength, first.bytesRead - second.bytesRead);

    CountingInput cloneInput = new CountingInput(compressed);
    decompressor.clone().decompress(cloneInput, original.length, 4096, 512, bytes, true);
    assertEquals(new BytesRef(original, 4096, 512), bytes);
    assertEquals(first.bytesRead, cloneInput.bytesRead);
  }

  public void testDefaultDecompressionAndEmptyRequest() throws IOException {
    byte[] first = new byte[8192];
    byte[] second = new byte[8192];
    Arrays.fill(first, (byte) 1);
    Arrays.fill(second, (byte) 2);
    byte[] compressedFirst = compress(first, 0, first.length);
    byte[] compressedSecond = compress(second, 0, second.length);
    Decompressor decompressor = mode.newDecompressor();
    BytesRef bytes = new BytesRef();
    decompressor.decompress(
        new ByteArrayDataInput(compressedFirst), first.length, 4096, 512, bytes);
    assertEquals(new BytesRef(first, 4096, 512), bytes);
    // The original API must not reuse a dictionary from a different compressed block.
    decompressor.decompress(
        new ByteArrayDataInput(compressedSecond), second.length, 4096, 512, bytes);
    assertEquals(new BytesRef(second, 4096, 512), bytes);
    // An empty first request for a new block must invalidate the previous dictionary.
    decompressor.decompress(
        new ByteArrayDataInput(compressedFirst), first.length, 0, 0, bytes, false);
    assertEquals(0, bytes.length);
    decompressor.decompress(
        new ByteArrayDataInput(compressedFirst), first.length, 4096, 512, bytes, true);
    assertEquals(new BytesRef(first, 4096, 512), bytes);
  }

  private static class CountingInput extends DataInput {
    private final ByteArrayDataInput input;
    private int bytesRead;

    CountingInput(byte[] bytes) {
      input = new ByteArrayDataInput(bytes);
    }

    @Override
    public byte readByte() {
      bytesRead++;
      return input.readByte();
    }

    @Override
    public void readBytes(byte[] bytes, int offset, int length) {
      bytesRead += length;
      input.readBytes(bytes, offset, length);
    }

    @Override
    public void skipBytes(long count) {
      input.skipBytes(count);
    }
  }
}
