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

package org.apache.lucene.sandbox.codecs.lucene99.randomaccess;

import java.io.IOException;
import org.apache.lucene.codecs.lucene99.Lucene99PostingsFormat.IntBlockTermState;
import org.apache.lucene.sandbox.codecs.lucene99.randomaccess.TestTermStateCodecImpl.TermStateTestFixture;
import org.apache.lucene.sandbox.codecs.lucene99.randomaccess.bitpacking.BitPerBytePacker;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.ArrayUtil;

public class TestTermDataWriter extends LuceneTestCase {

  public void testWriterAndDeserialize() throws IOException {
    TermStateTestFixture testFixture = TestTermStateCodecImpl.getTermStateTestFixture(777);

    try (Directory testDir = newDirectory()) {
      IndexOutput metaOut = testDir.createOutput("segment_meta", IOContext.DEFAULT);
      IndexOutput metadataOut = testDir.createOutput("term_meta_1", IOContext.DEFAULT);
      IndexOutput dataOut = testDir.createOutput("term_data_11", IOContext.DEFAULT);
      TermDataWriter writer = new TermDataWriter(testFixture.codec(), metadataOut, dataOut);
      for (var termState : testFixture.termStatesArray()) {
        writer.addTermState(termState);
      }
      writer.finish();
      metaOut.writeVLong(writer.getTotalMetaDataBytesWritten());
      metaOut.writeVLong(writer.getTotalDataBytesWritten());
      metaOut.close();
      metadataOut.close();
      dataOut.close();

      BitPerBytePacker referenceBitPacker = new BitPerBytePacker();
      // total size 777; there will be 4 blocks total.
      // The extra 8 byte per block is the long offset for where the block starts within data bytes.
      byte[] expectedMetadata = new byte[(testFixture.codec().getMetadataBytesLength() + 8) * 4];
      ByteArrayDataOutput expectedMetadataOut = new ByteArrayDataOutput(expectedMetadata);
      for (int start = 0;
          start < testFixture.termStatesArray().length;
          start += TermDataWriter.NUM_TERMS_PER_BLOCK) {
        expectedMetadataOut.writeLong(referenceBitPacker.getCompactBytes().length);
        byte[] metadata =
            testFixture
                .codec()
                .encodeBlock(
                    ArrayUtil.copyOfSubArray(
                        testFixture.termStatesArray(),
                        start,
                        Math.min(
                            start + TermDataWriter.NUM_TERMS_PER_BLOCK,
                            testFixture.termStatesArray().length)),
                    referenceBitPacker);
        expectedMetadataOut.writeBytes(metadata, 0, metadata.length);
      }
      ByteSlice expectedDataSlice = new ByteArrayByteSlice(referenceBitPacker.getCompactBytes());
      ByteSlice expectedMetadataSlice = new ByteArrayByteSlice(expectedMetadata);
      TermData expected = new TermData(expectedMetadataSlice, expectedDataSlice);

      IndexInput metaIn = testDir.openInput("segment_meta", IOContext.DEFAULT);
      IndexInput metadataIn = testDir.openInput("term_meta_1", IOContext.DEFAULT);
      IndexInput dataIn = testDir.openInput("term_data_11", IOContext.DEFAULT);

      TermDataProvider actualProvider =
          TermDataProvider.deserializeOnHeap(metaIn.clone(), metadataIn.clone(), dataIn.clone());
      assertByteSlice(expected.metadata(), actualProvider.metadataProvider().newByteSlice());
      assertByteSlice(expected.data(), actualProvider.dataProvider().newByteSlice());
      testDecodeTermState(testFixture, actualProvider);

      actualProvider =
          TermDataProvider.deserializeOnHeap(metaIn.clone(), metadataIn.clone(), dataIn.clone());
      assertByteSlice(expected.metadata(), actualProvider.metadataProvider().newByteSlice());
      assertByteSlice(expected.data(), actualProvider.dataProvider().newByteSlice());
      testDecodeTermState(testFixture, actualProvider);

      metaIn.close();
      metadataIn.close();
      dataIn.close();
    }
  }

  private static void testDecodeTermState(
      TermStateTestFixture testFixture, TermDataProvider actualProvider) throws IOException {
    TermData actual =
        new TermData(
            actualProvider.metadataProvider().newByteSlice(),
            actualProvider.dataProvider().newByteSlice());
    for (int i = 0; i < testFixture.termStatesArray().length; i++) {
      IntBlockTermState expectedTermState = testFixture.termStatesArray()[i];
      IntBlockTermState decoded = actual.getTermState(testFixture.codec(), i);
      assertEquals(expectedTermState.docFreq, decoded.docFreq);
      assertEquals(expectedTermState.docStartFP, decoded.docStartFP);
    }
  }

  private static void assertByteSlice(ByteSlice expected, ByteSlice actual) throws IOException {
    assertEquals(expected.size(), actual.size());
    byte[] bytesExpected = new byte[(int) expected.size()];
    ByteArrayDataOutput out = new ByteArrayDataOutput(bytesExpected);
    expected.writeAll(out);

    byte[] bytesActual = new byte[(int) actual.size()];
    ByteArrayDataOutput out1 = new ByteArrayDataOutput(bytesActual);
    actual.writeAll(out1);
    assertArrayEquals(bytesExpected, bytesActual);
  }
}
