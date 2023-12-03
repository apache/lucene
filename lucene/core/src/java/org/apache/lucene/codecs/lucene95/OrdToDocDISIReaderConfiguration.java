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

package org.apache.lucene.codecs.lucene95;

import java.io.IOException;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.DirectMonotonicWriter;

/**
 * Configuration for {@link DirectMonotonicReader} and {@link IndexedDISI} for reading sparse
 * vectors. The format in the static writing methods adheres to the Lucene95HnswVectorsFormat
 */
public class OrdToDocDISIReaderConfiguration implements Accountable {

  private static final long SHALLOW_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(OrdToDocDISIReaderConfiguration.class);

  /**
   * Writes out the docsWithField and ordToDoc mapping to the outputMeta and vectorData
   * respectively. This is in adherence to the Lucene95HnswVectorsFormat.
   *
   * <p>Within outputMeta the format is as follows:
   *
   * <ul>
   *   <li><b>[int8]</b> if equals to -2, empty - no vectory values. If equals to -1, dense – all
   *       documents have values for a field. If equals to 0, sparse – some documents missing
   *       values.
   *   <li>DocIds were encoded by {@link IndexedDISI#writeBitSet(DocIdSetIterator, IndexOutput,
   *       byte)}
   *   <li>OrdToDoc was encoded by {@link org.apache.lucene.util.packed.DirectMonotonicWriter}, note
   *       that only in sparse case
   * </ul>
   *
   * <p>Within the vectorData the format is as follows:
   *
   * <ul>
   *   <li>DocIds encoded by {@link IndexedDISI#writeBitSet(DocIdSetIterator, IndexOutput, byte)},
   *       note that only in sparse case
   *   <li>OrdToDoc was encoded by {@link org.apache.lucene.util.packed.DirectMonotonicWriter}, note
   *       that only in sparse case
   * </ul>
   *
   * @param outputMeta the outputMeta
   * @param vectorData the vectorData
   * @param count the count of docs with vectors
   * @param maxDoc the maxDoc for the index
   * @param docsWithField the docs contaiting a vector field
   * @throws IOException thrown when writing data fails to either output
   */
  public static void writeStoredMeta(
      int directMonotonicBlockShift,
      IndexOutput outputMeta,
      IndexOutput vectorData,
      int count,
      int maxDoc,
      DocsWithFieldSet docsWithField)
      throws IOException {
    if (count == 0) {
      outputMeta.writeLong(-2); // docsWithFieldOffset
      outputMeta.writeLong(0L); // docsWithFieldLength
      outputMeta.writeShort((short) -1); // jumpTableEntryCount
      outputMeta.writeByte((byte) -1); // denseRankPower
    } else if (count == maxDoc) {
      outputMeta.writeLong(-1); // docsWithFieldOffset
      outputMeta.writeLong(0L); // docsWithFieldLength
      outputMeta.writeShort((short) -1); // jumpTableEntryCount
      outputMeta.writeByte((byte) -1); // denseRankPower
    } else {
      long offset = vectorData.getFilePointer();
      outputMeta.writeLong(offset); // docsWithFieldOffset
      final short jumpTableEntryCount =
          IndexedDISI.writeBitSet(
              docsWithField.iterator(), vectorData, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
      outputMeta.writeLong(vectorData.getFilePointer() - offset); // docsWithFieldLength
      outputMeta.writeShort(jumpTableEntryCount);
      outputMeta.writeByte(IndexedDISI.DEFAULT_DENSE_RANK_POWER);

      // write ordToDoc mapping
      long start = vectorData.getFilePointer();
      outputMeta.writeLong(start);
      outputMeta.writeVInt(directMonotonicBlockShift);
      // dense case and empty case do not need to store ordToMap mapping
      final DirectMonotonicWriter ordToDocWriter =
          DirectMonotonicWriter.getInstance(
              outputMeta, vectorData, count, directMonotonicBlockShift);
      DocIdSetIterator iterator = docsWithField.iterator();
      for (int doc = iterator.nextDoc();
          doc != DocIdSetIterator.NO_MORE_DOCS;
          doc = iterator.nextDoc()) {
        ordToDocWriter.add(doc);
      }
      ordToDocWriter.finish();
      outputMeta.writeLong(vectorData.getFilePointer() - start);
    }
  }

  /**
   * Reads in the necessary fields stored in the outputMeta to configure {@link
   * DirectMonotonicReader} and {@link IndexedDISI}.
   *
   * @param inputMeta the inputMeta, previously written to via {@link #writeStoredMeta(int,
   *     IndexOutput, IndexOutput, int, int, DocsWithFieldSet)}
   * @param size The number of vectors
   * @return the configuration required to read sparse vectors
   * @throws IOException thrown when reading data fails
   */
  public static OrdToDocDISIReaderConfiguration fromStoredMeta(IndexInput inputMeta, int size)
      throws IOException {
    long docsWithFieldOffset = inputMeta.readLong();
    long docsWithFieldLength = inputMeta.readLong();
    short jumpTableEntryCount = inputMeta.readShort();
    byte denseRankPower = inputMeta.readByte();
    long addressesOffset = 0;
    int blockShift = 0;
    DirectMonotonicReader.Meta meta = null;
    long addressesLength = 0;
    if (docsWithFieldOffset > -1) {
      addressesOffset = inputMeta.readLong();
      blockShift = inputMeta.readVInt();
      meta = DirectMonotonicReader.loadMeta(inputMeta, size, blockShift);
      addressesLength = inputMeta.readLong();
    }
    return new OrdToDocDISIReaderConfiguration(
        size,
        jumpTableEntryCount,
        addressesOffset,
        addressesLength,
        docsWithFieldOffset,
        docsWithFieldLength,
        denseRankPower,
        meta);
  }

  final int size;
  // the following four variables used to read docIds encoded by IndexDISI
  // special values of docsWithFieldOffset are -1 and -2
  // -1 : dense
  // -2 : empty
  // other: sparse
  final short jumpTableEntryCount;
  final long docsWithFieldOffset, docsWithFieldLength;
  final byte denseRankPower;

  // the following four variables used to read ordToDoc encoded by DirectMonotonicWriter
  // note that only spare case needs to store ordToDoc
  final long addressesOffset, addressesLength;
  final DirectMonotonicReader.Meta meta;

  OrdToDocDISIReaderConfiguration(
      int size,
      short jumpTableEntryCount,
      long addressesOffset,
      long addressesLength,
      long docsWithFieldOffset,
      long docsWithFieldLength,
      byte denseRankPower,
      DirectMonotonicReader.Meta meta) {
    this.size = size;
    this.jumpTableEntryCount = jumpTableEntryCount;
    this.addressesOffset = addressesOffset;
    this.addressesLength = addressesLength;
    this.docsWithFieldOffset = docsWithFieldOffset;
    this.docsWithFieldLength = docsWithFieldLength;
    this.denseRankPower = denseRankPower;
    this.meta = meta;
  }

  @Override
  public long ramBytesUsed() {
    return SHALLOW_SIZE + RamUsageEstimator.sizeOf(meta);
  }

  /**
   * @param dataIn the dataIn
   * @return the IndexedDISI for sparse values
   * @throws IOException thrown when reading data fails
   */
  public IndexedDISI getIndexedDISI(IndexInput dataIn) throws IOException {
    assert docsWithFieldOffset > -1;
    return new IndexedDISI(
        dataIn,
        docsWithFieldOffset,
        docsWithFieldLength,
        jumpTableEntryCount,
        denseRankPower,
        size);
  }

  /**
   * @param dataIn the dataIn
   * @return the DirectMonotonicReader for sparse values
   * @throws IOException thrown when reading data fails
   */
  public DirectMonotonicReader getDirectMonotonicReader(IndexInput dataIn) throws IOException {
    assert docsWithFieldOffset > -1;
    final RandomAccessInput addressesData =
        dataIn.randomAccessSlice(addressesOffset, addressesLength);
    return DirectMonotonicReader.getInstance(meta, addressesData);
  }

  /**
   * @return If true, the field is empty, no vector values. If false, the field is either dense or
   *     sparse.
   */
  public boolean isEmpty() {
    return docsWithFieldOffset == -2;
  }

  /**
   * @return If true, the field is dense, all documents have values for a field. If false, the field
   *     is sparse, some documents missing values.
   */
  public boolean isDense() {
    return docsWithFieldOffset == -1;
  }
}
