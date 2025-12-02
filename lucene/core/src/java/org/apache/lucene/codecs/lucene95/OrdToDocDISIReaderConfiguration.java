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

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.KnnVectorValues.DocIndexIterator;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BpVectorReorderer;
import org.apache.lucene.util.GroupVIntUtil;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.DirectMonotonicWriter;
import org.apache.lucene.util.packed.DirectReader;
import org.apache.lucene.util.packed.DirectWriter;

/**
 * Configuration for {@link DirectMonotonicReader} and {@link IndexedDISI} for reading sparse
 * vectors. The format in the static writing methods adheres to the Lucene95HnswVectorsFormat
 */
public class OrdToDocDISIReaderConfiguration {

  /**
   * Writes out the docsWithField and ordToDoc mapping to the outputMeta and vectorData
   * respectively. This is in adherence to the Lucene95HnswVectorsFormat.
   *
   * <p>Within outputMeta the format is as follows:
   *
   * <ul>
   *   <li><b>[int64]</b> if equal to -2, empty - no vector values. If equal to -1, dense – all
   *       documents have values for a field, and -3, dense and reordered. Otherwise, the vectors
   *       are sparse – some documents are missing values, and this value is an offset to the DocIds
   *       having values in the vector data, encoded by IndexedDISI as a bitset.
   *   <li><b>[int64]</b> DocIds length in sparse case, otherwise 0.
   *   <li><b>[int16]</b> jump table entry count of DocIds in sparse cases, otherwise -1.
   *   <li><b>[int8]</b> dense rank power of DocIds in sparse cases, otherwise -1.
   *   <li>** WHEN sparse and vectors are ordered monotonically with docids:
   *   <li><b>[int64]</b>offset to OrdToDoc (as written by DirectMonotonicWriter)
   *   <li><b>[vint]</b>direct monotonic block shift of OrdToDoc
   *   <li><b>[int64]</b>size of OrdToDoc
   *   <li>** WHEN nonempty and <i>reordered</i> (sparse or dense):
   *   <li><b>[int64]</b>offset to OrdToDoc encoded using DirectWriter (addressesOffset)
   *   <li><b>[vint]</b> 0 (blockShift)
   *   <li><b>[int64]</b>length of OrdToDoc (addressesLength)
   *   <li><b>[byte]</b> bits used for DirectWriter encoding of ordToDoc
   *   <li><b>[int64]</b>length of DocToOrd encoded using GroupVint (docToOrdLength)
   * </ul>
   *
   * <p>Within the vectorData the format is as follows:
   *
   * <ul>
   *   <li>DocIds encoded by {@link IndexedDISI#writeBitSet(DocIdSetIterator, IndexOutput, byte)};
   *       only present in sparse cases.
   *   <li>OrdToDoc encoded by either {@link org.apache.lucene.util.packed.DirectMonotonicWriter},
   *       in sparse docid-ordered case, or as in int[] in reordered cases.
   * </ul>
   *
   * @param directMonotonicBlockShift used when writing OrdToDocs
   * @param outputMeta the outputMeta
   * @param vectorData the vectorData
   * @param count the count of docs with vectors
   * @param maxDoc the maxDoc for the index
   * @param docsWithField the docs containing a vector field
   * @param sortMap encoding the mapping from ordered doc (new) to doc (old); null if they are in
   *     the same order
   * @throws IOException thrown when writing data fails to either output
   */
  public static void writeStoredMeta(
      int directMonotonicBlockShift,
      IndexOutput outputMeta,
      IndexOutput vectorData,
      int count,
      int maxDoc,
      DocsWithFieldSet docsWithField,
      BpVectorReorderer.DocMap sortMap)
      throws IOException {
    if (count == 0) {
      outputMeta.writeLong(-2); // docsWithFieldOffset
      outputMeta.writeLong(0L); // docsWithFieldLength
      outputMeta.writeShort((short) -1); // jumpTableEntryCount
      outputMeta.writeByte((byte) -1); // denseRankPower
    } else if (count == maxDoc) {
      if (sortMap != null) {
        outputMeta.writeLong(-3); // docsWithFieldOffset
      } else {
        outputMeta.writeLong(-1); // docsWithFieldOffset
      }
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

      if (sortMap == null) {
        writeMonotonicOrdToDoc(
            directMonotonicBlockShift, outputMeta, vectorData, count, docsWithField);
      }
    }
    assert sortMap == null || count > 0;
    if (sortMap != null && count > 0) {
      writeRandomOrdToDoc(outputMeta, vectorData, maxDoc, sortMap);
    }
  }

  private static void writeRandomOrdToDoc(
      IndexOutput outputMeta, IndexOutput vectorData, int maxDoc, BpVectorReorderer.DocMap ordToDocMap)
      throws IOException {
    int bitsRequired = DirectWriter.bitsRequired(maxDoc);
    long start = vectorData.getFilePointer();
    outputMeta.writeLong(start);
    outputMeta.writeVInt(0); // blockShift = 0
    // iterate over the vector ordinals and write the docs they are in, in a format that supports
    // random access
    DirectWriter ordToDocWriter =
        DirectWriter.getInstance(vectorData, ordToDocMap.size(), bitsRequired);
    for (int ord = 0; ord < ordToDocMap.size(); ord++) {
      ordToDocWriter.add(ordToDocMap.newToOld(ord));
      ;
    }
    ordToDocWriter.finish();
    long startOrds = vectorData.getFilePointer();
    outputMeta.writeLong(startOrds - start);
    outputMeta.writeByte((byte) bitsRequired);
    // For docToOrd we do not require random access; this only needs to support forward iteration.
    // Write the ordinals in docid order using GroupVarInt encoding:
    // Note we don't need to encode the actual docids or gaps here since we will iterate to docs
    // having values using docsWithField bitset while advancing through this array of ords
    GroupVIntUtil.writeGroupVInts(
        vectorData,
        new byte[GroupVIntUtil.MAX_LENGTH_PER_GROUP],
        ordToDocMap.oldToNew,
        ordToDocMap.oldToNew.length);
    // write length of ordinals
    outputMeta.writeLong(vectorData.getFilePointer() - startOrds);
  }

  // write monotonic ordToDoc mapping
  private static void writeMonotonicOrdToDoc(
      int directMonotonicBlockShift,
      IndexOutput outputMeta,
      IndexOutput vectorData,
      int count,
      DocsWithFieldSet docsWithField)
      throws IOException {
    long start = vectorData.getFilePointer();
    outputMeta.writeLong(start);
    outputMeta.writeVInt(directMonotonicBlockShift);
    // dense case and empty case do not need to store ordToMap mapping
    final DirectMonotonicWriter ordToDocWriter =
        DirectMonotonicWriter.getInstance(outputMeta, vectorData, count, directMonotonicBlockShift);
    DocIdSetIterator iterator = docsWithField.iterator();
    for (int doc = iterator.nextDoc(); doc != NO_MORE_DOCS; doc = iterator.nextDoc()) {
      ordToDocWriter.add(doc);
    }
    ordToDocWriter.finish();
    outputMeta.writeLong(vectorData.getFilePointer() - start);
  }

  /**
   * Reads in the necessary fields stored in the outputMeta to configure {@link
   * DirectMonotonicReader} and {@link IndexedDISI}.
   *
   * @param inputMeta the inputMeta, previously written to via {@link #writeStoredMeta(int,
   *     IndexOutput, IndexOutput, int, int, DocsWithFieldSet, BpVectorReorderer.DocMap)}
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
    long docToOrdLength = 0;
    byte bitsRequired = 0;
    if (docsWithFieldOffset == -3) {
      addressesOffset = inputMeta.readLong();
      blockShift = inputMeta.readVInt();
      assert blockShift == 0;
      addressesLength = inputMeta.readLong();
      docToOrdLength = inputMeta.readLong();
    } else if (docsWithFieldOffset > -1) {
      addressesOffset = inputMeta.readLong();
      blockShift = inputMeta.readVInt();
      if (blockShift > 0) {
        meta = DirectMonotonicReader.loadMeta(inputMeta, size, blockShift);
      }
      addressesLength = inputMeta.readLong();
      if (blockShift == 0) {
        bitsRequired = inputMeta.readByte();
        docToOrdLength = inputMeta.readLong();
      }
    }
    return new OrdToDocDISIReaderConfiguration(
        size,
        jumpTableEntryCount,
        addressesOffset,
        addressesLength,
        docToOrdLength,
        docsWithFieldOffset,
        docsWithFieldLength,
        denseRankPower,
        bitsRequired,
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
  final byte bitsRequired;

  // the following variables used to read ordToDoc are encoded either by DirectMonotonicWriter, in
  // the sparse non-reordered case, or DirectWriter in the reordered case.  Reordered case is
  // indicated by
  // meta = null and addressesOffset > 0.
  final long addressesOffset, addressesLength;
  // This is used to read docToOrd encoded using GroupVInt, only present in reordered case
  final long docToOrdLength;
  final DirectMonotonicReader.Meta meta;

  OrdToDocDISIReaderConfiguration(
      int size,
      short jumpTableEntryCount,
      long addressesOffset,
      long addressesLength,
      long docToOrdLength,
      long docsWithFieldOffset,
      long docsWithFieldLength,
      byte denseRankPower,
      byte bitsRequired,
      DirectMonotonicReader.Meta meta) {
    this.size = size;
    this.jumpTableEntryCount = jumpTableEntryCount;
    this.addressesOffset = addressesOffset;
    this.addressesLength = addressesLength;
    this.docsWithFieldOffset = docsWithFieldOffset;
    this.docsWithFieldLength = docsWithFieldLength;
    this.docToOrdLength = docToOrdLength;
    this.denseRankPower = denseRankPower;
    this.bitsRequired = bitsRequired;
    this.meta = meta;
  }

  /**
   * @param dataIn the dataIn
   * @return the IndexedDISI for sparse values
   * @throws IOException thrown when reading data fails
   */
  public IndexedDISI getIndexedDISI(IndexInput dataIn) throws IOException {
    assert docsWithFieldOffset > -1;
    assert denseRankPower > 0;
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
    assert meta != null;
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
    return docsWithFieldOffset == -1 || docsWithFieldOffset == -3;
  }

  /**
   * @return If true, the field is reordered: vector ordinals are not monotonic with docids.
   */
  public boolean isReordered() {
    return (docsWithFieldOffset > -1 || docsWithFieldOffset == -3) && meta == null;
  }

  public OrdToDocReader getOrdToDocReader(IndexInput dataIn, int size) throws IOException {
    return new OrdToDocReader(dataIn.clone(), size);
  }

  public class OrdToDocReader {
    private final int size;
    private final IndexInput dataIn;
    private final LongValues ordToDoc;
    private final IndexInput docToOrdSlice;
    private final IndexedDISI disi;

    OrdToDocReader(IndexInput dataIn, int size) throws IOException {
      this.size = size;
      this.dataIn = dataIn;
      ordToDoc =
          DirectReader.getInstance(
              dataIn.randomAccessSlice(addressesOffset, addressesLength), bitsRequired);
      docToOrdSlice =
          dataIn.slice("OrdToDocReader", addressesOffset + addressesLength, docToOrdLength);
      if (isDense()) {
        disi = null;
      } else {
        disi = getIndexedDISI(dataIn.clone());
      }
    }

    public int ordToDoc(int ord) {
      return (int) ordToDoc.get(ord);
    }

    DocIndexIterator iterator() {
      int[] ordBuffer = new int[128];
      IndexInput docToOrd = docToOrdSlice.clone();

      return new DocIndexIterator() {
        int doc = -1;
        int pos = -1;

        void nextPos() throws IOException {
          ++pos;
          if (pos % ordBuffer.length == 0) {
            int remaining = size - pos;
            GroupVIntUtil.readGroupVInts(
                docToOrd, ordBuffer, Math.min(remaining, ordBuffer.length));
          }
        }

        @Override
        public int docID() {
          return doc;
        }

        @Override
        public int index() {
          return ordBuffer[pos % ordBuffer.length];
        }

        @Override
        public int nextDoc() throws IOException {
          if (doc == NO_MORE_DOCS) {
            return NO_MORE_DOCS;
          }
          if (disi == null) {
            // dense
            if (doc == size - 1) {
              doc = NO_MORE_DOCS;
            } else {
              ++doc;
            }
          } else {
            // sparse
            doc = disi.nextDoc();
          }
          if (doc != NO_MORE_DOCS) {
            nextPos();
          }
          return doc;
        }

        @Override
        public int advance(int target) throws IOException {
          if (disi != null) {
            doc = slowAdvance(target);
          } else if (target > size - 1) {
            doc = NO_MORE_DOCS;
          } else {
            doc = target;
            pos = doc - 1;
            nextPos();
          }
          return doc;
        }

        @Override
        public long cost() {
          return size;
        }
      };
    }
  }
}
