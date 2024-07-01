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

package org.apache.lucene.codecs.lucene99;

import java.io.IOException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.DirectMonotonicWriter;

/**
 * Configuration for {@link DirectMonotonicReader} for reading varying length vector data offsets in
 * Tensor values.
 *
 * <p>The format in the static writing methods adheres to the Lucene99FlatTensorsFormat
 */
public class TensorDataOffsetsReaderConfiguration {

  /**
   * Writes out data offsets for each tensor value per document. Across documents, tensors can have
   * variable number of vectors. Data offsets written here are used to read variable sized vector
   * slices per document (tensor ordinal). This is in adherence to the Lucene99FlatTensorsFormat.
   *
   * <p>Within outputMeta the format is as follows:
   *
   * <ul>
   *   <li>TensorDataOffsets encoded by {@link DirectMonotonicWriter}
   * </ul>
   *
   * <p>Within the vectorData the format is as follows:
   *
   * <ul>
   *   <li>TensorDataOffsets encoded by {@link DirectMonotonicWriter}
   * </ul>
   *
   * @param directMonotonicBlockShift block shift to use for DirectMonotonicWriter
   * @param outputMeta the outputMeta
   * @param tensorData the tensorData
   * @param tensorDataOffsets array holding data offsets for each tensor
   * @throws IOException thrown when writing data fails to either output
   */
  public static void writeStoredMeta(
      int directMonotonicBlockShift,
      IndexOutput outputMeta,
      IndexOutput tensorData,
      long[] tensorDataOffsets)
      throws IOException {
    long start = tensorData.getFilePointer();
    outputMeta.writeLong(start);
    outputMeta.writeVInt(directMonotonicBlockShift);
    final int numValues = tensorDataOffsets.length;
    outputMeta.writeVInt(numValues);
    final DirectMonotonicWriter dataOffsetsWriter =
        DirectMonotonicWriter.getInstance(
            outputMeta, tensorData, numValues, directMonotonicBlockShift);
    for (long offset : tensorDataOffsets) {
      dataOffsetsWriter.add(offset);
    }
    dataOffsetsWriter.finish();
    outputMeta.writeLong(tensorData.getFilePointer() - start);
  }

  /**
   * Reads in the necessary fields stored in the outputMeta to configure {@link
   * DirectMonotonicReader} over TensorDataOffsets
   *
   * @param inputMeta the inputMeta, previously written to via {@link #writeStoredMeta(int,
   *     IndexOutput, IndexOutput, long[])}
   * @return the configuration required to read tensor data offsets
   * @throws IOException thrown when reading data fails
   */
  public static TensorDataOffsetsReaderConfiguration fromStoredMeta(IndexInput inputMeta)
      throws IOException {
    long addressesOffset = inputMeta.readLong();
    int blockShift = inputMeta.readVInt();
    int numValues = inputMeta.readVInt();
    DirectMonotonicReader.Meta meta =
        DirectMonotonicReader.loadMeta(inputMeta, numValues, blockShift);
    long addressesLength = inputMeta.readLong();
    return new TensorDataOffsetsReaderConfiguration(addressesOffset, addressesLength, meta);
  }

  final long addressesOffset, addressesLength;
  final DirectMonotonicReader.Meta meta;

  TensorDataOffsetsReaderConfiguration(
      long addressesOffset, long addressesLength, DirectMonotonicReader.Meta meta) {
    this.addressesOffset = addressesOffset;
    this.addressesLength = addressesLength;
    this.meta = meta;
  }

  /**
   * @param dataIn the IndexInput to read data from
   * @return the DirectMonotonicReader stored values
   * @throws IOException thrown when reading data fails
   */
  public DirectMonotonicReader getDirectMonotonicReader(IndexInput dataIn) throws IOException {
    final RandomAccessInput addressesData =
        dataIn.randomAccessSlice(addressesOffset, addressesLength);
    return DirectMonotonicReader.getInstance(meta, addressesData);
  }
}
