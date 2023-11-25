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
import org.apache.lucene.sandbox.codecs.lucene99.randomaccess.bitpacking.BitPacker;
import org.apache.lucene.sandbox.codecs.lucene99.randomaccess.bitpacking.BitUnpacker;
import org.apache.lucene.util.BytesRef;

interface TermStateCodec {

  /** Get the number of bytes that the metadata per block needs. */
  int getMetadataBytesLength();

  /** Get the maximum span of a record in terms of bytes */
  int getMaximumRecordSizeInBytes();

  /** Get the number of bits per data record within the block, based on the provided metadata. */
  int getNumBitsPerRecord(BytesRef metadataBytes);

  /**
   * Encode the sequence of {@link IntBlockTermState}s with the given bitPacker into a block of
   * bytes.
   *
   * @return the metadata associated with the encoded bytes
   */
  default byte[] encodeBlock(IntBlockTermState[] inputs, BitPacker bitPacker) throws IOException {
    return encodeBlockUpTo(inputs, inputs.length, bitPacker);
  }

  /**
   * Encode the sequence of {@link IntBlockTermState}s up to length, with the given bitPacker into a
   * block of bytes.
   *
   * @return the metadata associated with the encoded bytes
   */
  byte[] encodeBlockUpTo(IntBlockTermState[] inputs, int upto, BitPacker bitPacker)
      throws IOException;

  /**
   * Decode out a {@link IntBlockTermState} with the provided bit-unpacker, metadata byte slice and
   * data byte slice, at the given index within an encoded block.
   *
   * <p>Note: This method expects dataBytes that starts at the start of the block. Also, dataBytes
   * should contain enough bytes (but not necessarily the whole block) to decode at the term state
   * at `index`.
   *
   * @return the decoded term state
   */
  IntBlockTermState decodeWithinBlock(
      BytesRef metadataBytes, BytesRef dataBytes, BitUnpacker bitUnpacker, int index);

  /**
   * Decode out a {@link IntBlockTermState} with the provided bit-unpacker, metadata byte slice and
   * data byte slice, starting at `startBitIndex`.
   *
   * <p>Note: The dataBytes should contain enough bits to decode out the term state. passing more
   * bytes than needed is fine but excessive ones are not used.
   *
   * <p>e.g. we want to decode a term state which contains value x, y and z, that has 18 bits in
   * total. Assume x takes 4 bit, y takes 4 bit and z takes 10 bits.
   *
   * <p>Here is the visualization wh en we decode with startBitIndex=7
   *
   * <pre>
   *     Note: little-endian bit order
   *     [x.......][zyyyyxxx][zzzzzzzz][.......z]
   * </pre>
   */
  IntBlockTermState decodeAt(
      BytesRef metadataBytes, BytesRef dataBytes, BitUnpacker bitUnpacker, int startBitIndex);
}
