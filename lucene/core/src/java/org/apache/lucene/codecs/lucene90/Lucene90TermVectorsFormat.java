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
package org.apache.lucene.codecs.lucene90;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.lucene90.compressing.FieldsIndexWriter;
import org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingTermVectorsFormat;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.packed.BlockPackedWriter;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Lucene 9.0 {@link TermVectorsFormat term vectors format}.
 *
 * <p>Very similarly to {@link Lucene90StoredFieldsFormat}, this format is based on compressed
 * chunks of data, with document-level granularity so that a document can never span across distinct
 * chunks. Moreover, data is made as compact as possible:
 *
 * <ul>
 *   <li>textual data is compressed using the very light, <a
 *       href="http://code.google.com/p/lz4/">LZ4</a> compression algorithm,
 *   <li>binary data is written using fixed-size blocks of {@link PackedInts packed ints}.
 * </ul>
 *
 * <p>Term vectors are stored using two files
 *
 * <ul>
 *   <li>a data file where terms, frequencies, positions, offsets and payloads are stored,
 *   <li>an index file, loaded into memory, used to locate specific documents in the data file.
 * </ul>
 *
 * Looking up term vectors for any document requires at most 1 disk seek.
 *
 * <p><b>File formats</b>
 *
 * <ol>
 *   <li><a id="vector_meta"></a>
 *       <p>A vector metadata file (extension <code>.tvm</code>).
 *       <ul>
 *         <li>VectorMeta (.tvm) --&gt; &lt;Header&gt;, PackedIntsVersion, ChunkSize,
 *             ChunkIndexMetadata, ChunkCount, DirtyChunkCount, DirtyDocsCount, Footer
 *         <li>Header --&gt; {@link CodecUtil#writeIndexHeader IndexHeader}
 *         <li>PackedIntsVersion, ChunkSize --&gt; {@link DataOutput#writeVInt VInt}
 *         <li>ChunkCount, DirtyChunkCount, DirtyDocsCount --&gt; {@link DataOutput#writeVLong
 *             VLong}
 *         <li>ChunkIndexMetadata --&gt; {@link FieldsIndexWriter}
 *         <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}
 *       </ul>
 *       <p>Notes:
 *       <ul>
 *         <li>PackedIntsVersion is {@link PackedInts#VERSION_CURRENT}.
 *         <li>ChunkSize is the number of bytes of terms to accumulate before flushing.
 *         <li>ChunkCount is not known in advance and is the number of chunks necessary to store all
 *             document of the segment.
 *         <li>DirtyChunkCount is the number of prematurely flushed chunks in the .tvd file.
 *       </ul>
 *   <li><a id="vector_data"></a>
 *       <p>A vector data file (extension <code>.tvd</code>). This file stores terms, frequencies,
 *       positions, offsets and payloads for every document. Upon writing a new segment, it
 *       accumulates data into memory until the buffer used to store terms and payloads grows beyond
 *       4KB. Then it flushes all metadata, terms and positions to disk using <a
 *       href="http://code.google.com/p/lz4/">LZ4</a> compression for terms and payloads and {@link
 *       BlockPackedWriter blocks of packed ints} for positions.
 *       <p>Here is a more detailed description of the field data file format:
 *       <ul>
 *         <li>VectorData (.tvd) --&gt; &lt;Header&gt;, &lt;Chunk&gt;<sup>ChunkCount</sup>, Footer
 *         <li>Header --&gt; {@link CodecUtil#writeIndexHeader IndexHeader}
 *         <li>Chunk --&gt; DocBase, ChunkDocs, &lt; NumFields &gt;, &lt; FieldNums &gt;, &lt;
 *             FieldNumOffs &gt;, &lt; Flags &gt;, &lt; NumTerms &gt;, &lt; TermLengths &gt;, &lt;
 *             TermFreqs &gt;, &lt; Positions &gt;, &lt; StartOffsets &gt;, &lt; Lengths &gt;, &lt;
 *             PayloadLengths &gt;, &lt; TermAndPayloads &gt;
 *         <li>NumFields --&gt; DocNumFields<sup>ChunkDocs</sup>
 *         <li>FieldNums --&gt; FieldNumDelta<sup>TotalDistincFields</sup>
 *         <li>Flags --&gt; Bit &lt; FieldFlags &gt;
 *         <li>FieldFlags --&gt; if Bit==1: Flag<sup>TotalDistinctFields</sup> else
 *             Flag<sup>TotalFields</sup>
 *         <li>NumTerms --&gt; FieldNumTerms<sup>TotalFields</sup>
 *         <li>TermLengths --&gt; PrefixLength<sup>TotalTerms</sup>
 *             SuffixLength<sup>TotalTerms</sup>
 *         <li>TermFreqs --&gt; TermFreqMinus1<sup>TotalTerms</sup>
 *         <li>Positions --&gt; PositionDelta<sup>TotalPositions</sup>
 *         <li>StartOffsets --&gt; (AvgCharsPerTerm<sup>TotalDistinctFields</sup>)
 *             StartOffsetDelta<sup>TotalOffsets</sup>
 *         <li>Lengths --&gt; LengthMinusTermLength<sup>TotalOffsets</sup>
 *         <li>PayloadLengths --&gt; PayloadLength<sup>TotalPayloads</sup>
 *         <li>TermAndPayloads --&gt; LZ4-compressed representation of &lt; FieldTermsAndPayLoads
 *             &gt;<sup>TotalFields</sup>
 *         <li>FieldTermsAndPayLoads --&gt; Terms (Payloads)
 *         <li>DocBase, ChunkDocs, DocNumFields (with ChunkDocs==1) --&gt; {@link
 *             DataOutput#writeVInt VInt}
 *         <li>AvgCharsPerTerm --&gt; {@link DataOutput#writeInt Int}
 *         <li>DocNumFields (with ChunkDocs&gt;=1), FieldNumOffs --&gt; {@link PackedInts} array
 *         <li>FieldNumTerms, PrefixLength, SuffixLength, TermFreqMinus1, PositionDelta,
 *             StartOffsetDelta, LengthMinusTermLength, PayloadLength --&gt; {@link
 *             BlockPackedWriter blocks of 64 packed ints}
 *         <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}
 *       </ul>
 *       <p>Notes:
 *       <ul>
 *         <li>DocBase is the ID of the first doc of the chunk.
 *         <li>ChunkDocs is the number of documents in the chunk.
 *         <li>DocNumFields is the number of fields for each doc.
 *         <li>FieldNums is a delta-encoded list of the sorted unique field numbers present in the
 *             chunk.
 *         <li>FieldNumOffs is the array of FieldNumOff; array size is the total number of fields in
 *             the chunk.
 *         <li>FieldNumOff is the offset of the field number in FieldNums.
 *         <li>TotalFields is the total number of fields (sum of the values of NumFields).
 *         <li>Bit in Flags is a single bit which when true means that fields have the same options
 *             for every document in the chunk.
 *         <li>Flag: a 3-bits int where:
 *             <ul>
 *               <li>the first bit means that the field has positions
 *               <li>the second bit means that the field has offsets
 *               <li>the third bit means that the field has payloads
 *             </ul>
 *         <li>FieldNumTerms is the number of terms for each field.
 *         <li>TotalTerms is the total number of terms (sum of NumTerms).
 *         <li>PrefixLength is 0 for the first term of a field, the common prefix with the previous
 *             term otherwise.
 *         <li>SuffixLength is the length of the term minus PrefixLength for every term using.
 *         <li>TermFreqMinus1 is (frequency - 1) for each term.
 *         <li>TotalPositions is the sum of frequencies of terms of all fields that have positions.
 *         <li>PositionDelta is the absolute position for the first position of a term, and the
 *             difference with the previous positions for following positions.
 *         <li>TotalOffsets is the sum of frequencies of terms of all fields that have offsets.
 *         <li>AvgCharsPerTerm is the average number of chars per term, encoded as a float on 4
 *             bytes. They are not present if no field has both positions and offsets enabled.
 *         <li>StartOffsetDelta is the (startOffset - previousStartOffset - AvgCharsPerTerm *
 *             PositionDelta). previousStartOffset is 0 for the first offset and AvgCharsPerTerm is
 *             0 if the field has no positions.
 *         <li>LengthMinusTermLength is (endOffset - startOffset - termLength).
 *         <li>TotalPayloads is the sum of frequencies of terms of all fields that have payloads.
 *         <li>PayloadLength is the payload length encoded.
 *         <li>Terms is term bytes.
 *         <li>Payloads is payload bytes (if the field has payloads).
 *       </ul>
 *   <li><a id="vector_index"></a>
 *       <p>An index file (extension <code>.tvx</code>).
 *       <ul>
 *         <li>VectorIndex (.tvx) --&gt; &lt;Header&gt;, &lt;ChunkIndex&gt;, Footer
 *         <li>Header --&gt; {@link CodecUtil#writeIndexHeader IndexHeader}
 *         <li>ChunkIndex --&gt; {@link FieldsIndexWriter}
 *         <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}
 *       </ul>
 * </ol>
 *
 * @lucene.experimental
 */
public final class Lucene90TermVectorsFormat extends Lucene90CompressingTermVectorsFormat {

  /** Sole constructor. */
  public Lucene90TermVectorsFormat() {
    super("Lucene90TermVectorsData", "", CompressionMode.FAST, 1 << 12, 128, 10);
  }
}
