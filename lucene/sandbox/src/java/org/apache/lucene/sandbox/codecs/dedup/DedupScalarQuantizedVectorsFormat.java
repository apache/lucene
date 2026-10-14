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
package org.apache.lucene.sandbox.codecs.dedup;

import java.io.IOException;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues.ScalarEncoding;

/**
 * A scalar quantized version of {@link DedupFlatVectorsFormat} that stores each distinct vector
 * once, in both raw and quantized form.
 *
 * <p>Quantization is <i>data-blind</i>: input vectors are assumed to be evenly distributed, i.e.
 * centered on a zero vector (see {@link DedupQuantizer}). The quantized record of a vector is then
 * a pure function of the raw vector and the {@link DedupQuantizer.Flavor} derived from the field's
 * similarity function — independent of the other vectors in the segment — so quantized records
 * de-duplicate like raw vectors: identical vectors across documents and fields of a group whose
 * similarity functions map to the same flavor share one quantized record, resolved through the same
 * {@code fieldOrdToGroupOrd} translation map. Records follow the {@link
 * org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat} conventions per flavor,
 * and are scored by the stock {@link
 * org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorScorer}. Note the accuracy
 * tradeoff relative to that format, which centers vectors on a per-field centroid before
 * quantizing.
 *
 * <p>Only {@link org.apache.lucene.index.VectorEncoding#FLOAT32} vectors are quantized; BYTE and
 * FLOAT16 vectors are stored raw only, identical to {@link DedupFlatVectorsFormat}.
 *
 * <h2>.vdd (vector de-dup data) file</h2>
 *
 * <p>The raw vectors and per-field data, laid out exactly as the {@link DedupFlatVectorsFormat}
 * {@code .vdd} file.
 *
 * <h2>.vdqd (vector de-dup quantized data) file</h2>
 *
 * <p>One block per FLOAT32 group and {@link DedupQuantizer.Flavor} in use (aligned to {@code
 * Float.BYTES}), holding one quantized record per distinct vector:
 *
 * <ul>
 *   <li><b>[byte]</b> the packed quantized values (layout depends on the {@link ScalarEncoding})
 *   <li><b>[float]</b> the lower optimized interval
 *   <li><b>[float]</b> the upper optimized interval
 *   <li><b>[float]</b> the additional correction (the squared norm for the EUCLIDEAN flavor, zero
 *       otherwise)
 *   <li><b>[int]</b> the sum of the quantized components
 * </ul>
 *
 * <h2>.vdqm (vector de-dup quantized metadata) file</h2>
 *
 * <p>The layout of the {@link DedupFlatVectorsFormat} {@code .vdm} file, with each group
 * additionally recording its flavor blocks:
 *
 * <ul>
 *   <li><b>[vint]</b> the number of flavor blocks ({@code 0} for non-FLOAT32 groups)
 *   <li>per flavor block: <b>[vint]</b> the flavor wire number, <b>[vint]</b> the {@link
 *       ScalarEncoding} wire number of its records, <b>[int64]</b> offset and <b>[int64]</b> length
 *       of its quantized records in the .vdqd file
 * </ul>
 *
 * @lucene.experimental
 */
final class DedupScalarQuantizedVectorsFormat extends FlatVectorsFormat {
  static final String NAME = "DedupScalarQuantizedVectorsFormat";

  private static final String META_CODEC_NAME = "DedupScalarQuantizedVectorsFormatMeta";
  private static final String META_EXTENSION = "vdqm";

  private static final String VECTOR_DATA_CODEC_NAME =
      "DedupScalarQuantizedVectorsFormatVectorData";
  private static final String VECTOR_DATA_EXTENSION = "vdd";

  private static final String QUANTIZED_VECTOR_DATA_CODEC_NAME =
      "DedupScalarQuantizedVectorsFormatQuantizedVectorData";
  private static final String QUANTIZED_VECTOR_DATA_EXTENSION = "vdqd";

  private static final int VERSION_START = 0;
  private static final int VERSION_CURRENT = VERSION_START;

  private static final DedupScalarQuantizedVectorsScorer FLAT_VECTORS_SCORER =
      new DedupScalarQuantizedVectorsScorer();

  private final ScalarEncoding encoding;

  DedupScalarQuantizedVectorsFormat(ScalarEncoding encoding) {
    super(NAME);
    this.encoding = encoding;
  }

  @Override
  public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new DedupFlatVectorsWriter(
        state,
        FLAT_VECTORS_SCORER,
        new DedupQuantizer(encoding),
        META_CODEC_NAME,
        META_EXTENSION,
        VECTOR_DATA_CODEC_NAME,
        VECTOR_DATA_EXTENSION,
        QUANTIZED_VECTOR_DATA_CODEC_NAME,
        QUANTIZED_VECTOR_DATA_EXTENSION,
        VERSION_CURRENT);
  }

  @Override
  public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return new DedupScalarQuantizedVectorsReader(
        state,
        FLAT_VECTORS_SCORER,
        META_CODEC_NAME,
        META_EXTENSION,
        VECTOR_DATA_CODEC_NAME,
        VECTOR_DATA_EXTENSION,
        QUANTIZED_VECTOR_DATA_CODEC_NAME,
        QUANTIZED_VECTOR_DATA_EXTENSION,
        VERSION_START,
        VERSION_CURRENT);
  }
}
