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
package org.apache.lucene.sandbox.codecs.rotation;

import java.io.IOException;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/**
 * A {@link FlatVectorsFormat} that applies a data-oblivious, randomized Hadamard rotation
 * (preconditioner) to every vector before handing it to a delegate format, and applies the inverse
 * / matching rotation to query vectors at search time. Because the rotation is orthogonal it
 * preserves dot product, cosine, and Euclidean distance — so the delegate sees exactly the same
 * similarity structure as the original data, but with component distributions that look much more
 * Gaussian. This makes scalar quantizers like
 * {@link org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat OSQ} substantially
 * more robust on datasets whose raw components are skewed or uniformly distributed (e.g. image
 * pixel values, histogram features, non-transformer embeddings).
 *
 * <p>Typical usage (compose with OSQ):
 *
 * <pre>{@code
 * FlatVectorsFormat inner = new Lucene104ScalarQuantizedVectorsFormat();
 * FlatVectorsFormat preconditioned = new RotationPreconditionedVectorsFormat(inner);
 * }</pre>
 *
 * <p>Key properties:
 *
 * <ul>
 *   <li><b>Orthogonal</b>: distances and inner products are unchanged.
 *   <li><b>Deterministic</b>: the rotation is derived from a configurable seed plus the field's
 *       vector dimension. All segments use the same rotation, which enables byte-copy merges in the
 *       underlying format.
 *   <li><b>O(d log d)</b> per vector via a block-diagonal Fast Walsh-Hadamard Transform. For
 *       non-power-of-2 dimensions, the vector is split into power-of-2 blocks (e.g. 768 = 512+256).
 *   <li><b>No training / no first pass</b>: the transform is data-oblivious, so no sample of
 *       vectors is needed up front.
 * </ul>
 *
 * <p>Based on the approach discussed in Apache Lucene PR #15903 and in Elastic's April 2026 blog on
 * BBQ preconditioning, which showed 41-74% recall improvements on GIST / SIFT / Fashion-MNIST with
 * ~2-4% query overhead.
 *
 * @lucene.experimental
 */
public final class RotationPreconditionedVectorsFormat extends FlatVectorsFormat {

  /** Name of this format. */
  public static final String NAME = "RotationPreconditionedVectorsFormat";

  /** Default seed used to derive the rotation. Chosen arbitrarily but fixed for reproducibility. */
  public static final long DEFAULT_SEED = 0x5eed_face_cafe_babeL;

  private final FlatVectorsFormat delegate;
  private final long seed;

  /**
   * No-arg constructor required for SPI / ServiceLoader-based lookup. Defaults to wrapping a
   * {@link org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat} with the
   * {@link #DEFAULT_SEED default seed}.
   */
  public RotationPreconditionedVectorsFormat() {
    this(
        new org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat(),
        DEFAULT_SEED);
  }

  /** Wrap the given delegate with the {@link #DEFAULT_SEED default} rotation seed. */
  public RotationPreconditionedVectorsFormat(FlatVectorsFormat delegate) {
    this(delegate, DEFAULT_SEED);
  }

  /**
   * Wrap the given delegate with the specified rotation seed. The seed is mixed with the vector
   * dimension to pick a rotation per field dimension.
   */
  public RotationPreconditionedVectorsFormat(FlatVectorsFormat delegate, long seed) {
    super(NAME);
    if (delegate == null) {
      throw new IllegalArgumentException("delegate FlatVectorsFormat must not be null");
    }
    this.delegate = delegate;
    this.seed = seed;
  }

  @Override
  public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new RotationPreconditionedVectorsWriter(delegate.fieldsWriter(state), seed);
  }

  @Override
  public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return new RotationPreconditionedVectorsReader(delegate.fieldsReader(state), seed);
  }

  @Override
  public int getMaxDimensions(String fieldName) {
    return delegate.getMaxDimensions(fieldName);
  }

  @Override
  public String toString() {
    return NAME + "(seed=" + Long.toHexString(seed) + ", delegate=" + delegate + ")";
  }
}
