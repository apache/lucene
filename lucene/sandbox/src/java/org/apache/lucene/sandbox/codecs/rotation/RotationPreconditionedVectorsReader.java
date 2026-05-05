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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorer;

/**
 * Reader-side half of the rotation preconditioner. At search time, float query vectors are rotated
 * once with the same rotation that was applied at index time (derived from {@code (seed,
 * dimension)}). Because the rotation is orthogonal, the dot product / cosine / Euclidean distance
 * between the rotated query and a rotated document vector equals the distance between the original
 * query and original document — so the downstream quantized scorer returns meaningful scores
 * without any changes to its math.
 *
 * <p>For user-facing {@link #getFloatVectorValues(String)} calls, the reader returns a view that
 * inverse-rotates values on access so callers (rescoring queries, {@code CheckIndex}, etc.) see
 * the original vectors they indexed. For merge reads (via {@link #getMergeInstance()}) the raw
 * rotated values are exposed directly so that the delegate's merge logic preserves rotation end
 * to end.
 */
final class RotationPreconditionedVectorsReader extends FlatVectorsReader {

  private final FlatVectorsReader delegate;
  private final long seed;
  /** When true, this instance exposes raw (rotated) vectors to callers — intended for merge. */
  private final boolean rawForMerge;

  private final Map<Integer, HadamardRotation> rotationCache;

  RotationPreconditionedVectorsReader(FlatVectorsReader delegate, long seed) {
    this(delegate, seed, false, new ConcurrentHashMap<>());
  }

  private RotationPreconditionedVectorsReader(
      FlatVectorsReader delegate,
      long seed,
      boolean rawForMerge,
      Map<Integer, HadamardRotation> cache) {
    super(delegate.getFlatVectorScorer());
    this.delegate = delegate;
    this.seed = seed;
    this.rawForMerge = rawForMerge;
    this.rotationCache = cache;
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(String field, float[] target) throws IOException {
    if (target == null) {
      return delegate.getRandomVectorScorer(field, target);
    }
    HadamardRotation rotation = rotationFor(target.length);
    float[] rotated = new float[target.length];
    rotation.rotate(target, rotated);
    return delegate.getRandomVectorScorer(field, rotated);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(String field, byte[] target) throws IOException {
    return delegate.getRandomVectorScorer(field, target);
  }

  @Override
  public FloatVectorValues getFloatVectorValues(String field) throws IOException {
    FloatVectorValues inner = delegate.getFloatVectorValues(field);
    if (inner == null || rawForMerge) {
      // Merge instance: hand through the raw rotated values directly. The delegate's merge logic
      // will re-rotate nothing (values are already in rotated space) and quantize them correctly.
      return inner;
    }
    return new InverseRotatedFloatVectorValues(inner, rotationFor(inner.dimension()));
  }

  @Override
  public ByteVectorValues getByteVectorValues(String field) throws IOException {
    return delegate.getByteVectorValues(field);
  }

  @Override
  public void checkIntegrity() throws IOException {
    delegate.checkIntegrity();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @Override
  public long ramBytesUsed() {
    return delegate.ramBytesUsed();
  }

  @Override
  public FlatVectorsReader getMergeInstance() throws IOException {
    FlatVectorsReader inner = delegate.getMergeInstance();
    // Return a view that exposes raw rotated values directly to the merger. The delegate's merge
    // logic operates on the rotated space and will preserve it end to end.
    return new RotationPreconditionedVectorsReader(inner, seed, true, rotationCache);
  }

  @Override
  public KnnVectorsReader unwrapReaderForField(String field) {
    return delegate.unwrapReaderForField(field);
  }

  @Override
  public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
    return delegate.getOffHeapByteSize(fieldInfo);
  }

  private HadamardRotation rotationFor(int dim) {
    return rotationCache.computeIfAbsent(
        dim, d -> HadamardRotation.create(d, seed ^ ((long) d * 0x9E3779B97F4A7C15L)));
  }

  /**
   * A {@link FloatVectorValues} view that inverse-rotates the underlying (rotated) stored vectors
   * on demand so that callers see the original un-rotated values. The scoring path on this wrapper
   * re-rotates the query internally and scores against the (still-rotated) delegate for efficiency.
   */
  private static final class InverseRotatedFloatVectorValues extends FloatVectorValues {

    private final FloatVectorValues delegate;
    private final HadamardRotation rotation;
    private final float[] out;
    private final float[] scratch;

    InverseRotatedFloatVectorValues(FloatVectorValues delegate, HadamardRotation rotation) {
      this.delegate = delegate;
      this.rotation = rotation;
      this.out = new float[rotation.dimension()];
      this.scratch = new float[rotation.dimension()];
    }

    @Override
    public float[] vectorValue(int ord) throws IOException {
      float[] rotated = delegate.vectorValue(ord);
      rotation.inverseRotate(rotated, out, scratch);
      return out;
    }

    @Override
    public int dimension() {
      return delegate.dimension();
    }

    @Override
    public int size() {
      return delegate.size();
    }

    @Override
    public FloatVectorValues copy() throws IOException {
      return new InverseRotatedFloatVectorValues(delegate.copy(), rotation);
    }

    @Override
    public KnnVectorValues.DocIndexIterator iterator() {
      return delegate.iterator();
    }

    @Override
    public int ordToDoc(int ord) {
      return delegate.ordToDoc(ord);
    }

    @Override
    public VectorScorer scorer(float[] target) throws IOException {
      float[] rotated = new float[target.length];
      rotation.rotate(target, rotated);
      return delegate.scorer(rotated);
    }

    @Override
    public VectorScorer rescorer(float[] target) throws IOException {
      float[] rotated = new float[target.length];
      rotation.rotate(target, rotated);
      return delegate.rescorer(rotated);
    }
  }
}
