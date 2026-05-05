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
import java.util.List;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;

/**
 * Writer that rotates each incoming float vector via {@link HadamardRotation} and forwards the
 * rotated vector to a delegate {@link FlatVectorsWriter}. Byte vectors are passed through
 * unchanged: rotation only makes sense on float vectors that the downstream format will quantize.
 *
 * <p>The rotation applied to a field is chosen deterministically from {@code (seed, dimension)} so
 * that queries at search time apply the same rotation.
 */
final class RotationPreconditionedVectorsWriter extends FlatVectorsWriter {

  private final FlatVectorsWriter delegate;
  private final long seed;

  RotationPreconditionedVectorsWriter(FlatVectorsWriter delegate, long seed) {
    super(delegate.getFlatVectorScorer());
    this.delegate = delegate;
    this.seed = seed;
  }

  @Override
  public FlatFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
    FlatFieldVectorsWriter<?> inner = delegate.addField(fieldInfo);
    if (fieldInfo.getVectorEncoding() == VectorEncoding.FLOAT32) {
      HadamardRotation rotation =
          HadamardRotation.create(fieldInfo.getVectorDimension(), rotationSeedFor(fieldInfo));
      @SuppressWarnings("unchecked")
      FlatFieldVectorsWriter<float[]> innerFloat = (FlatFieldVectorsWriter<float[]>) inner;
      return new RotatingFieldVectorsWriter(innerFloat, rotation);
    }
    // Byte vectors: nothing to precondition, pass through.
    return inner;
  }

  @Override
  public void mergeOneFlatVectorField(FieldInfo fieldInfo, MergeState mergeState)
      throws IOException {
    // Because our reader's getMergeInstance() exposes the raw (rotated) float vectors, the
    // delegate's own merge logic reads rotated-space vectors across all source segments and
    // writes them back in rotated space — which is exactly what we want. So we can simply
    // delegate.
    delegate.mergeOneFlatVectorField(fieldInfo, mergeState);
  }

  @Override
  public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
    delegate.flush(maxDoc, sortMap);
  }

  @Override
  public void finish() throws IOException {
    delegate.finish();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @Override
  public long ramBytesUsed() {
    return delegate.ramBytesUsed();
  }

  private long rotationSeedFor(FieldInfo fieldInfo) {
    return seed ^ ((long) fieldInfo.getVectorDimension() * 0x9E3779B97F4A7C15L);
  }

  /**
   * A {@link FlatFieldVectorsWriter} that rotates each incoming float vector in-place before
   * delegating to the wrapped writer.
   */
  private static final class RotatingFieldVectorsWriter extends FlatFieldVectorsWriter<float[]> {

    private final FlatFieldVectorsWriter<float[]> delegate;
    private final HadamardRotation rotation;
    private final float[] scratch;

    RotatingFieldVectorsWriter(FlatFieldVectorsWriter<float[]> delegate, HadamardRotation rotation) {
      this.delegate = delegate;
      this.rotation = rotation;
      this.scratch = new float[rotation.dimension()];
    }

    @Override
    public void addValue(int docID, float[] vectorValue) throws IOException {
      // Allocate a fresh rotated vector because the delegate is allowed to retain references to
      // what we hand it. Using one persistent buffer and relying on the delegate to copy is
      // fragile — several delegates call addValue in a loop and keep the reference directly.
      float[] rotated = new float[scratch.length];
      rotation.rotate(vectorValue, rotated, scratch);
      delegate.addValue(docID, rotated);
    }

    @Override
    public float[] copyValue(float[] vectorValue) {
      // copyValue is used by callers that want to stash the input *before* it changes. Rotating
      // here would be wrong because the caller expects a faithful copy of what it passed in. So
      // just forward to the delegate. Any rotation still happens at addValue time.
      return delegate.copyValue(vectorValue);
    }

    @Override
    public List<float[]> getVectors() {
      return delegate.getVectors();
    }

    @Override
    public DocsWithFieldSet getDocsWithFieldSet() {
      return delegate.getDocsWithFieldSet();
    }

    @Override
    public void finish() throws IOException {
      delegate.finish();
    }

    @Override
    public boolean isFinished() {
      return delegate.isFinished();
    }

    @Override
    public long ramBytesUsed() {
      return delegate.ramBytesUsed();
    }
  }
}
