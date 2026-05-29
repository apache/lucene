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
package org.apache.lucene.codecs;

import java.io.IOException;
import java.util.Map;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.util.IORunnable;
import org.apache.lucene.util.quantization.HadamardRotation;

/**
 * A KnnVectorsFormat wrapper that applies Hadamard rotation preconditioning to float vectors before
 * passing them to a delegate format. The rotation redistributes variance across dimensions, making
 * per-component distributions more Gaussian — which improves scalar quantization recall on datasets
 * with skewed or uniform component distributions.
 *
 * <p>Because the rotation is orthogonal, dot product, cosine similarity, and Euclidean distance are
 * all preserved. Query-side rotation is handled upstream in {@link
 * org.apache.lucene.search.KnnFloatVectorQuery} via a FieldInfo attribute check, so no per-segment
 * query rotation occurs here.
 *
 * <p>This wrapper is format-agnostic: it can wrap any KnnVectorsFormat (HNSW, flat, etc.) and the
 * delegate remains unaware of the rotation.
 *
 * @lucene.experimental
 */
public class RotationAwareKnnVectorsFormat extends KnnVectorsFormat {

  /** FieldInfo attribute key signaling rotation is enabled. Checked by KnnFloatVectorQuery. */
  public static final String ROTATION_ENABLED_KEY = "Lucene104SQVecRotation";

  private final KnnVectorsFormat delegate;

  /** Wraps the given delegate format with rotation preconditioning. */
  public RotationAwareKnnVectorsFormat(KnnVectorsFormat delegate) {
    super(delegate.getName());
    this.delegate = delegate;
  }

  @Override
  public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new RotatingWriter(delegate.fieldsWriter(state));
  }

  @Override
  public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return new RotatingReader(delegate.fieldsReader(state));
  }

  @Override
  public int getMaxDimensions(String fieldName) {
    return delegate.getMaxDimensions(fieldName);
  }

  @Override
  public String toString() {
    return "RotationAwareKnnVectorsFormat(delegate=" + delegate + ")";
  }

  /**
   * Writer that rotates incoming float vectors before forwarding to the delegate. Byte vectors pass
   * through unchanged.
   */
  private static final class RotatingWriter extends KnnVectorsWriter {

    private final KnnVectorsWriter delegateWriter;

    RotatingWriter(KnnVectorsWriter delegateWriter) {
      this.delegateWriter = delegateWriter;
    }

    @Override
    public KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
      KnnFieldVectorsWriter<?> delegateFieldWriter = delegateWriter.addField(fieldInfo);
      if (fieldInfo.getVectorEncoding() == VectorEncoding.FLOAT32) {
        fieldInfo.putAttribute(ROTATION_ENABLED_KEY, "true");
        @SuppressWarnings("unchecked")
        KnnFieldVectorsWriter<float[]> floatDelegate =
            (KnnFieldVectorsWriter<float[]>) delegateFieldWriter;
        return new RotatingFieldWriter(floatDelegate, fieldInfo.getVectorDimension());
      }
      return delegateFieldWriter;
    }

    @Override
    public IORunnable mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
      // During merge, getMergeInstance() returns the delegate reader directly (no inverse rotation),
      // so vectors are already in rotated space. Delegate directly to avoid double-rotation.
      return delegateWriter.mergeOneField(fieldInfo, mergeState);
    }

    @Override
    public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
      delegateWriter.flush(maxDoc, sortMap);
    }

    @Override
    public void finish() throws IOException {
      delegateWriter.finish();
    }

    @Override
    public void close() throws IOException {
      delegateWriter.close();
    }

    @Override
    public long ramBytesUsed() {
      return delegateWriter.ramBytesUsed();
    }
  }

  /** Field-level writer that rotates each float vector before forwarding. */
  private static final class RotatingFieldWriter extends KnnFieldVectorsWriter<float[]> {

    private final KnnFieldVectorsWriter<float[]> delegate;
    private final HadamardRotation rotation;

    RotatingFieldWriter(KnnFieldVectorsWriter<float[]> delegate, int dimension) {
      this.delegate = delegate;
      this.rotation = HadamardRotation.forDimension(dimension);
    }

    @Override
    public void addValue(int docID, float[] vectorValue) throws IOException {
      float[] rotated = new float[vectorValue.length];
      rotation.rotate(vectorValue, rotated);
      delegate.addValue(docID, rotated);
    }

    @Override
    public float[] copyValue(float[] vectorValue) {
      return delegate.copyValue(vectorValue);
    }

    @Override
    public long ramBytesUsed() {
      return delegate.ramBytesUsed();
    }
  }

  /**
   * Reader that inverse-rotates stored float vectors for external callers, but exposes rotated
   * vectors during merge so the delegate's merge runs entirely in rotated space.
   */
  private static final class RotatingReader extends KnnVectorsReader {

    private final KnnVectorsReader delegateReader;

    RotatingReader(KnnVectorsReader delegateReader) {
      this.delegateReader = delegateReader;
    }

    @Override
    public void checkIntegrity() throws IOException {
      delegateReader.checkIntegrity();
    }

    @Override
    public FloatVectorValues getFloatVectorValues(String field) throws IOException {
      FloatVectorValues values = delegateReader.getFloatVectorValues(field);
      if (values == null) {
        return null;
      }
      HadamardRotation rotation = HadamardRotation.forDimension(values.dimension());
      return new InverseRotatedFloatVectorValues(values, rotation);
    }

    @Override
    public ByteVectorValues getByteVectorValues(String field) throws IOException {
      return delegateReader.getByteVectorValues(field);
    }

    @Override
    public void search(String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
        throws IOException {
      delegateReader.search(field, target, knnCollector, acceptDocs);
    }

    @Override
    public void search(String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
        throws IOException {
      delegateReader.search(field, target, knnCollector, acceptDocs);
    }

    @Override
    public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
      return delegateReader.getOffHeapByteSize(fieldInfo);
    }

    @Override
    public void close() throws IOException {
      delegateReader.close();
    }

    @Override
    public KnnVectorsReader getMergeInstance() throws IOException {
      // Merge operates in rotated space — return the delegate directly so vectors are not
      // inverse-rotated. The merge writer will see already-rotated vectors and write them as-is.
      return delegateReader.getMergeInstance();
    }
  }

  /** Wraps FloatVectorValues to inverse-rotate each vector on access. */
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
      // The target arrives already rotated (from KnnFloatVectorQuery.rewrite()), and the delegate
      // stores rotated vectors — delegate directly for correct rotated-space comparison.
      return delegate.scorer(target);
    }

    @Override
    public VectorScorer rescorer(float[] target) throws IOException {
      return delegate.rescorer(target);
    }
  }
}
