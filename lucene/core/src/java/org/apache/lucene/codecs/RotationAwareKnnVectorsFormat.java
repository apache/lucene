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
import org.apache.lucene.index.FieldInfos;
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

  /** FieldInfo attribute key signaling rotation is enabled. */
  public static final String ROTATION_ENABLED_KEY = "Lucene104SQVecRotation";

  /**
   * FieldInfo attribute key recording the delegate format name (resolvable via {@link
   * KnnVectorsFormat#forName(String)}) used at write time. The reader uses it to recreate the same
   * delegate at read time, so an index written with a non-default delegate is still readable when
   * the SPI no-arg constructor is used.
   */
  public static final String DELEGATE_FORMAT_KEY = "RotationAwareDelegateFormat";

  /**
   * Delegate used at write time and as the default for fields whose persisted delegate name is
   * unknown at read time. Null when this format was instantiated via the no-arg SPI constructor —
   * in that case writes are not supported, and reads resolve the delegate per FieldInfo attribute
   * via {@link KnnVectorsFormat#forName(String)}.
   */
  private final KnnVectorsFormat delegate;

  /** No-arg constructor for SPI registration. Read-only — writing requires an explicit delegate. */
  public RotationAwareKnnVectorsFormat() {
    super("RotationAwareKnnVectorsFormat");
    this.delegate = null;
  }

  /** Wraps the given delegate format with rotation preconditioning. */
  public RotationAwareKnnVectorsFormat(KnnVectorsFormat delegate) {
    super("RotationAwareKnnVectorsFormat");
    this.delegate = delegate;
  }

  @Override
  public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    if (delegate == null) {
      throw new UnsupportedOperationException(
          "RotationAwareKnnVectorsFormat was constructed via the no-arg SPI constructor and "
              + "cannot write; use the (KnnVectorsFormat delegate) constructor for indexing.");
    }
    return new RotatingWriter(delegate.fieldsWriter(state), delegate.getName());
  }

  @Override
  public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    // Resolve the actual delegate per the FieldInfo attribute persisted at write time. If a
    // user-supplied delegate matches that name, prefer it (so user-tuned constructor args
    // are honoured); otherwise resolve via SPI. Falls back to the user-supplied delegate
    // when no rotation-enabled field is present (e.g. byte-only segment).
    KnnVectorsFormat actualDelegate = null;
    for (FieldInfo fi : state.fieldInfos) {
      String name = fi.getAttribute(DELEGATE_FORMAT_KEY);
      if (name != null) {
        actualDelegate =
            (delegate != null && name.equals(delegate.getName()))
                ? delegate
                : KnnVectorsFormat.forName(name);
        break;
      }
    }
    if (actualDelegate == null) {
      actualDelegate = delegate;
    }
    if (actualDelegate == null) {
      throw new IllegalStateException(
          "RotationAwareKnnVectorsFormat: no delegate format could be resolved for segment "
              + state.segmentInfo.name
              + " (no field carries the "
              + DELEGATE_FORMAT_KEY
              + " attribute and the no-arg "
              + "SPI constructor was used).");
    }
    return new RotatingReader(actualDelegate.fieldsReader(state), state.fieldInfos);
  }

  @Override
  public int getMaxDimensions(String fieldName) {
    if (delegate == null) {
      // No-arg/SPI path is read-only; getMaxDimensions is consulted at write time, so this
      // shouldn't be called. Be permissive rather than crash if it is.
      return KnnVectorsFormat.DEFAULT_MAX_DIMENSIONS;
    }
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
    private final String delegateName;

    RotatingWriter(KnnVectorsWriter delegateWriter, String delegateName) {
      this.delegateWriter = delegateWriter;
      this.delegateName = delegateName;
    }

    @Override
    public KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
      KnnFieldVectorsWriter<?> delegateFieldWriter = delegateWriter.addField(fieldInfo);
      // Persist the delegate format name so RotatingReader can recreate the same delegate at
      // read time even if the SPI no-arg constructor would create a different default. We do
      // this for every field (both float and byte) so the reader can resolve the delegate
      // even on byte-only segments.
      fieldInfo.putAttribute(DELEGATE_FORMAT_KEY, delegateName);
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
      // During merge, getMergeInstance() returns the delegate reader directly (no inverse
      // rotation),
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
   * Reader that inverse-rotates stored float vectors for external callers, rotates query vectors
   * before search/scoring, and exposes raw rotated vectors during merge so the delegate's merge
   * runs entirely in rotated space.
   */
  private static final class RotatingReader extends KnnVectorsReader {

    private final KnnVectorsReader delegateReader;
    private final FieldInfos fieldInfos;

    RotatingReader(KnnVectorsReader delegateReader, FieldInfos fieldInfos) {
      this.delegateReader = delegateReader;
      this.fieldInfos = fieldInfos;
    }

    @Override
    public KnnVectorsReader unwrapReaderForField(String field) {
      return delegateReader.unwrapReaderForField(field);
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
      if (isRotationEnabled(field)) {
        HadamardRotation rotation = HadamardRotation.forDimension(values.dimension());
        return new InverseRotatedFloatVectorValues(values, rotation);
      }
      return values;
    }

    @Override
    public ByteVectorValues getByteVectorValues(String field) throws IOException {
      return delegateReader.getByteVectorValues(field);
    }

    @Override
    public void search(
        String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
        throws IOException {
      float[] searchTarget = maybeRotateTarget(field, target);
      delegateReader.search(field, searchTarget, knnCollector, acceptDocs);
    }

    @Override
    public void search(
        String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
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

    private boolean isRotationEnabled(String field) {
      FieldInfo info = fieldInfos.fieldInfo(field);
      return info != null && "true".equals(info.getAttribute(ROTATION_ENABLED_KEY));
    }

    private float[] maybeRotateTarget(String field, float[] target) {
      if (target != null && isRotationEnabled(field)) {
        float[] rotated = new float[target.length];
        HadamardRotation.forDimension(target.length).rotate(target, rotated);
        return rotated;
      }
      return target;
    }
  }

  /** Wraps FloatVectorValues to inverse-rotate each vector on access. */
  public static final class InverseRotatedFloatVectorValues extends FloatVectorValues {

    private final FloatVectorValues delegate;
    private final HadamardRotation rotation;
    private final float[] out;
    private final float[] scratch;

    /** Wraps the given delegate values with inverse-rotation using the provided rotation. */
    public InverseRotatedFloatVectorValues(FloatVectorValues delegate, HadamardRotation rotation) {
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
      // Target arrives in original (unrotated) space; rotate before delegating to scorer
      // that operates in rotated space.
      float[] rotated = new float[target.length];
      rotation.rotate(target, rotated);
      return delegate.scorer(rotated);
    }

    @Override
    public VectorScorer rescorer(float[] target) throws IOException {
      // Target arrives in original (unrotated) space; rotate before delegating to rescorer
      // that operates in rotated space.
      float[] rotated = new float[target.length];
      rotation.rotate(target, rotated);
      return delegate.rescorer(rotated);
    }
  }
}
