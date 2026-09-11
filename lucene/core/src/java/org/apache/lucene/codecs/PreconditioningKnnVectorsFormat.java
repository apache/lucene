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
import java.util.Objects;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Float16VectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.util.IORunnable;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.quantization.HadamardRotation;

/**
 * A {@link KnnVectorsFormat} wrapper that applies a deterministic Hadamard rotation to float32
 * vectors before passing them to a delegate format, improving scalar quantization recall on
 * datasets with skewed component distributions. The rotation is orthogonal, so norms, dot products
 * and distances are preserved exactly and the delegate is unaware of it.
 *
 * <p>Wrap a format with {@link #rotating(KnnVectorsFormat)} and return it from your codec's {@code
 * getKnnVectorsFormatForField}.
 *
 * @lucene.experimental
 */
public class PreconditioningKnnVectorsFormat extends KnnVectorsFormat {

  /** Format name, as returned by {@link #getName()} and registered for SPI lookup. */
  public static final String NAME = "PreconditioningKnnVectorsFormat";

  /**
   * {@link FieldInfo} attribute naming the delegate format, so a reader created through the no-arg
   * SPI constructor can re-open it via {@link KnnVectorsFormat#forName(String)}. Its presence also
   * marks the field as rotated.
   */
  public static final String DELEGATE_FORMAT_KEY = NAME + "_DelegateFormat";

  /**
   * {@link FieldInfo} attribute recording the rotation seed as a decimal {@code long}, so the exact
   * rotation is reconstructible from the index alone.
   */
  public static final String ROTATION_SEED_KEY = NAME + "_Seed";

  /**
   * The wrapped format, or {@code null} for a read-only instance from the no-arg SPI constructor.
   */
  private final KnnVectorsFormat delegate;

  /**
   * For SPI registration only; the resulting instance is read-only. Use {@link
   * #rotating(KnnVectorsFormat)} for indexing.
   */
  public PreconditioningKnnVectorsFormat() {
    super(NAME);
    this.delegate = null;
  }

  /**
   * Wraps {@code format} so its float32 vectors and queries are rotated. Sole entry point for
   * enabling rotation.
   *
   * @param format any concrete {@link KnnVectorsFormat} to wrap
   * @throws NullPointerException if {@code format} is null
   * @throws IllegalArgumentException if {@code format} is already rotating, or is a {@code
   *     PerFieldKnnVectorsFormat} (nesting either yields a double-rotated or corrupt index)
   */
  public static PreconditioningKnnVectorsFormat rotating(KnnVectorsFormat format) {
    Objects.requireNonNull(format, "format must not be null");
    if (format instanceof PreconditioningKnnVectorsFormat) {
      throw new IllegalArgumentException(
          "Already rotating; cannot double-wrap. A field is either rotated or not.");
    }
    if (format instanceof PerFieldKnnVectorsFormat) {
      throw new IllegalArgumentException(
          "Cannot wrap "
              + PerFieldKnnVectorsFormat.class.getSimpleName()
              + " with rotation. Instead, return PreconditioningKnnVectorsFormat.rotating(yourFormat) "
              + "from getKnnVectorsFormatForField for the fields you want rotated.");
    }
    return new PreconditioningKnnVectorsFormat(format);
  }

  /**
   * @param delegate already validated by {@link #rotating(KnnVectorsFormat)}
   */
  PreconditioningKnnVectorsFormat(KnnVectorsFormat delegate) {
    super(NAME);
    this.delegate = delegate;
  }

  @Override
  public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new PreconditioningWriter(
        requireDelegateForWrite().fieldsWriter(state), delegate.getName());
  }

  @Override
  public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return new PreconditioningReader(state, delegate);
  }

  @Override
  public int getMaxDimensions(String fieldName) {
    return requireDelegateForWrite().getMaxDimensions(fieldName);
  }

  private KnnVectorsFormat requireDelegateForWrite() {
    if (delegate == null) {
      throw new IllegalStateException(
          NAME
              + " was created by the no-arg SPI constructor and is read-only; use "
              + "PreconditioningKnnVectorsFormat.rotating(KnnVectorsFormat) for indexing");
    }
    return delegate;
  }

  @Override
  public String toString() {
    return NAME + "(delegate=" + delegate + ")";
  }

  private static final class PreconditioningWriter extends KnnVectorsWriter {

    private final KnnVectorsWriter delegateWriter;
    private final String delegateName;

    PreconditioningWriter(KnnVectorsWriter delegateWriter, String delegateName) {
      this.delegateWriter = delegateWriter;
      this.delegateName = delegateName;
    }

    @Override
    public KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
      if (fieldInfo.getVectorEncoding() != VectorEncoding.FLOAT32) {
        // Only float32 can be rotated losslessly; byte/float16 support is future work.
        throw new IllegalArgumentException(
            NAME
                + " rotates "
                + VectorEncoding.FLOAT32
                + " vectors only, but field \""
                + fieldInfo.name
                + "\" uses "
                + fieldInfo.getVectorEncoding()
                + ". Route this field to a different format instead of wrapping it.");
      }
      fieldInfo.putAttribute(DELEGATE_FORMAT_KEY, delegateName);
      long seed = HadamardRotation.seedForDimension(fieldInfo.getVectorDimension());
      fieldInfo.putAttribute(ROTATION_SEED_KEY, Long.toString(seed));

      @SuppressWarnings("unchecked")
      KnnFieldVectorsWriter<float[]> floatWriter =
          (KnnFieldVectorsWriter<float[]>) delegateWriter.addField(fieldInfo);
      return new PreconditioningFieldWriter(
          floatWriter, HadamardRotation.forDimension(fieldInfo.getVectorDimension()));
    }

    /**
     * Straight pass-through: every source is already in the same rotated basis, enforced by {@link
     * FieldInfos.Builder#add} when the IndexWriter opens.
     */
    @Override
    public IORunnable mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
      // Refresh attribute: Builder merges attrs forward from sources, so stale names persist
      // without this.
      fieldInfo.putAttribute(DELEGATE_FORMAT_KEY, delegateName);
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

  /** Rotates each incoming float vector, then hands it to the delegate's field writer. */
  private static final class PreconditioningFieldWriter extends KnnFieldVectorsWriter<float[]> {

    private final KnnFieldVectorsWriter<float[]> delegate;
    private final HadamardRotation rotation;
    private final float[] scratch;

    PreconditioningFieldWriter(KnnFieldVectorsWriter<float[]> delegate, HadamardRotation rotation) {
      this.delegate = delegate;
      this.rotation = rotation;
      this.scratch = new float[rotation.dimension()];
    }

    @Override
    public void addValue(int docID, float[] vectorValue) throws IOException {
      // Fresh array: delegate may retain it.
      float[] rotated = new float[vectorValue.length];
      rotation.rotate(vectorValue, rotated, scratch);
      delegate.addValue(docID, rotated);
    }

    @Override
    public float[] copyValue(float[] vectorValue) {
      // Unreachable: delegate copies via its own copyValue inside addValue.
      throw new UnsupportedOperationException(
          "copyValue is not used by " + NAME + "; the delegate copies values itself");
    }

    @Override
    public long ramBytesUsed() {
      return delegate.ramBytesUsed();
    }
  }

  /**
   * Rotates queries before scoring and inverse-rotates stored vectors for callers that iterate
   * them. The delegate is opened eagerly into a final field, so reads are lock-free.
   */
  private static final class PreconditioningReader extends KnnVectorsReader {

    private final SegmentReadState state;
    private final FieldInfos fieldInfos;

    // Opened eagerly in the constructor; final for lock-free reads on the search path.
    private final KnnVectorsReader delegateReader;

    PreconditioningReader(SegmentReadState state, KnnVectorsFormat configuredDelegate)
        throws IOException {
      this.state = state;
      this.fieldInfos = state.fieldInfos;
      // Resolve delegate eagerly (matches PerFieldKnnVectorsFormat's lock-free pattern).
      KnnVectorsFormat resolved = resolveDelegateFormat(configuredDelegate);
      if (resolved != null) {
        try {
          this.delegateReader = resolved.fieldsReader(state);
        } catch (Throwable t) {
          throw IOUtils.rethrowAlways(t);
        }
      } else {
        this.delegateReader = null;
      }
    }

    /** Returns {@code true} if this reader is responsible for {@code fieldInfo}. */
    private boolean ownsField(FieldInfo fieldInfo) {
      if (fieldInfo.getVectorDimension() == 0) {
        return false;
      }
      if (state.segmentSuffix.isEmpty()) {
        return true;
      }
      if (NAME.equals(fieldInfo.getAttribute(PerFieldKnnVectorsFormat.PER_FIELD_FORMAT_KEY))
          == false) {
        return false;
      }
      String perFieldSuffix = fieldInfo.getAttribute(PerFieldKnnVectorsFormat.PER_FIELD_SUFFIX_KEY);
      return perFieldSuffix != null && state.segmentSuffix.endsWith(NAME + "_" + perFieldSuffix);
    }

    /** Returns the delegate reader. Lock-free: the field is final. */
    private KnnVectorsReader delegateReader() {
      assert delegateReader != null;
      return delegateReader;
    }

    private KnnVectorsFormat resolveDelegateFormat(KnnVectorsFormat configuredDelegate) {
      if (configuredDelegate != null) {
        return configuredDelegate;
      }
      for (FieldInfo fieldInfo : fieldInfos) {
        if (ownsField(fieldInfo)) {
          String name = fieldInfo.getAttribute(DELEGATE_FORMAT_KEY);
          if (name != null) {
            return KnnVectorsFormat.forName(name);
          }
        }
      }
      // No fields carry the delegate attribute; null signals the constructor to skip opening.
      return null;
    }

    /**
     * Returns the rotation applied to {@code field}, rebuilt from the persisted seed so it matches
     * index time exactly.
     */
    private HadamardRotation rotationFor(String field) {
      FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
      if (fieldInfo == null) {
        throw new IllegalArgumentException(
            "field \"" + field + "\" does not exist in segment " + state.segmentInfo.name);
      }
      // Use persisted seed; fall back to dimension-derived if absent (legacy).
      int dimension = fieldInfo.getVectorDimension();
      String seedStr = fieldInfo.getAttribute(ROTATION_SEED_KEY);
      if (seedStr == null) {
        // Legacy fallback.
        return HadamardRotation.forDimension(dimension);
      }
      long persistedSeed = Long.parseLong(seedStr);
      return persistedSeed == HadamardRotation.seedForDimension(dimension)
          ? HadamardRotation.forDimension(dimension)
          : HadamardRotation.create(dimension, persistedSeed);
    }

    @Override
    public void checkIntegrity(MergePolicy.OneMerge merge) throws IOException {
      if (delegateReader != null) {
        delegateReader.checkIntegrity(merge);
      }
    }

    @Override
    public FloatVectorValues getFloatVectorValues(String field) throws IOException {
      FloatVectorValues values = delegateReader().getFloatVectorValues(field);
      if (values == null) {
        return null;
      }
      return new InverseRotatedFloatVectorValues(values, rotationFor(field));
    }

    @Override
    public ByteVectorValues getByteVectorValues(String field) {
      throw unsupportedEncoding(field, VectorEncoding.BYTE);
    }

    @Override
    public Float16VectorValues getFloat16VectorValues(String field) {
      throw unsupportedEncoding(field, VectorEncoding.FLOAT16);
    }

    @Override
    public void search(
        String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
        throws IOException {
      Objects.requireNonNull(target, "target must not be null");
      // Rotate query; fresh array since caller may reuse target.
      float[] searchTarget = new float[target.length];
      rotationFor(field).rotate(target, searchTarget);
      delegateReader().search(field, searchTarget, knnCollector, acceptDocs);
    }

    @Override
    public void search(
        String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) {
      throw unsupportedEncoding(field, VectorEncoding.BYTE);
    }

    @Override
    public void search(
        String field, short[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) {
      throw unsupportedEncoding(field, VectorEncoding.FLOAT16);
    }

    /**
     * Unreachable: the writer refuses non-float32 fields. Thrown rather than forwarded so a
     * violated assumption is loud instead of serving vectors in the wrong basis.
     */
    private UnsupportedOperationException unsupportedEncoding(
        String field, VectorEncoding encoding) {
      return new UnsupportedOperationException(
          NAME
              + " never indexes "
              + encoding
              + " vectors, so field \""
              + field
              + "\" cannot be read through it");
    }

    /**
     * Returns the delegate's merge instance, deliberately in <em>rotated</em> space: a merge reads
     * and writes rotated vectors, so the delegate's byte copying and centroid logic keeps working
     * and nothing is rotated twice.
     */
    @Override
    public KnnVectorsReader getMergeInstance() throws IOException {
      return delegateReader().getMergeInstance();
    }

    @Override
    public void finishMerge() throws IOException {
      delegateReader().finishMerge();
    }

    /**
     * Unwraps to the delegate, exposing rotated vectors: callers such as {@code CheckIndex} want
     * the format's internal view, not the application's original vectors.
     */
    @Override
    public KnnVectorsReader unwrapReaderForField(String field) {
      return delegateReader().unwrapReaderForField(field);
    }

    @Override
    public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
      return delegateReader().getOffHeapByteSize(fieldInfo);
    }

    @Override
    public void close() throws IOException {
      if (delegateReader != null) {
        delegateReader.close();
      }
    }
  }

  /**
   * Presents rotated stored vectors in their original space; doc and ordinal numbering are
   * untouched.
   *
   * @lucene.experimental
   */
  public static final class InverseRotatedFloatVectorValues extends FloatVectorValues {

    private final FloatVectorValues delegate;
    private final HadamardRotation rotation;
    private final float[] out;
    private final float[] scratch;

    /** Wraps {@code delegate}, undoing {@code rotation} on every value read. */
    public InverseRotatedFloatVectorValues(FloatVectorValues delegate, HadamardRotation rotation) {
      if (delegate.dimension() != rotation.dimension()) {
        throw new IllegalArgumentException(
            "dimension mismatch: values="
                + delegate.dimension()
                + " rotation="
                + rotation.dimension());
      }
      this.delegate = delegate;
      this.rotation = rotation;
      this.out = new float[rotation.dimension()];
      this.scratch = new float[rotation.dimension()];
    }

    @Override
    public float[] vectorValue(int ord) throws IOException {
      // Reused buffer per off-heap convention.
      rotation.inverseRotate(delegate.vectorValue(ord), out, scratch);
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
      // Independent buffers for the copy.
      return new InverseRotatedFloatVectorValues(delegate.copy(), rotation);
    }

    @Override
    public KnnVectorValues.DocIndexIterator iterator() {
      // Doc -> ord only; no vector values exposed.
      return delegate.iterator();
    }

    @Override
    public int ordToDoc(int ord) {
      return delegate.ordToDoc(ord);
    }

    @Override
    public VectorScorer scorer(float[] target) throws IOException {
      return delegate.scorer(rotate(target));
    }

    @Override
    public VectorScorer rescorer(float[] target) throws IOException {
      return delegate.rescorer(rotate(target));
    }

    /**
     * Rotates an original-space query so it can be scored against the rotated stored vectors.
     * Exact, since orthogonal transforms preserve the dot products and norms every similarity is
     * built on.
     */
    private float[] rotate(float[] target) {
      // Fresh array; delegate may retain.
      float[] rotated = new float[target.length];
      rotation.rotate(target, rotated, scratch);
      return rotated;
    }
  }
}
