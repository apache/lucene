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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Exercises the {@code strategyFactory} constructor of {@link Lucene99FlatVectorsWriter} against
 * the full {@link BaseKnnVectorsFormatTestCase} suite, using a paged storage strategy as a concrete
 * example of a non-default {@link FlatFieldVectorsWriter}.
 *
 * <p>The strategy stores vectors in a {@code List<ByteBuffer>} of fixed-size pages instead of the
 * default {@code ArrayList<float[]>} / {@code ArrayList<byte[]>}, and exposes them back through
 * {@link FlatFieldVectorsWriter#getVectors()} via an {@link AbstractList} adapter that materializes
 * a heap array per access. The on-disk format produced is identical to the default configuration —
 * only the in-memory accumulation differs.
 */
public class TestKnnVectorsFormatCustomWriter extends BaseKnnVectorsFormatTestCase {

  @Override
  protected Codec getCodec() {
    return TestUtil.alwaysKnnVectorsFormat(new PagedHnswVectorsFormat());
  }

  @Override
  protected boolean supportsFloatVectorFallback() {
    return false;
  }

  /**
   * A {@link KnnVectorsFormat} that produces an HNSW writer whose underlying flat vector writer
   * uses the paged storage strategy. Reads are delegated to a standard {@link
   * Lucene99HnswVectorsFormat} — the on-disk format is unchanged.
   */
  private static final class PagedHnswVectorsFormat extends KnnVectorsFormat {

    private static final FlatVectorsScorer SCORER =
        FlatVectorScorerUtil.getLucene99FlatVectorsScorer();
    private static final FlatVectorsFormat FLAT_FORMAT = new Lucene99FlatVectorsFormat(SCORER);
    private static final Lucene99HnswVectorsFormat READ_FORMAT = new Lucene99HnswVectorsFormat();

    PagedHnswVectorsFormat() {
      // Reuse the registered name so segments written by this format can be reopened by the
      // standard Lucene99HnswVectorsFormat via SPI — the on-disk bytes are identical.
      super("Lucene99HnswVectorsFormat");
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
      // Build a flat writer with our paged strategy, and wrap it in the standard HNSW writer.
      var flatWriter =
          new Lucene99FlatVectorsWriter(state, SCORER, PagedFieldVectorsWriter::create);
      return new Lucene99HnswVectorsWriter(
          state,
          Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN,
          Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH,
          FLAT_FORMAT,
          flatWriter,
          Lucene99HnswVectorsFormat.DEFAULT_NUM_MERGE_WORKER,
          null);
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
      return READ_FORMAT.fieldsReader(state);
    }

    @Override
    public int getMaxDimensions(String fieldName) {
      return DEFAULT_MAX_DIMENSIONS;
    }
  }

  /**
   * A {@link FlatFieldVectorsWriter} that accumulates vectors into a list of fixed-size {@link
   * ByteBuffer} pages, each holding {@link #VECTORS_PER_PAGE} vectors.
   */
  private abstract static class PagedFieldVectorsWriter<T> extends FlatFieldVectorsWriter<T> {

    private static final long SHALLOW_RAM_BYTES_USED =
        RamUsageEstimator.shallowSizeOfInstance(PagedFieldVectorsWriter.class);

    /** Fixed at 10 to keep the test deliberately simple. */
    private static final int VECTORS_PER_PAGE = 10;

    private final FieldInfo fieldInfo;
    private final int bytesPerVector;
    private final DocsWithFieldSet docsWithField = new DocsWithFieldSet();
    private final List<ByteBuffer> pages = new ArrayList<>();
    private int size;
    private int lastDocID = -1;
    private boolean finished;

    static PagedFieldVectorsWriter<?> create(FieldInfo fi) {
      return switch (fi.getVectorEncoding()) {
        case FLOAT32 -> new FloatPaged(fi);
        case BYTE -> new BytePaged(fi);
      };
    }

    PagedFieldVectorsWriter(FieldInfo fi, int bytesPerVector) {
      this.fieldInfo = fi;
      this.bytesPerVector = bytesPerVector;
    }

    /** Writes one vector's worth of bytes into {@code buf} at its current position. */
    abstract void serialize(ByteBuffer buf, T vector);

    /** Reads one vector's worth of bytes from {@code buf} at its current position. */
    abstract T deserialize(ByteBuffer buf);

    /** Returns a fresh, positioned view of the byte region holding vector {@code ord}. */
    private ByteBuffer bufferFor(int ord) {
      ByteBuffer page = pages.get(ord / VECTORS_PER_PAGE);
      int offset = (ord % VECTORS_PER_PAGE) * bytesPerVector;
      return page.slice(offset, bytesPerVector).order(ByteOrder.LITTLE_ENDIAN);
    }

    @Override
    public final void addValue(int docID, T vectorValue) {
      if (finished) {
        throw new IllegalStateException("already finished, cannot add more values");
      }
      if (docID == lastDocID) {
        throw new IllegalArgumentException(
            "VectorValuesField \""
                + fieldInfo.name
                + "\" appears more than once in this document (only one value is allowed per field)");
      }
      assert docID > lastDocID;
      lastDocID = docID;

      if (size % VECTORS_PER_PAGE == 0) {
        pages.add(
            ByteBuffer.allocate(VECTORS_PER_PAGE * bytesPerVector).order(ByteOrder.LITTLE_ENDIAN));
      }
      serialize(bufferFor(size), vectorValue);
      docsWithField.add(docID);
      size++;
    }

    /**
     * Unsupported: this writer owns its storage and copies vector bytes directly in {@link
     * #addValue} via {@link #serialize(ByteBuffer, Object)}. {@code copyValue} is never invoked.
     */
    @Override
    public final T copyValue(T vectorValue) {
      throw new UnsupportedOperationException();
    }

    @Override
    public final List<T> getVectors() {
      return new AbstractList<>() {
        @Override
        public T get(int index) {
          return deserialize(bufferFor(index));
        }

        @Override
        public int size() {
          return size;
        }
      };
    }

    @Override
    public final DocsWithFieldSet getDocsWithFieldSet() {
      return docsWithField;
    }

    @Override
    public final void finish() {
      finished = true;
    }

    @Override
    public final boolean isFinished() {
      return finished;
    }

    @Override
    public final long ramBytesUsed() {
      return SHALLOW_RAM_BYTES_USED
          + docsWithField.ramBytesUsed()
          + (long) pages.size()
              * (RamUsageEstimator.NUM_BYTES_OBJECT_REF + VECTORS_PER_PAGE * bytesPerVector);
    }

    private static final class FloatPaged extends PagedFieldVectorsWriter<float[]> {
      private final int dim;

      FloatPaged(FieldInfo fi) {
        super(fi, fi.getVectorDimension() * Float.BYTES);
        this.dim = fi.getVectorDimension();
      }

      @Override
      void serialize(ByteBuffer buf, float[] vector) {
        buf.asFloatBuffer().put(vector);
      }

      @Override
      float[] deserialize(ByteBuffer buf) {
        float[] out = new float[dim];
        buf.asFloatBuffer().get(out);
        return out;
      }
    }

    private static final class BytePaged extends PagedFieldVectorsWriter<byte[]> {
      private final int dim;

      BytePaged(FieldInfo fi) {
        super(fi, fi.getVectorDimension());
        this.dim = fi.getVectorDimension();
      }

      @Override
      void serialize(ByteBuffer buf, byte[] vector) {
        buf.put(vector);
      }

      @Override
      byte[] deserialize(ByteBuffer buf) {
        byte[] out = new byte[dim];
        buf.get(out);
        return out;
      }
    }
  }
}
