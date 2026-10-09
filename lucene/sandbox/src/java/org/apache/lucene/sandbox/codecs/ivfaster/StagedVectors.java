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
package org.apache.lucene.sandbox.codecs.ivfaster;

import java.io.Closeable;
import java.io.IOException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.VectorUtil;

/**
 * The corpus of one build, staged to a temp file so that neither clustering nor emission holds it
 * in heap.
 *
 * <h2>Layout</h2>
 *
 * <p>One record per ordinal, in ASCENDING DOC-ID ORDER, at a fixed stride:
 *
 * <pre>
 *   [ fine CodeRecord: code | docId | primaryCell (placeholder) | corrections | pad ][ coarse planes ]
 * </pre>
 *
 * <p>The fine half is byte-identical to the record the code table will hold, so emission is one
 * copy plus a patch of the primary cell, and a donor's record is copied into it verbatim. The
 * coarse half is the packed thermometer planes the routing scan Hammings and the coarse section
 * stores. Both are derived exactly once per build.
 *
 * <p>Ordinal order is doc order by construction, verified as chunks are written: the ordinal map
 * the reader persists and the per-cell slot order the filtered path reads both depend on it.
 *
 * <h2>Access</h2>
 *
 * <p>Clustering reads through per-thread {@link Cursor}s, each over its own clone of the input,
 * decoding the fine code back to a rotated unit vector on demand. Every clustering pass walks
 * ordinals upward, so the file is streamed, and a pass reads {@code stride} bytes per document,
 * about a quarter of the float vector. Emission gathers records by ordinal in cell order through
 * {@link #copyRecord} and {@link #copyCoarse}.
 *
 * <p>An optional second file holds the caller's original FP32 vectors in the same order, for the
 * inert full-precision section; it is copied to the segment by concatenation.
 *
 * <p>The files are deleted on {@link #close}, and by the builder on any failure before that.
 */
final class StagedVectors implements VectorSource, Closeable {

  /** Ordinals per staging chunk: the one heap buffer proportional to a record, not the corpus. */
  static final int CHUNK_ORDS = Integer.getInteger("ivfaster.stageChunk", 16_384);

  private final Directory dir;
  private final String name;
  private final String rawName;
  private final IndexInput input;
  private final IndexInput rawInput;
  private final RandomAccessInput emit;
  private final long rawLength;
  private final int count;
  private final int dim;
  private final int codeBytes;
  private final int recordLen;
  private final int coarseBytes;
  private final int stride;
  private final FineQuantizer quantizer;
  private final float[] mean;

  private StagedVectors(Builder b, IndexInput input, IndexInput rawInput) throws IOException {
    this.dir = b.dir;
    this.name = b.name;
    this.rawName = b.rawName;
    this.input = input;
    this.rawInput = rawInput;
    this.rawLength = rawInput == null ? 0 : rawInput.length();
    this.count = b.written;
    this.dim = b.dim;
    this.codeBytes = b.codeBytes;
    this.recordLen = b.recordLen;
    this.coarseBytes = b.coarseBytes;
    this.stride = b.stride;
    this.quantizer = b.quantizer;
    this.mean = b.mean;
    this.emit = input.randomAccessSlice(0, input.length());
  }

  /** Opens a builder writing the temp file(s) for one field of one segment. */
  static Builder begin(
      Directory dir,
      String segmentName,
      IOContext ctx,
      int dim,
      FineQuantizer quantizer,
      float[] mean,
      boolean withRaw)
      throws IOException {
    return new Builder(dir, segmentName, ctx, dim, quantizer, mean, withRaw);
  }

  /** Stride of one staged record: fine record plus coarse planes. */
  static int strideFor(int dim, FineQuantizer quantizer) {
    return CodeRecord.length(quantizer.codeBytes(dim)) + Nitrox2.bytesPerVector(dim);
  }

  /**
   * Writes staged records in chunks and produces the {@link StagedVectors} on {@link #finish}.
   *
   * <p>The caller fills a chunk buffer in parallel over its ordinals with the static producers
   * below and hands it over whole, so the only heap proportional to the corpus is one chunk.
   */
  static final class Builder {
    final Directory dir;
    final IOContext ctx;
    final int dim;
    final int codeBytes;
    final int recordLen;
    final int coarseBytes;
    final int stride;
    final FineQuantizer quantizer;
    final float[] mean;
    private IndexOutput out;
    private IndexOutput rawOut;
    private String name;
    private String rawName;
    private int written;
    private int lastDoc = -1;

    private Builder(
        Directory dir,
        String segmentName,
        IOContext ctx,
        int dim,
        FineQuantizer quantizer,
        float[] mean,
        boolean withRaw)
        throws IOException {
      this.dir = dir;
      this.ctx = ctx;
      this.dim = dim;
      this.quantizer = quantizer;
      this.mean = mean;
      this.codeBytes = quantizer.codeBytes(dim);
      this.recordLen = CodeRecord.length(codeBytes);
      this.coarseBytes = Nitrox2.bytesPerVector(dim);
      this.stride = recordLen + coarseBytes;
      boolean success = false;
      try {
        out = dir.createTempOutput(segmentName, "ivfstage", ctx);
        name = out.getName();
        if (withRaw) {
          rawOut = dir.createTempOutput(segmentName, "ivfraw", ctx);
          rawName = rawOut.getName();
        }
        success = true;
      } finally {
        if (success == false) {
          abort();
        }
      }
    }

    /** A chunk buffer for {@code ords} records; reuse it across {@link #writeChunk} calls. */
    byte[] chunk(int ords) {
      return new byte[Math.multiplyExact(ords, stride)];
    }

    /** A raw-float chunk for {@code ords} vectors, or null when no raw section is kept. */
    float[] rawChunk(int ords) {
      return rawOut == null ? null : new float[Math.multiplyExact(ords, dim)];
    }

    /**
     * Appends {@code n} records from {@code chunk} (and their raw vectors from {@code raw}, when
     * kept), checking that doc ids keep ascending across everything written so far.
     */
    void writeChunk(byte[] chunk, float[] raw, int n) throws IOException {
      final int docIdOffset = CodeRecord.docIdOffset(codeBytes);
      for (int j = 0; j < n; j++) {
        final int doc = CodeRecord.readIntLE(chunk, j * stride + docIdOffset);
        if (doc <= lastDoc) {
          throw new IllegalStateException(
              "staged records must ascend by doc id: " + doc + " after " + lastDoc);
        }
        lastDoc = doc;
      }
      out.writeBytes(chunk, 0, n * stride);
      if (rawOut != null) {
        final int floats = n * dim;
        for (int i = 0; i < floats; i++) {
          rawOut.writeInt(Float.floatToIntBits(raw[i]));
        }
      }
      written += n;
    }

    int written() {
      return written;
    }

    /** Closes the outputs and reopens them for reading; deletes everything on failure. */
    StagedVectors finish() throws IOException {
      IndexInput in = null;
      IndexInput rawIn = null;
      boolean success = false;
      try {
        IOUtils.close(out, rawOut);
        out = null;
        rawOut = null;
        in = dir.openInput(name, ctx);
        if (rawName != null) {
          rawIn = dir.openInput(rawName, ctx);
        }
        final StagedVectors staged = new StagedVectors(this, in, rawIn);
        success = true;
        return staged;
      } finally {
        if (success == false) {
          IOUtils.closeWhileHandlingException(in, rawIn);
          abort();
        }
      }
    }

    /** Releases and deletes the temp files; safe to call more than once and after failure. */
    void abort() {
      IOUtils.closeWhileHandlingException(out, rawOut);
      out = null;
      rawOut = null;
      if (name != null) {
        IOUtils.deleteFilesIgnoringExceptions(dir, name);
      }
      if (rawName != null) {
        IOUtils.deleteFilesIgnoringExceptions(dir, rawName);
      }
    }

    // ---- record producers, safe to call from several threads on disjoint chunk ranges ----

    /** Per-worker scratch for {@link #encodeInto}. */
    static final class EncodeScratch {
      final byte[] code;
      final float[] corrections = new float[CodeRecord.CORRECTIONS];
      final float[] centred;

      EncodeScratch(int dim, int codeBytes, boolean withMean) {
        this.code = new byte[codeBytes];
        this.centred = withMean ? new float[dim] : null;
      }
    }

    EncodeScratch encodeScratch() {
      return new EncodeScratch(dim, codeBytes, mean != null);
    }

    /**
     * Encodes one rotated unit vector into the staged record at {@code buf[off]}: fine code and
     * corrections through the quantizer (centred on the mean when the tier centres), the doc id, a
     * zero primary cell for emission to patch, and the packed coarse planes.
     */
    void encodeInto(float[] rotated, int docId, byte[] buf, int off, EncodeScratch sc) {
      float[] toEncode = rotated;
      if (mean != null) {
        for (int d = 0; d < dim; d++) {
          sc.centred[d] = rotated[d] - mean[d];
        }
        toEncode = sc.centred;
      }
      quantizer.encode(toEncode, dim, mean, sc.code, sc.corrections);
      System.arraycopy(sc.code, 0, buf, off + CodeRecord.codeOffset(), codeBytes);
      CodeRecord.writeIntLE(buf, off + CodeRecord.docIdOffset(codeBytes), docId);
      CodeRecord.writeIntLE(buf, off + CodeRecord.primaryCellOffset(codeBytes), 0);
      for (int k = 0; k < CodeRecord.CORRECTIONS; k++) {
        CodeRecord.writeIntLE(
            buf,
            off + CodeRecord.correctionOffset(codeBytes, k),
            Float.floatToIntBits(sc.corrections[k]));
      }
      Nitrox2.packPlanes(rotated, dim, buf, off + recordLen, Nitrox2.planeBytes(dim));
    }

    /**
     * Copies a donor document's fine record and coarse planes VERBATIM into the staged record at
     * {@code buf[off]}, rewriting only the doc id; see {@code DonorView.copyRecord} for why a copy
     * rather than a re-encode.
     */
    void copyInto(IVFasterVectorsReader.DonorView donor, int srcOrd, int docId, byte[] buf, int off)
        throws IOException {
      donor.copyRecordAt(srcOrd, docId, 0, buf, off);
      donor.copyCoarse(srcOrd, buf, off + recordLen);
    }

    /** Overwrites the coarse planes of the record at {@code buf[off]} with a source's verbatim. */
    void copyCoarseInto(IVFasterVectorsReader.DonorView source, int srcOrd, byte[] buf, int off)
        throws IOException {
      source.copyCoarse(srcOrd, buf, off + recordLen);
    }
  }

  @Override
  public int count() {
    return count;
  }

  @Override
  public int dim() {
    return dim;
  }

  int recordLen() {
    return recordLen;
  }

  int coarseBytes() {
    return coarseBytes;
  }

  FineQuantizer quantizer() {
    return quantizer;
  }

  @Override
  public Cursor cursor() throws IOException {
    return new Cursor(input.clone().randomAccessSlice(0, input.length()));
  }

  /** One thread's cursor: a record buffer and a decoded vector over its own input clone. */
  final class Cursor implements VectorSource.Cursor {
    private final RandomAccessInput in;
    private final byte[] rec;
    private final float[] vec;
    private final float[] corrections = new float[CodeRecord.CORRECTIONS];
    private boolean decoded;

    private Cursor(RandomAccessInput in) {
      this.in = in;
      this.rec = new byte[stride];
      this.vec = new float[dim];
    }

    @Override
    public void load(int ord) throws IOException {
      in.readBytes((long) ord * stride, rec, 0, stride);
      decoded = false;
    }

    /**
     * The fine code decoded back to a rotated unit vector, once per load. The decode is the
     * quantizer's own, so the vector clustering sees is exactly the one a query scores.
     */
    @Override
    public float[] vector() {
      if (decoded == false) {
        for (int k = 0; k < CodeRecord.CORRECTIONS; k++) {
          corrections[k] =
              Float.intBitsToFloat(
                  CodeRecord.readIntLE(rec, CodeRecord.correctionOffset(codeBytes, k)));
        }
        quantizer.decode(rec, CodeRecord.codeOffset(), dim, mean, corrections, vec);
        // Lossy reconstruction, so renormalize; every distance assumes unit length.
        VectorUtil.l2normalize(vec, false);
        decoded = true;
      }
      return vec;
    }

    @Override
    public void coarseInto(byte[] dest) {
      System.arraycopy(rec, recordLen, dest, 0, coarseBytes);
    }
  }

  // ---- emission, single-threaded ----

  /** Copies ordinal {@code ord}'s fine record into {@code dest[0..recordLen)}. */
  void copyRecord(int ord, byte[] dest) throws IOException {
    emit.readBytes((long) ord * stride, dest, 0, recordLen);
  }

  /** Copies ordinal {@code ord}'s coarse planes into {@code dest[0..coarseBytes)}. */
  void copyCoarse(int ord, byte[] dest) throws IOException {
    emit.readBytes((long) ord * stride + recordLen, dest, 0, coarseBytes);
  }

  /** Hints ordinal {@code ord}'s record, for a gather that knows its next few ordinals. */
  void prefetch(int ord) throws IOException {
    emit.prefetch((long) ord * stride, stride);
  }

  /** The doc id of ordinal {@code ord}. */
  int docId(int ord) throws IOException {
    return emit.readInt((long) ord * stride + CodeRecord.docIdOffset(codeBytes));
  }

  /** Length in bytes of the raw FP32 section, or 0 when none was kept. */
  long rawLength() {
    return rawLength;
  }

  /** The raw FP32 vectors in ordinal order, positioned at the start; null when none were kept. */
  IndexInput rawInput() throws IOException {
    if (rawInput == null) {
      return null;
    }
    final IndexInput clone = rawInput.clone();
    clone.seek(0);
    return clone;
  }

  @Override
  public void close() throws IOException {
    try {
      IOUtils.close(input, rawInput);
    } finally {
      IOUtils.deleteFilesIgnoringExceptions(dir, name);
      if (rawName != null) {
        IOUtils.deleteFilesIgnoringExceptions(dir, rawName);
      }
    }
  }
}
