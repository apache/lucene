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

package org.apache.lucene.sandbox.codecs.ivfaster_evo;

import java.io.Closeable;
import java.io.IOException;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.sandbox.codecs.ivfaster_evo.IVFasterEvoVectorsFormat.Tier;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * One disk-backed [coarse][fine] record per document, encoded (or copied from a compatible merge
 * source) exactly once at ingestion. Clustering trains on the staged fine tier and reuses the
 * staged coarse codes; the writer then gathers both sections into cell order. No float vectors are
 * held on heap, and the temporary file is deleted on close.
 */
final class StagedVectors extends KnnFieldVectorsWriter<float[]> implements Closeable {
  private final Directory directory;
  private final String name;
  private final TierCodec coarse, fine;
  private final int stride;
  private final DocsWithFieldSet docs = new DocsWithFieldSet();
  private IndexOutput output;
  private IndexInput input;
  private boolean closed;

  StagedVectors(
      Directory directory, String segment, IOContext context, TierCodec coarse, TierCodec fine)
      throws IOException {
    this.directory = directory;
    this.coarse = coarse;
    this.fine = fine;
    stride = coarse.bytes + fine.bytes;
    output = directory.createTempOutput(segment, "ivfe-stage", context);
    name = output.getName();
  }

  @Override
  public void addValue(int docID, float[] vector) throws IOException {
    if (vector.length != fine.dim) throw new IllegalArgumentException("incorrect vector dimension");
    add(docID);
    byte[] code = coarse.encode(vector);
    output.writeBytes(code, code.length);
    code = fine.encode(vector);
    output.writeBytes(code, code.length);
  }

  /** Copies a compatible source's record byte-for-byte, without decoding it. */
  void addRecord(int docID, TieredVectors source, int ord) throws IOException {
    add(docID);
    source.copyRecord(ord, output);
  }

  private void add(int docID) {
    if (output == null) throw new IllegalStateException("staging is finished");
    docs.add(docID);
  }

  @Override
  public float[] copyValue(float[] vector) {
    return vector.clone();
  }

  DocsWithFieldSet getDocsWithFieldSet() {
    return docs;
  }

  /** Whether training values (and therefore seeds and centroids) are in rotated space. */
  boolean rotated() {
    return fine.tier == Tier.U8;
  }

  float[][] rotateSeeds(float[][] seeds) {
    if (seeds == null || rotated() == false) return seeds;
    float[][] result = new float[seeds.length][fine.dim];
    for (int c = 0; c < seeds.length; c++) fine.rotation.rotate(seeds[c], result[c]);
    return result;
  }

  void restoreCentroids(float[][] centroids) {
    if (rotated() == false) return;
    for (int c = 0; c < centroids.length; c++) {
      float[] restored = new float[fine.dim];
      fine.rotation.inverseRotate(centroids[c], restored);
      centroids[c] = restored;
    }
  }

  /** Ends ingestion and returns an ordinal-dense training view with its own cursor and scratch. */
  Values values() throws IOException {
    return new Values(input().clone());
  }

  void copySection(int ord, boolean coarseSection, IndexOutput out) throws IOException {
    IndexInput in = input();
    in.seek((long) ord * stride + (coarseSection ? 0 : coarse.bytes));
    out.copyBytes(in, coarseSection ? coarse.bytes : fine.bytes);
  }

  private IndexInput input() throws IOException {
    if (closed) throw new IllegalStateException("staging is closed");
    if (output != null) {
      output.close();
      output = null;
    }
    if (input == null) {
      input = directory.openInput(name, IOContext.DEFAULT);
    }
    return input;
  }

  /** U8 records decode directly into rotated space; FP32 records are the original vectors. */
  final class Values extends FloatVectorValues {
    private final IndexInput in;
    private final byte[] record = new byte[stride];
    private final float[] vector = new float[fine.dim];
    private int loaded = -1;

    private Values(IndexInput in) {
      this.in = in;
    }

    private void load(int ord) throws IOException {
      java.util.Objects.checkIndex(ord, size());
      if (ord == loaded) return;
      loaded = -1;
      in.seek((long) ord * stride);
      in.readBytes(record, 0, stride);
      loaded = ord;
    }

    boolean rotated() {
      return StagedVectors.this.rotated();
    }

    void coarseCode(int ord, byte[] dest) throws IOException {
      load(ord);
      System.arraycopy(record, 0, dest, 0, coarse.bytes);
    }

    @Override
    public float[] vectorValue(int ord) throws IOException {
      load(ord);
      fine.decodeUnrotated(record, coarse.bytes, vector);
      return vector;
    }

    @Override
    public int dimension() {
      return fine.dim;
    }

    @Override
    public int size() {
      return docs.cardinality();
    }

    @Override
    public Values copy() {
      return new Values(in.clone());
    }

    @Override
    public DocIndexIterator iterator() {
      return createDenseIterator();
    }
  }

  @Override
  public long ramBytesUsed() {
    return RamUsageEstimator.shallowSizeOfInstance(StagedVectors.class)
        + docs.ramBytesUsed()
        + 16 * 1024;
  }

  @Override
  public void close() throws IOException {
    if (closed) return;
    closed = true;
    try {
      IOUtils.close(input, output);
    } catch (Throwable t) {
      IOUtils.deleteFilesSuppressingExceptions(t, directory, name);
      throw t;
    }
    directory.deleteFile(name);
  }
}
