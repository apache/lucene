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

import static org.apache.lucene.index.VectorEncoding.FLOAT32;

import java.io.IOException;
import org.apache.lucene.codecs.lucene104.OffHeapScalarQuantizedVectorValues;
import org.apache.lucene.codecs.lucene95.OffHeapFloatVectorValues;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.sandbox.codecs.dedup.DedupVectorValues.FieldOrdToGroupOrd;
import org.apache.lucene.sandbox.codecs.dedup.DedupVectorValues.FieldOrdToGroupOrdOffHeap;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues.ScalarEncoding;

/**
 * Vector values over data-blind scalar quantized, de-duplicated vectors. Mirrors the raw {@link
 * DedupVectorValues} structure: a dense group view (an {@link OffHeapScalarQuantizedVectorValues}
 * with a zero centroid) holds one quantized record per distinct vector, and a {@link FieldValues}
 * view maps a field's per-document ordinals onto it via the shared {@code fieldOrdToGroupOrd}
 * translation.
 *
 * @lucene.experimental
 */
final class DedupScalarQuantizedVectorValues {

  private DedupScalarQuantizedVectorValues() {}

  /** Number of corrective bytes stored per quantized vector. */
  static final int CORRECTIVE_BYTES = 3 * Float.BYTES + Integer.BYTES;

  /**
   * A dense view over a group's quantized records of one {@link DedupQuantizer.Flavor}, indexed by
   * group ordinal. Quantization is data-blind, i.e. against a zero centroid (see {@link
   * DedupQuantizer}). With {@code isQuerySide}, records hold query-encoded vectors instead (only
   * different for asymmetric encodings, used for merge-time graph construction).
   */
  static QuantizedByteVectorValues groupValues(
      boolean isQuerySide,
      DedupScalarQuantizedVectorsScorer vectorsScorer,
      VectorSimilarityFunction function,
      DedupQuantizer.Flavor flavor,
      ScalarEncoding encoding,
      int dimension,
      int groupNumVectors,
      IndexInput slice) {

    return new OffHeapScalarQuantizedVectorValues.DenseOffHeapVectorValues(
        isQuerySide,
        dimension,
        groupNumVectors,
        new float[dimension], // always a zero centroid, quantization is data-blind
        0,
        flavor.quantizer(),
        encoding,
        function,
        vectorsScorer,
        slice);
  }

  static FieldValues loadQuantized(
      DedupScalarQuantizedVectorsScorer vectorsScorer,
      VectorSimilarityFunction function,
      ScalarEncoding encoding,
      OrdToDocDISIReaderConfiguration configuration,
      int dimension,
      int groupNumVectors,
      IndexInput vectorData,
      IndexInput quantizedVectorData,
      long quantizedDataOffset,
      long quantizedDataSize,
      long fieldOrdToGroupOrdOffset,
      long fieldOrdToGroupOrdSize)
      throws IOException {

    final OffHeapFloatVectorValues fieldView =
        OffHeapFloatVectorValues.load(
            function, vectorsScorer, configuration, FLOAT32, dimension, 0, 0, vectorData);

    final QuantizedByteVectorValues groupView =
        groupValues(
            false,
            vectorsScorer,
            function,
            DedupQuantizer.Flavor.of(function),
            encoding,
            dimension,
            groupNumVectors,
            quantizedVectorData.slice(
                "quantized-group-slice", quantizedDataOffset, quantizedDataSize));

    final FieldOrdToGroupOrd fieldOrdToGroupOrd =
        new FieldOrdToGroupOrdOffHeap(vectorData, fieldOrdToGroupOrdOffset, fieldOrdToGroupOrdSize);

    return new FieldValues(vectorsScorer, function, fieldView, groupView, fieldOrdToGroupOrd);
  }

  /**
   * A field's quantized vector values: doc operations delegate to the field's ordinal-to-doc
   * mapping while vector operations resolve through {@code fieldOrdToGroupOrd} into the shared
   * group view.
   */
  static final class FieldValues extends QuantizedByteVectorValues implements DedupVectorValues {
    private final DedupScalarQuantizedVectorsScorer vectorsScorer;
    private final VectorSimilarityFunction function;
    private final FloatVectorValues fieldView;
    private final QuantizedByteVectorValues groupView;
    private final FieldOrdToGroupOrd fieldOrdToGroupOrd;
    private int[] scratch;

    FieldValues(
        DedupScalarQuantizedVectorsScorer vectorsScorer,
        VectorSimilarityFunction function,
        FloatVectorValues fieldView,
        QuantizedByteVectorValues groupView,
        FieldOrdToGroupOrd fieldOrdToGroupOrd) {
      this.vectorsScorer = vectorsScorer;
      this.function = function;
      this.fieldView = fieldView;
      this.groupView = groupView;
      this.fieldOrdToGroupOrd = fieldOrdToGroupOrd;
      this.scratch = new int[DedupUtil.SCRATCH_INITIAL_SIZE];
    }

    @Override
    public QuantizedByteVectorValues getGroupView() {
      return groupView;
    }

    @Override
    public FieldOrdToGroupOrd getFieldOrdToGroupOrd() {
      return fieldOrdToGroupOrd;
    }

    /** The quantization flavor of this field's records, derived from its similarity function. */
    DedupQuantizer.Flavor flavor() {
      return DedupQuantizer.Flavor.of(function);
    }

    @Override
    public int dimension() {
      return groupView.dimension();
    }

    @Override
    public int size() {
      return fieldView.size();
    }

    @Override
    public int ordToDoc(int ord) {
      return fieldView.ordToDoc(ord);
    }

    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
      return fieldView.getAcceptOrds(acceptDocs);
    }

    @Override
    public DocIndexIterator iterator() {
      return fieldView.iterator();
    }

    @Override
    public void prefetch(int[] ordsToPrefetch, int numOrds) throws IOException {
      if (scratch.length < ordsToPrefetch.length) { // grow if needed
        scratch = ArrayUtil.grow(scratch, ordsToPrefetch.length);
      }
      for (int i = 0; i < numOrds; i++) {
        scratch[i] = fieldOrdToGroupOrd.get(ordsToPrefetch[i]);
      }
      groupView.prefetch(scratch, numOrds);
    }

    @Override
    public byte[] vectorValue(int ord) throws IOException {
      return groupView.vectorValue(fieldOrdToGroupOrd.get(ord));
    }

    @Override
    public OptimizedScalarQuantizer.QuantizationResult getCorrectiveTerms(int ord)
        throws IOException {
      return groupView.getCorrectiveTerms(fieldOrdToGroupOrd.get(ord));
    }

    @Override
    public OptimizedScalarQuantizer getQuantizer() {
      return groupView.getQuantizer();
    }

    @Override
    public ScalarEncoding getScalarEncoding() {
      return groupView.getScalarEncoding();
    }

    @Override
    public float[] getCentroid() throws IOException {
      return groupView.getCentroid();
    }

    @Override
    public float getCentroidDP() {
      return 0;
    }

    @Override
    public int getVectorByteLength() {
      return groupView.getVectorByteLength();
    }

    @Override
    public FieldValues copy() throws IOException {
      return new FieldValues(
          vectorsScorer, function, fieldView.copy(), groupView.copy(), fieldOrdToGroupOrd.copy());
    }

    @Override
    public VectorScorer scorer(float[] target) throws IOException {
      if (size() == 0) {
        return null;
      }
      FieldValues copy = copy();
      DocIndexIterator indexIterator = copy.iterator();
      RandomVectorScorer vectorScorer = vectorsScorer.getRandomVectorScorer(function, copy, target);
      boolean isDense = copy.fieldView instanceof OffHeapFloatVectorValues.DenseOffHeapVectorValues;
      return new DedupVectorScorer(indexIterator, vectorScorer, isDense);
    }
  }

  /**
   * Full-precision view of a field backed by the raw de-duplicated vectors, whose {@link
   * #scorer(float[])} scores against the quantized values instead. Mirrors {@code
   * Lucene104ScalarQuantizedVectorsReader.ScalarQuantizedVectorValues}.
   */
  static final class RawAndQuantizedValues extends FloatVectorValues implements DedupVectorValues {
    private final DedupVectorValues.FloatImpl rawValues;
    private final FieldValues quantizedValues;

    RawAndQuantizedValues(DedupVectorValues.FloatImpl rawValues, FieldValues quantizedValues) {
      this.rawValues = rawValues;
      this.quantizedValues = quantizedValues;
    }

    FieldValues getQuantizedValues() {
      return quantizedValues;
    }

    @Override
    public FloatVectorValues getGroupView() {
      return rawValues.getGroupView();
    }

    @Override
    public FieldOrdToGroupOrd getFieldOrdToGroupOrd() {
      return rawValues.getFieldOrdToGroupOrd();
    }

    @Override
    public int dimension() {
      return rawValues.dimension();
    }

    @Override
    public int size() {
      return rawValues.size();
    }

    @Override
    public int ordToDoc(int ord) {
      return rawValues.ordToDoc(ord);
    }

    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
      return rawValues.getAcceptOrds(acceptDocs);
    }

    @Override
    public void prefetch(int[] ordsToPrefetch, int numOrds) throws IOException {
      rawValues.prefetch(ordsToPrefetch, numOrds);
    }

    @Override
    public float[] vectorValue(int ord) throws IOException {
      return rawValues.vectorValue(ord);
    }

    @Override
    public DocIndexIterator iterator() {
      return rawValues.iterator();
    }

    @Override
    public RawAndQuantizedValues copy() throws IOException {
      return new RawAndQuantizedValues(rawValues.copy(), quantizedValues.copy());
    }

    @Override
    public VectorScorer scorer(float[] target) throws IOException {
      return quantizedValues.scorer(target);
    }

    @Override
    public VectorScorer rescorer(float[] target) throws IOException {
      return rawValues.rescorer(target);
    }
  }
}
