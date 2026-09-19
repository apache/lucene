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

package org.apache.lucene.search;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.VectorBatch;

/**
 * A {@link DoubleValuesSource} that computes vector similarity between a query vector and raw full
 * precision vectors indexed in provided {@link org.apache.lucene.document.KnnFloatVectorField} in
 * documents.
 */
public class FullPrecisionFloatVectorSimilarityValuesSource extends DoubleValuesSource {

  private final float[] queryVector;
  private final String fieldName;
  private VectorSimilarityFunction vectorSimilarityFunction;

  /**
   * Creates a {@link DoubleValuesSource} that returns vector similarity score between provided
   * query vector and field for documents.
   *
   * @param vector the query vector
   * @param fieldName the field name of the {@link org.apache.lucene.document.KnnFloatVectorField}
   * @param vectorSimilarityFunction the vector similarity function to use
   */
  public FullPrecisionFloatVectorSimilarityValuesSource(
      float[] vector, String fieldName, VectorSimilarityFunction vectorSimilarityFunction) {
    this.queryVector = vector;
    this.fieldName = fieldName;
    this.vectorSimilarityFunction = vectorSimilarityFunction;
  }

  /**
   * Creates a {@link DoubleValuesSource} that returns vector similarity score between provided
   * query vector and field for documents. Uses the configured vector similarity function for the
   * field.
   *
   * @param vector the query vector
   * @param fieldName the field name of the {@link org.apache.lucene.document.KnnFloatVectorField}
   */
  public FullPrecisionFloatVectorSimilarityValuesSource(float[] vector, String fieldName) {
    this(vector, fieldName, null);
  }

  /** Sugar to fetch full precision similarity score values */
  public DoubleValues getSimilarityScores(LeafReaderContext ctx) throws IOException {
    return getValues(ctx, null);
  }

  @Override
  public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
    final FloatVectorValues vectorValues = ctx.reader().getFloatVectorValues(fieldName);
    if (vectorValues == null) {
      FloatVectorValues.checkField(ctx.reader(), fieldName);
      return DoubleValues.EMPTY;
    }
    final FieldInfo fi = ctx.reader().getFieldInfos().fieldInfo(fieldName);
    if (fi.getVectorDimension() != queryVector.length) {
      throw new IllegalArgumentException(
          "Query vector dimension does not match field dimension: "
              + queryVector.length
              + " != "
              + fi.getVectorDimension());
    }

    if (vectorSimilarityFunction == null) {
      VectorScorer scorer = vectorValues.rescorer(queryVector);
      if (scorer == null) {
        return DoubleValues.EMPTY;
      }
      DocIdSetIterator iterator = scorer.iterator();
      return new DoubleValues() {
        @Override
        public double doubleValue() throws IOException {
          return scorer.score();
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
          return doc >= iterator.docID()
              && (iterator.docID() == doc || iterator.advance(doc) == doc);
        }
      };
    }
    final KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();
    return new DoubleValues() {
      @Override
      public double doubleValue() throws IOException {
        return vectorSimilarityFunction.compare(
            queryVector, vectorValues.vectorValue(iterator.index()));
      }

      @Override
      public boolean advanceExact(int doc) throws IOException {
        return doc >= iterator.docID() && (iterator.docID() == doc || iterator.advance(doc) == doc);
      }
    };
  }

  /**
   * One segment's shortlist: the vectors requested for it and, once the batch has run, the scores.
   * Held between {@link #queueShortlist} and {@link #scores()} so a query can queue every segment
   * before any read is issued.
   */
  final class Pending {
    private final float[] vectors;
    private final int dim;
    private final int count;
    private final VectorSimilarityFunction fn;

    private Pending(float[] vectors, int dim, int count, VectorSimilarityFunction fn) {
      this.vectors = vectors;
      this.dim = dim;
      this.count = count;
      this.fn = fn;
    }

    /**
     * Similarity of the query to each requested vector; valid only after the batch has executed.
     */
    float[] scores() {
      float[] scores = new float[count];
      float[] vector = new float[dim];
      for (int i = 0; i < count; i++) {
        System.arraycopy(vectors, i * dim, vector, 0, dim);
        scores[i] = fn.compare(queryVector, vector);
      }
      return scores;
    }
  }

  /**
   * Opens a batch able to gather raw-vector reads across the segments of {@code ctx}'s index, or
   * null when the store cannot serve them that way (in which case callers score vectors per
   * document).
   */
  VectorBatch newVectorBatch(LeafReaderContext ctx) throws IOException {
    FlatVectorsReader flatReader = rawVectorsReader(ctx);
    return flatReader == null ? null : flatReader.newRawVectorBatch(fieldName);
  }

  /**
   * Resolves this segment's shortlist to vector positions and queues its reads into {@code batch}.
   * Nothing is read until {@link VectorBatch#execute()}. Returns null when this segment cannot be
   * served from the batch.
   */
  Pending queueShortlist(LeafReaderContext ctx, int[] docs, int count, VectorBatch batch)
      throws IOException {
    if (count == 0) {
      return null;
    }
    FlatVectorsReader flatReader = rawVectorsReader(ctx);
    if (flatReader == null) {
      return null;
    }
    FloatVectorValues values = ctx.reader().getFloatVectorValues(fieldName);
    if (values == null || values.dimension() != queryVector.length) {
      return null;
    }
    final int dim = queryVector.length;
    KnnVectorValues.DocIndexIterator it = values.iterator();
    int[] ords = new int[count];
    for (int i = 0; i < count; i++) {
      if (it.advance(docs[i]) != docs[i]) {
        return null;
      }
      ords[i] = it.index();
    }
    float[] vectors = new float[count * dim];
    if (flatReader.addRawVectors(fieldName, ords, count, vectors, batch) == false) {
      return null;
    }
    VectorSimilarityFunction fn =
        vectorSimilarityFunction != null
            ? vectorSimilarityFunction
            : ctx.reader().getFieldInfos().fieldInfo(fieldName).getVectorSimilarityFunction();
    return new Pending(vectors, dim, count, fn);
  }

  /** The reader holding this field's raw float32 vectors, or null if there is none. */
  private FlatVectorsReader rawVectorsReader(LeafReaderContext ctx) throws IOException {
    if (!(ctx.reader() instanceof CodecReader codecReader)) {
      return null;
    }
    KnnVectorsReader kr = codecReader.getVectorReader();
    if (kr != null) {
      kr = kr.unwrapReaderForField(fieldName);
    }
    // An HNSW reader keeps the raw vectors in its flat delegate; a flat reader holds them itself.
    if (kr instanceof Lucene99HnswVectorsReader hnsw) {
      return hnsw.getFlatVectorsReader();
    }
    return kr instanceof FlatVectorsReader fr ? fr : null;
  }

  @Override
  public boolean needsScores() {
    return false;
  }

  @Override
  public DoubleValuesSource rewrite(IndexSearcher reader) throws IOException {
    return this;
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldName, Arrays.hashCode(queryVector), vectorSimilarityFunction);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    FullPrecisionFloatVectorSimilarityValuesSource other =
        (FullPrecisionFloatVectorSimilarityValuesSource) obj;
    return Objects.equals(fieldName, other.fieldName)
        && Objects.equals(vectorSimilarityFunction, other.vectorSimilarityFunction)
        && Arrays.equals(queryVector, other.queryVector);
  }

  @Override
  public String toString() {
    return "FullPrecisionFloatVectorSimilarityValuesSource(fieldName="
        + fieldName
        + " vectorSimilarityFunction="
        + vectorSimilarityFunction.name()
        + " queryVector="
        + Arrays.toString(queryVector)
        + ")";
  }

  @Override
  public boolean isCacheable(LeafReaderContext ctx) {
    return true;
  }
}
