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

package org.apache.lucene.codecs.hnsw;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.IntPredicate;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.util.IORunnable;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;

/**
 * Vectors' writer for a field that allows additional indexing logic to be implemented by the caller
 *
 * @lucene.experimental
 */
public abstract class FlatVectorsWriter extends KnnVectorsWriter {
  /** Scorer for flat vectors */
  protected final FlatVectorsScorer vectorsScorer;

  /** Sole constructor */
  protected FlatVectorsWriter(FlatVectorsScorer vectorsScorer) {
    this.vectorsScorer = vectorsScorer;
  }

  /**
   * @return the {@link FlatVectorsScorer} for this reader.
   */
  public FlatVectorsScorer getFlatVectorScorer() {
    return vectorsScorer;
  }

  /**
   * Add a new field for indexing
   *
   * @param fieldInfo fieldInfo of the field to add
   * @return a writer for the field
   * @throws IOException if an I/O error occurs when adding the field
   */
  @Override
  public abstract FlatFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException;

  @Override
  public final IORunnable mergeOneField(FieldInfo fieldInfo, MergeState mergeState)
      throws IOException {
    mergeOneFlatVectorField(fieldInfo, mergeState);
    return null;
  }

  public abstract void mergeOneFlatVectorField(FieldInfo fieldInfo, MergeState mergeState)
      throws IOException;

  /**
   * Merges one field and, while writing the merged vectors, optionally prepares what a merge scorer
   * needs, so that the merged float vectors need not be read back.
   *
   * <p>A wrapping writer should forward this method along with {@link #mergeOneFlatVectorField}.
   * Forwarding only the latter stays correct, but the wrapped writer then prepares nothing.
   * Forwarding helps only when the wrapping format opens the wrapped writer's own reader over the
   * merged segment. Otherwise {@link MergeScorerData#scorerSupplier} rejects the prepared data and
   * the merge uses its fallback.
   *
   * @param fieldInfo field to merge
   * @param mergeState merge state
   * @param needsMergeScorer whether to prepare scorer data for a vector count. The writer tests
   *     this predicate against the count before deletions, which can exceed the number of vectors
   *     written.
   * @return prepared data that the caller must close, or {@code null} if none was prepared, which
   *     is what the default implementation returns and what any writer returns for a field it
   *     cannot prepare for. The caller then builds the scorer itself.
   * @throws IOException if an I/O error occurs
   */
  public MergeScorerData mergeOneFlatVectorFieldForMergeScorer(
      FieldInfo fieldInfo, MergeState mergeState, IntPredicate needsMergeScorer)
      throws IOException {
    mergeOneFlatVectorField(fieldInfo, mergeState);
    return null;
  }

  /**
   * Data prepared while merging a field for scoring its merged vectors. The caller must close it
   * whether or not it obtains a supplier; closing releases only what {@link #scorerSupplier} did
   * not hand over. A single merge thread uses each instance, so implementations need not be
   * thread-safe.
   */
  public interface MergeScorerData extends Closeable {

    /**
     * Creates a scorer supplier for {@code mergedReader}. The caller must close the supplier to
     * release the prepared data.
     *
     * <p>The reader is matched to the preparing writer's own reader by type. A reader that wraps
     * that reader is rejected even when it reads the correct segment.
     *
     * <p>This method may be called at most once, after the preparing writer is finished and closed
     * and {@code mergedReader} is open on the segment it wrote. A second call, or a call after
     * {@link #close}, throws {@link IllegalStateException}.
     *
     * @param mergedReader a reader over the segment the writer wrote
     * @return a scorer supplier over the merged vectors, or {@code null} if this data cannot serve
     *     that reader
     * @throws IOException if an I/O error occurs
     */
    CloseableRandomVectorScorerSupplier scorerSupplier(FlatVectorsReader mergedReader)
        throws IOException;
  }
}
