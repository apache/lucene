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
package org.apache.lucene.search.knn;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TopDocs;

/**
 * A {@link DocIdSetIterator} that wraps a {@link TopDocs} object.
 *
 * @lucene.internal
 */
public sealed class TopDocsDISI extends DocIdSetIterator {

  private final int[] sortedDocIds;
  protected int idx = -1;

  /**
   * Creates a {@link TopDocsDISI} from a {@link TopDocs} object. The resulting docs account for the
   * provided leaf context base
   *
   * @param topDocs the TopDocs to wrap
   * @param ctx the leaf for the top docs
   * @return new iterator over the top docs
   */
  public static TopDocsDISI fromTopDocs(TopDocs topDocs, LeafReaderContext ctx) {
    int[] sortedDocIds = new int[topDocs.scoreDocs.length];
    for (int i = 0; i < topDocs.scoreDocs.length; i++) {
      assert topDocs.scoreDocs[i].doc >= ctx.docBase;
      // Remove the doc base as added by the collector
      sortedDocIds[i] = topDocs.scoreDocs[i].doc - ctx.docBase;
    }
    Arrays.sort(sortedDocIds);
    return new TopDocsDISI(sortedDocIds);
  }

  /**
   * Creates a {@link TopDocsDISI.Scored} from a {@link TopDocs} object. This allows callers to
   * potentially request the underlying original score
   *
   * @param topDocs the TopDocs to wrap
   * @param ctx the leaf for the top docs
   * @return new iterator over the top docs
   */
  public static TopDocsDISI.Scored fromTopDocsWithScores(TopDocs topDocs, LeafReaderContext ctx) {
    // now sort both `scores` and `docs` within `TopDocs` according to doc id
    int[] sortedDocIds = new int[topDocs.scoreDocs.length];
    float[] sortedScores = new float[topDocs.scoreDocs.length];
    Integer[] sortedIndices = new Integer[topDocs.scoreDocs.length];
    for (int i = 0; i < topDocs.scoreDocs.length; i++) {
      sortedIndices[i] = i;
    }
    Arrays.sort(sortedIndices, Comparator.comparingInt(a -> topDocs.scoreDocs[a].doc));
    for (int i = 0; i < topDocs.scoreDocs.length; i++) {
      assert topDocs.scoreDocs[sortedIndices[i]].doc >= ctx.docBase;
      sortedScores[i] = topDocs.scoreDocs[sortedIndices[i]].score;
      sortedDocIds[i] = topDocs.scoreDocs[sortedIndices[i]].doc - ctx.docBase;
    }
    return new Scored(sortedDocIds, sortedScores);
  }

  TopDocsDISI(int[] sortedDocIds) {
    this.sortedDocIds = sortedDocIds;
  }

  @Override
  public int advance(int target) throws IOException {
    return slowAdvance(target);
  }

  @Override
  public long cost() {
    return sortedDocIds.length;
  }

  @Override
  public int docID() {
    if (idx == -1) {
      return -1;
    } else if (idx >= sortedDocIds.length) {
      return DocIdSetIterator.NO_MORE_DOCS;
    } else {
      return sortedDocIds[idx];
    }
  }

  @Override
  public int nextDoc() {
    idx += 1;
    return docID();
  }

  /** A {@link TopDocsDISI} but can also provide scores */
  public static final class Scored extends TopDocsDISI {
    private final float[] scores;

    Scored(int[] sortedDocIds, float[] scores) {
      super(sortedDocIds);
      this.scores = scores;
    }

    public float score() {
      return scores[idx];
    }
  }
}
