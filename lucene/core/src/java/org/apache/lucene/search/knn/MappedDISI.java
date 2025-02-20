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
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.search.DocIdSetIterator;

/**
 * A {@link DocIdSetIterator} that maps the doc ids from a {@link TopDocsDISI} to the corresponding
 * vector ordinal
 *
 * @lucene.internal
 */
public sealed class MappedDISI extends DocIdSetIterator {
  KnnVectorValues.DocIndexIterator indexedDISI;
  DocIdSetIterator sourceDISI;

  /**
   * Create a new instance of a {@link MappedDISI}
   *
   * @param indexedDISI the mapping from doc to vector ordinal iterator
   * @param sourceDISI the source iterator
   * @return a new {@link MappedDISI}
   */
  public static MappedDISI from(
      KnnVectorValues.DocIndexIterator indexedDISI, TopDocsDISI sourceDISI) {
    if (sourceDISI instanceof TopDocsDISI.Scored scoredIterator) {
      return new Scored(indexedDISI, scoredIterator);
    }
    return new MappedDISI(indexedDISI, sourceDISI);
  }

  MappedDISI(KnnVectorValues.DocIndexIterator indexedDISI, TopDocsDISI sourceDISI) {
    this.indexedDISI = indexedDISI;
    this.sourceDISI = sourceDISI;
  }

  /**
   * Advances the source iterator to the first document number that is greater than or equal to the
   * provided target and returns the corresponding index.
   */
  @Override
  public int advance(int target) throws IOException {
    int newTarget = sourceDISI.advance(target);
    if (newTarget != NO_MORE_DOCS) {
      indexedDISI.advance(newTarget);
    }
    return docID();
  }

  @Override
  public long cost() {
    return sourceDISI.cost();
  }

  @Override
  public int docID() {
    if (indexedDISI.docID() == NO_MORE_DOCS || sourceDISI.docID() == NO_MORE_DOCS) {
      return NO_MORE_DOCS;
    }
    return indexedDISI.index();
  }

  /** Advances to the next document in the source iterator and returns the corresponding index. */
  @Override
  public int nextDoc() throws IOException {
    int newTarget = sourceDISI.nextDoc();
    if (newTarget != NO_MORE_DOCS) {
      indexedDISI.advance(newTarget);
    }
    return docID();
  }

  /** A {@link MappedDISI}, but where the source iterator can also provide scores */
  public static final class Scored extends MappedDISI {
    private final TopDocsDISI.Scored scoredIterator;

    Scored(KnnVectorValues.DocIndexIterator indexedDISI, TopDocsDISI.Scored scoredIterator) {
      super(indexedDISI, scoredIterator);
      this.scoredIterator = scoredIterator;
    }

    public float score() {
      return scoredIterator.score();
    }
  }
}
