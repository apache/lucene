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

package org.apache.lucene.util.hnsw;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;

/**
 * KnnResults is a specific NeighborQueue, enforcing a minHeap is utilized for results.
 *
 * <p>This way as better results are found, the minimum result can be easily removed from the
 * collection
 */
public abstract class KnnResults extends NeighborQueue {

  /** KnnResults when exiting search early and returning empty top docs */
  public static class EmptyKnnResults extends KnnResults {
    public EmptyKnnResults() {
      super(1);
      markIncomplete();
    }

    @Override
    public void popWhileFull() {
      throw new IllegalArgumentException("cannot pop empty knn results");
    }

    @Override
    public boolean isFull() {
      return true;
    }

    @Override
    protected void doClear() {}

    @Override
    public TopDocs topDocs() {
      TotalHits th = new TotalHits(visitedCount(), TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
      return new TopDocs(th, new ScoreDoc[0]);
    }
  }

  public KnnResults(int initialSize) {
    super(initialSize, false);
  }

  @Override
  public final void clear() {
    super.clear();
    doClear();
  }

  public abstract void popWhileFull();

  public abstract boolean isFull();

  protected abstract void doClear();

  public abstract TopDocs topDocs();

  @Override
  public String toString() {
    return "KnnResults[" + size() + "]";
  }
}
