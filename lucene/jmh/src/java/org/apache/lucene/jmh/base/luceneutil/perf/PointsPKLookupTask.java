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
package org.apache.lucene.jmh.base.luceneutil.perf;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;

final class PointsPKLookupTask extends Task {

  private final int[] ids;
  private final int[] answers;
  private final int ord;

  @Override
  public String getCategory() {
    return "PointsPKLookup";
  }

  private PointsPKLookupTask(PointsPKLookupTask other) {
    ids = other.ids;
    ord = other.ord;
    answers = new int[ids.length];
    Arrays.fill(answers, -1);
  }

  public PointsPKLookupTask(int maxDoc, Random random, int count, Set<Integer> seen, int ord) {
    this.ord = ord;
    ids = new int[count];
    answers = new int[count];
    Arrays.fill(answers, -1);
    int idx = 0;
    while (idx < count) {
      final int id = random.nextInt(maxDoc);
      if (!seen.contains(id)) {
        seen.add(id);
        ids[idx++] = id;
      }
    }
  }

  @Override
  public Task clone() {
    return new PointsPKLookupTask(this);
  }

  @Override
  public void go(IndexState state) throws IOException {

    final IndexSearcher searcher = state.mgr.acquire();
    try {
      final List<LeafReaderContext> subReaders = searcher.getIndexReader().leaves();
      IndexState.PointsPKLookupState[] pkStates =
          new IndexState.PointsPKLookupState[subReaders.size()];
      for (int subIDX = 0; subIDX < subReaders.size(); subIDX++) {
        LeafReaderContext ctx = subReaders.get(subIDX);
        ThreadLocal<IndexState.PointsPKLookupState> states =
            state.pointsPKLookupStates.get(ctx.reader().getCoreCacheHelper().getKey());
        // NPE here means you are trying to use this task on a newly refreshed NRT reader!
        IndexState.PointsPKLookupState pkState = states.get();
        if (pkState == null) {
          pkState = new IndexState.PointsPKLookupState(ctx.reader(), "id");
          states.set(pkState);
        }
        pkStates[subIDX] = pkState;
      }
      for (int idx = 0; idx < ids.length; idx++) {
        /*
        int base = 0;
        final int id = ids[idx];
        for(int subIDX=0;subIDX<subReaders.size();subIDX++) {
          IndexState.PointsPKLookupState pkState = pkStates[subIDX];
          pkState.visitor.reset(id);
          pkState.bkdReader.intersect(pkState.state);
          if (pkState.visitor.answer != -1) {
            answers[idx] = base + pkState.visitor.answer;
            //System.out.println(id + " -> " + answers[idx]);
            break;
          }
          base += subReaders.get(subIDX).reader().maxDoc();
        }
        */

        // this approach works, uses public APIs, but is slowish:
        /*
        Query q = IntPoint.newExactQuery("id", ids[idx]);
        TopDocs hits = searcher.search(q, 1);
        if (hits.totalHits == 1) {
          answers[idx] = hits.scoreDocs[0].doc;
        }
        */
      }
    } finally {
      state.mgr.release(searcher);
    }
  }

  @Override
  public String toString() {
    return "PointsPK" + ord + "[" + ids.length + "]";
  }

  @Override
  public long checksum() {
    // TODO, but, not sure it makes sense since we will
    // run a different PK lookup each time...?
    return 0;
  }

  @Override
  public void printResults(PrintStream out, IndexState state) throws IOException {
    for (int idx = 0; idx < ids.length; idx++) {

      if (answers[idx] == -1) {
        if (state.hasDeletions == false) {
          throw new RuntimeException(
              "PointsPKLookup: idPoints=" + ids[idx] + " failed to find a matching document");
        } else {
          // TODO: we should verify that these are in fact
          // the deleted docs...
          continue;
        }
      }
    }
  }
}
