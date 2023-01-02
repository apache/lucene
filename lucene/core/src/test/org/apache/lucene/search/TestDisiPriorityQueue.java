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

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.PrimitiveIterator.OfInt;
import java.util.Random;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestDisiPriorityQueue extends LuceneTestCase {

  public void testRandom() throws Exception {
    Random r = random();

    int size = RandomNumbers.randomIntBetween(random(), 1, TEST_NIGHTLY ? 1000 : 10);
    DisiWrapper[] all = new DisiWrapper[size];
    for (int i = 0; i < size; i++) {
      DocIdSetIterator it = randomDisi(r);
      DisiWrapper w = wrapper(it);
      all[i] = w;
    }

    DisiPriorityQueue pq = new DisiPriorityQueue(size);
    if (r.nextBoolean()) {
      for (DisiWrapper w : all) {
        pq.add(w);
      }
    } else {
      if (r.nextInt(10) < 2 && size > 1) {
        int len = RandomNumbers.randomIntBetween(random(), 1, size);
        for (int i = 0; i < len; i++) {
          pq.add(all[i]);
        }
        pq.addAll(all, len, size - len);
      } else {
        pq.addAll(all, 0, size);
      }
    }

    while (pq.size() > 0) {
      Arrays.sort(all, Comparator.comparingInt(w -> w.doc));
      DisiWrapper top = pq.top();
      assertEquals(all[0].doc, top.doc);
      top.doc = top.iterator.nextDoc();
      if (top.doc == DocIdSetIterator.NO_MORE_DOCS) {
        pq.pop();
      } else {
        pq.updateTop();
      }
    }
  }

  private static DisiWrapper wrapper(DocIdSetIterator iterator) throws IOException {
    Query q = new DummyQuery(iterator);
    Scorer s = q.createWeight(null, ScoreMode.COMPLETE_NO_SCORES, 1.0f).scorer(null);
    return new DisiWrapper(s);
  }

  private static DocIdSetIterator randomDisi(Random r) {
    int maxSize = r.nextInt(50);
    OfInt randomInts =
        r.ints(maxSize, 0, DocIdSetIterator.NO_MORE_DOCS - 1).sorted().distinct().iterator();
    return new DocIdSetIterator() {
      private int doc = -1;

      @Override
      public int docID() {
        return doc;
      }

      @Override
      public int nextDoc() {
        if (randomInts.hasNext()) {
          return doc = randomInts.nextInt();
        } else {
          return doc = DocIdSetIterator.NO_MORE_DOCS;
        }
      }

      @Override
      public int advance(int target) {
        while (doc < target) {
          nextDoc();
        }
        return doc;
      }

      @Override
      public long cost() {
        return maxSize;
      }
    };
  }

  private static class DummyQuery extends Query {
    private static int COUNTER = 0;
    private final int id;
    private final DocIdSetIterator disi;

    DummyQuery(DocIdSetIterator disi) {
      id = COUNTER++;
      this.disi = disi;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
        throws IOException {
      return new ConstantScoreWeight(this, boost) {
        @Override
        public Scorer scorer(LeafReaderContext context) {
          return new ConstantScoreScorer(this, score(), scoreMode, disi);
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
          return true;
        }
      };
    }

    @Override
    public String toString(String field) {
      return "DummyQuery (" + id + ")";
    }

    @Override
    public void visit(QueryVisitor visitor) {}

    @Override
    public boolean equals(Object other) {
      return sameClassAs(other) && id == ((DummyQuery) other).id;
    }

    @Override
    public int hashCode() {
      return id;
    }
  }
}
