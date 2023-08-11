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
package org.apache.lucene.tests.search;

import java.io.IOException;
import org.apache.lucene.search.CheckedIntConsumer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocIdStream;
import org.apache.lucene.search.FilterLeafCollector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;

/** Wraps another Collector and checks that order is respected. */
class AssertingLeafCollector extends FilterLeafCollector {

  private final int min;
  private final int max;

  private Scorable scorer;
  private int lastCollected = -1;
  private boolean finishCalled;

  AssertingLeafCollector(LeafCollector collector, int min, int max) {
    super(collector);
    this.min = min;
    this.max = max;
  }

  @Override
  public void setScorer(Scorable scorer) throws IOException {
    this.scorer = scorer;
    super.setScorer(AssertingScorable.wrap(scorer));
  }

  @Override
  public void collect(DocIdStream stream) throws IOException {
    in.collect(new AssertingDocIdStream(stream));
  }

  @Override
  public void collect(int doc) throws IOException {
    assert doc > lastCollected : "Out of order : " + lastCollected + " " + doc;
    assert doc >= min : "Out of range: " + doc + " < " + min;
    assert doc < max : "Out of range: " + doc + " >= " + max;
    assert scorer.docID() == doc : "Collected: " + doc + " but scorer: " + scorer.docID();
    in.collect(doc);
    lastCollected = doc;
  }

  @Override
  public DocIdSetIterator competitiveIterator() throws IOException {
    final DocIdSetIterator in = this.in.competitiveIterator();
    if (in == null) {
      return null;
    }
    return new DocIdSetIterator() {

      @Override
      public int nextDoc() throws IOException {
        assert in.docID() < max
            : "advancing beyond the end of the scored window: docID=" + in.docID() + ", max=" + max;
        return in.nextDoc();
      }

      @Override
      public int docID() {
        return in.docID();
      }

      @Override
      public long cost() {
        return in.cost();
      }

      @Override
      public int advance(int target) throws IOException {
        assert target <= max
            : "advancing beyond the end of the scored window: target=" + target + ", max=" + max;
        return in.advance(target);
      }
    };
  }

  @Override
  public void finish() throws IOException {
    assert finishCalled == false;
    finishCalled = true;
    super.finish();
  }

  private class AssertingDocIdStream extends DocIdStream {

    private final DocIdStream stream;
    private boolean consumed;

    AssertingDocIdStream(DocIdStream stream) {
      this.stream = stream;
    }

    @Override
    public void forEach(CheckedIntConsumer<IOException> consumer) throws IOException {
      assert consumed == false : "A terminal operation has already been called";
      stream.forEach(
          doc -> {
            assert doc > lastCollected : "Out of order : " + lastCollected + " " + doc;
            assert doc >= min : "Out of range: " + doc + " < " + min;
            assert doc < max : "Out of range: " + doc + " >= " + max;
            consumer.accept(doc);
            lastCollected = doc;
          });
      consumed = true;
    }

    @Override
    public int count() throws IOException {
      assert consumed == false : "A terminal operation has already been called";
      int count = stream.count();
      consumed = true;
      return count;
    }
  }
}
