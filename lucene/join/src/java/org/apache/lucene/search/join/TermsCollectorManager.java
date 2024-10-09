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
package org.apache.lucene.search.join;

import java.io.IOException;
import java.util.Collection;
import java.util.function.Supplier;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;

/**
 * A collector that collects all terms from a specified field matching the query.
 *
 * @lucene.experimental
 */
class TermsCollectorManager<C extends TermsCollectorManager.TermsCollector<DV>, DV>
    implements CollectorManager<C, GenericTermsResult> {
  private final Supplier<C> collectorSupplier;

  public TermsCollectorManager(Supplier<C> collectorSupplier) {
    this.collectorSupplier = collectorSupplier;
  }

  @Override
  public C newCollector() {
    return collectorSupplier.get();
  }

  @Override
  public GenericTermsResult reduce(Collection<C> collectors) throws IOException {
    BytesRefHash allTerms = new BytesRefHash();
    for (C collector : collectors) {
      int[] ids = collector.collectedTerms.compact();
      BytesRef scratch = new BytesRef();
      for (int i = 0; i < collector.collectedTerms.size(); i++) {
        allTerms.add(collector.collectedTerms.get(ids[i], scratch));
      }
    }
    return new GenericTermsResult() {
      @Override
      public BytesRefHash getCollectedTerms() {
        return allTerms;
      }

      @Override
      public float[] getScoresPerTerm() {
        throw new UnsupportedOperationException("scores are not available for " + this);
      }
    };
  }

  abstract static class TermsCollector<DV> extends DocValuesTermsCollector<DV> {

    TermsCollector(Function<DV> docValuesCall) {
      super(docValuesCall);
    }

    final BytesRefHash collectedTerms = new BytesRefHash();

    // impl that works with multiple values per document
    static class MV extends TermsCollector<SortedSetDocValues> {

      MV(Function<SortedSetDocValues> docValuesCall) {
        super(docValuesCall);
      }

      @Override
      public void collect(int doc) throws IOException {
        if (doc > docValues.docID()) {
          docValues.advance(doc);
        }
        if (doc == docValues.docID()) {
          for (int i = 0; i < docValues.docValueCount(); i++) {
            final BytesRef term = docValues.lookupOrd(docValues.nextOrd());
            collectedTerms.add(term);
          }
        }
      }
    }

    // impl that works with single value per document
    static class SV extends TermsCollector<SortedDocValues> {

      SV(Function<SortedDocValues> docValuesCall) {
        super(docValuesCall);
      }

      @Override
      public void collect(int doc) throws IOException {
        BytesRef term;
        if (docValues.advanceExact(doc)) {
          term = docValues.lookupOrd(docValues.ordValue());
        } else {
          term = new BytesRef(BytesRef.EMPTY_BYTES);
        }
        collectedTerms.add(term);
      }
    }

    @Override
    public org.apache.lucene.search.ScoreMode scoreMode() {
      return org.apache.lucene.search.ScoreMode.COMPLETE_NO_SCORES;
    }
  }
}
