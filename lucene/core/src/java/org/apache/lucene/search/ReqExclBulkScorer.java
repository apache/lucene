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
import org.apache.lucene.util.Bits;

final class ReqExclBulkScorer extends BulkScorer {

  private final BulkScorer req;
  private final DocIdSetIterator exclApproximation;
  private final TwoPhaseIterator exclTwoPhase;

  ReqExclBulkScorer(BulkScorer req, Scorer excl) {
    this.req = req;
    this.exclTwoPhase = excl.twoPhaseIterator();
    if (exclTwoPhase != null) {
      this.exclApproximation = exclTwoPhase.approximation();
    } else {
      this.exclApproximation = excl.iterator();
    }
  }

  ReqExclBulkScorer(BulkScorer req, DocIdSetIterator excl) {
    this.req = req;
    this.exclTwoPhase = null;
    this.exclApproximation = excl;
  }

  ReqExclBulkScorer(BulkScorer req, TwoPhaseIterator excl) {
    this.req = req;
    this.exclTwoPhase = excl;
    this.exclApproximation = excl.approximation();
  }

  @Override
  public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
    int upTo = min;
    int exclDoc = exclApproximation.docID();

    while (upTo < max) {
      if (exclDoc < upTo) {
        exclDoc = exclApproximation.advance(upTo);
      }
      if (exclDoc == upTo) {
        if (exclTwoPhase == null) {
          // from upTo to docIdRunEnd() are excluded, so we scored up to docIdRunEnd()
          upTo = Math.min(exclApproximation.docIDRunEnd(), max);
        } else if (exclTwoPhase.matches()) {
          // upTo is excluded so we can consider that we scored up to upTo+1
          upTo += 1;
        }
        exclDoc = exclApproximation.nextDoc();
      } else {
        upTo = req.score(collector, acceptDocs, upTo, Math.min(exclDoc, max));
      }
    }

    if (upTo == max) {
      upTo = req.score(collector, acceptDocs, upTo, upTo);
    }

    return upTo;
  }

  @Override
  public long cost() {
    return req.cost();
  }
}
