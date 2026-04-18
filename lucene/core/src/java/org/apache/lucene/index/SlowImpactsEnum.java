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
package org.apache.lucene.index;

import java.io.IOException;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

/**
 * {@link ImpactsEnum} that doesn't index impacts but implements the API in a legal way. This is
 * typically used for short postings that do not need skipping.
 */
public final class SlowImpactsEnum extends ImpactsEnum {

  private final PostingsEnum delegate;

  /** Wrap the given {@link PostingsEnum}. */
  public SlowImpactsEnum(PostingsEnum delegate) {
    this.delegate = delegate;
  }

  @Override
  public int nextDoc() throws IOException {
    return delegate.nextDoc();
  }

  @Override
  public int docID() {
    return delegate.docID();
  }

  @Override
  public long cost() {
    return delegate.cost();
  }

  @Override
  public int advance(int target) throws IOException {
    return delegate.advance(target);
  }

  @Override
  public int startOffset() throws IOException {
    return delegate.startOffset();
  }

  @Override
  public int nextPosition() throws IOException {
    return delegate.nextPosition();
  }

  @Override
  public BytesRef getPayload() throws IOException {
    return delegate.getPayload();
  }

  @Override
  public int freq() throws IOException {
    return delegate.freq();
  }

  @Override
  public int endOffset() throws IOException {
    return delegate.endOffset();
  }

  @Override
  public void advanceShallow(int target) {}

  @Override
  public Impacts getImpacts() {
    return new Impacts() {

      private final FreqAndNormBuffer impacts = new FreqAndNormBuffer();

      {
        impacts.growNoCopy(1);
      }

      @Override
      public int numLevels() {
        return 1;
      }

      @Override
      public int getDocIdUpTo(int level) {
        return DocIdSetIterator.NO_MORE_DOCS;
      }

      @Override
      public FreqAndNormBuffer getImpacts(int level) {
        impacts.freqs[0] = Integer.MAX_VALUE;
        impacts.norms[0] = 1L;
        impacts.size = 1;
        return impacts;
      }
    };
  }
}
