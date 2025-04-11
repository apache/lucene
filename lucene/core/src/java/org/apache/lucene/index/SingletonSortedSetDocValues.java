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
 * Exposes multi-valued iterator view over a single-valued iterator.
 *
 * <p>This can be used if you want to have one multi-valued implementation that works for single or
 * multi-valued types.
 */
final class SingletonSortedSetDocValues extends SortedSetDocValues {
  private final SortedDocValues in;

  /** Creates a multi-valued view over the provided SortedDocValues */
  public SingletonSortedSetDocValues(SortedDocValues in) {
    if (in.docID() != -1) {
      throw new IllegalStateException("iterator has already been used: docID=" + in.docID());
    }
    this.in = in;
  }

  /** Return the wrapped {@link SortedDocValues} */
  public SortedDocValues getSortedDocValues() {
    if (in.docID() != -1) {
      throw new IllegalStateException("iterator has already been used: docID=" + in.docID());
    }
    return in;
  }

  @Override
  public DocIdSetIterator iterator() {
    return in.iterator();
  }

  @Override
  public long nextOrd() throws IOException {
    return in.ordValue();
  }

  @Override
  public int docValueCount() {
    return 1;
  }

  @Override
  public boolean advanceExact(int target) throws IOException {
    return in.advanceExact(target);
  }

  @Override
  public BytesRef lookupOrd(long ord) throws IOException {
    // cast is ok: single-valued cannot exceed Integer.MAX_VALUE
    return in.lookupOrd((int) ord);
  }

  @Override
  public long getValueCount() {
    return in.getValueCount();
  }

  @Override
  public long lookupTerm(BytesRef key) throws IOException {
    return in.lookupTerm(key);
  }

  @Override
  public TermsEnum termsEnum() throws IOException {
    return in.termsEnum();
  }
}
