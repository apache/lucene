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
package org.apache.lucene.sandbox.codecs.pkhash;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.util.BytesRef;

/**
 * Postings of a term that occurs in exactly one document of a {@code DOCS}-only field, as resolved
 * by the hash table. Frequencies are 1 and there are no positions, offsets or payloads, which is
 * what the delegate returns for such a field whatever flags were asked for.
 *
 * <p>Holds no state besides the document (and an enum of the delegate kept for reuse, which the
 * delegate checks before reusing it), so an instance may be reused for any term of any field or
 * segment.
 */
final class SingleDocPostingsEnum extends PostingsEnum {

  private int singleDoc;
  private int doc;

  /**
   * A postings enum of the delegate format that was given up for reuse when this enum was created,
   * kept for when this one is given back for reuse on a key that the delegate resolves. Only used
   * by {@link HashTermsEnum#postings}.
   */
  PostingsEnum delegateReuse;

  SingleDocPostingsEnum reset(int singleDoc) {
    assert singleDoc >= 0;
    this.singleDoc = singleDoc;
    this.doc = -1;
    return this;
  }

  @Override
  public int docID() {
    return doc;
  }

  @Override
  public int nextDoc() {
    doc = doc == -1 ? singleDoc : NO_MORE_DOCS;
    return doc;
  }

  @Override
  public int advance(int target) {
    doc = doc == -1 && target <= singleDoc ? singleDoc : NO_MORE_DOCS;
    return doc;
  }

  @Override
  public long cost() {
    return 1;
  }

  @Override
  public int freq() {
    return 1;
  }

  @Override
  public int nextPosition() {
    return -1;
  }

  @Override
  public int startOffset() {
    return -1;
  }

  @Override
  public int endOffset() {
    return -1;
  }

  @Override
  public BytesRef getPayload() {
    return null;
  }
}
