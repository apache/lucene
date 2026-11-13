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

import org.apache.lucene.index.TermState;

/**
 * The state of a term resolved by the hash table: its only document. It needs no I/O to restore,
 * and is valid on any {@link HashTermsEnum} of the same field and segment; terms that the delegate
 * resolved carry the delegate's {@link TermState}.
 */
final class HashTermState extends TermState {

  int docID;

  HashTermState(int docID) {
    this.docID = docID;
  }

  @Override
  public void copyFrom(TermState other) {
    this.docID = ((HashTermState) other).docID;
  }

  @Override
  public String toString() {
    return "HashTermState(docID=" + docID + ")";
  }
}
