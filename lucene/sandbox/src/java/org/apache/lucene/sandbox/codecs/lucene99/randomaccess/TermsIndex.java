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

package org.apache.lucene.sandbox.codecs.lucene99.randomaccess;

import java.io.IOException;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.OffHeapFSTStore;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;

record TermsIndex(FST<Long> fst) {

  TypeAndOrd getTerm(BytesRef term) throws IOException {
    long encoded = Util.get(fst, term);
    TermType termType = TermType.fromId((int) ((encoded & 0b1110L) >>> 1));
    long ord = encoded >>> 4;
    return new TypeAndOrd(termType, ord);
  }

  record TypeAndOrd(TermType termType, long ord) {}

  void serialize(DataOutput metaOut, DataOutput dataOut) throws IOException {
    fst.save(metaOut, dataOut);
  }

  static TermsIndex deserialize(DataInput metaIn, DataInput dataIn, boolean loadOffHeap)
      throws IOException {
    FST<Long> fst;
    if (loadOffHeap) {
      fst = new FST<>(metaIn, dataIn, PositiveIntOutputs.getSingleton(), new OffHeapFSTStore());
    } else {
      fst = new FST<>(metaIn, dataIn, PositiveIntOutputs.getSingleton());
    }
    return new TermsIndex(fst);
  }
}
