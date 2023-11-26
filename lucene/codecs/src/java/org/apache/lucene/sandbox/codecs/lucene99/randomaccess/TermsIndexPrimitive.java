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

import static org.apache.lucene.sandbox.codecs.lucene99.randomaccess.TermsIndex.decodeLong;

import java.io.IOException;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.fst.OffHeapFSTStore;
import org.apache.lucene.util.fst.PrimitiveLongFST;

record TermsIndexPrimitive(PrimitiveLongFST primitiveLongFST) {

  TermsIndex.TypeAndOrd getTerm(BytesRef term) throws IOException {
    long encoded = PrimitiveLongFST.get(primitiveLongFST, term);
    return decodeLong(encoded);
  }

  static TermsIndexPrimitive deserialize(DataInput metaIn, DataInput dataIn, boolean loadOffHeap)
      throws IOException {
    PrimitiveLongFST fst;
    if (loadOffHeap) {
      var fstStore = new OffHeapFSTStore();
      fst =
          new PrimitiveLongFST(
              PrimitiveLongFST.readMetadata(
                  metaIn, PrimitiveLongFST.PrimitiveLongFSTOutputs.getSingleton()),
              dataIn.clone(),
              fstStore);
      dataIn.skipBytes(fstStore.size());
    } else {
      fst =
          new PrimitiveLongFST(
              PrimitiveLongFST.readMetadata(
                  metaIn, PrimitiveLongFST.PrimitiveLongFSTOutputs.getSingleton()),
              dataIn);
    }
    return new TermsIndexPrimitive(fst);
  }
}
