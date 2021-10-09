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

package org.apache.lucene.jmh.base.luceneutil.perf;

import java.io.IOException;
import java.util.Set;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.misc.index.IndexRearranger;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

/** The type String field doc selector. */
public class StringFieldDocSelector implements IndexRearranger.DocumentSelector {

  private final String field;
  private final Set<String> keySet;

  /**
   * Instantiates a new String field doc selector.
   *
   * @param field the field
   * @param keySet the key set
   */
  StringFieldDocSelector(String field, Set<String> keySet) {
    this.field = field;
    this.keySet = keySet;
  }

  @Override
  public BitSet getFilteredLiveDocs(CodecReader reader) throws IOException {
    TermsEnum termsEnum = reader.terms(field).iterator();
    Bits oldLiveDocs = reader.getLiveDocs();
    FixedBitSet bits = new FixedBitSet(reader.maxDoc());
    PostingsEnum reuse = null;
    for (String key : keySet) {
      if (termsEnum.seekExact(new BytesRef(key))) {
        reuse = termsEnum.postings(reuse);
        assert reuse != null;
        while (reuse.docID() != DocIdSetIterator.NO_MORE_DOCS) {
          if (reuse.docID() != -1 && (oldLiveDocs == null || oldLiveDocs.get(reuse.docID()))) {
            bits.set(reuse.docID());
          }
          reuse.nextDoc();
        }
      }
    }
    return bits;
  }
}
