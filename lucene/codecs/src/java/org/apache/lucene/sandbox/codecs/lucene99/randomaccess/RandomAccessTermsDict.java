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
import org.apache.lucene.codecs.lucene99.Lucene99PostingsFormat.IntBlockTermState;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;

/** A term dictionary that offer random-access to read a specific term */
record RandomAccessTermsDict(
    TermsStats termsStats,
    TermsIndexPrimitive termsIndex,
    TermDataReaderProvider termDataReaderProvider,
    IndexOptions indexOptions) {

  /** test only * */
  IntBlockTermState getTermState(BytesRef term) throws IOException {
    TermsIndex.TypeAndOrd typeAndOrd = termsIndex.getTerm(term);
    return termDataReaderProvider
        .newReader()
        .getTermState(typeAndOrd.termType(), typeAndOrd.ord(), indexOptions);
  }

  static RandomAccessTermsDict deserialize(
      IndexOptionsProvider indexOptionsProvider,
      DataInput metaInput,
      DataInput termIndexInput,
      TermDataInputProvider termDataInputProvider)
      throws IOException {

    // (1) deserialize field stats
    TermsStats termsStats = TermsStats.deserialize(metaInput);
    IndexOptions indexOptions = indexOptionsProvider.getIndexOptions(termsStats.fieldNumber());
    boolean hasPayloads = indexOptionsProvider.hasPayloads(termsStats.fieldNumber());

    // (2) deserialize terms index
    TermsIndexPrimitive termsIndex = null;
    if (termsStats.size() > 0) {
      termsIndex =
          TermsIndexPrimitive.deserialize(metaInput, termIndexInput, /* load off heap */ true);
    }

    // (3) deserialize all the term data by each TermType
    // (3.1) number of unique TermType this field has
    int numTermTypes = metaInput.readByte();

    // (3.2) read per TermType
    TermDataReaderProvider.Builder termDataReaderBuilder =
        new TermDataReaderProvider.Builder(indexOptions, hasPayloads);
    for (int i = 0; i < numTermTypes; i++) {
      TermType termType = TermType.fromId(metaInput.readByte());
      TermDataInput termDataInput = termDataInputProvider.getTermDataInputForType(termType);
      termDataReaderBuilder.readOne(
          termType, metaInput, termDataInput.metadataInput, termDataInput.dataInput);
    }

    return new RandomAccessTermsDict(
        termsStats, termsIndex, termDataReaderBuilder.build(), indexOptions);
  }

  interface IndexOptionsProvider {

    IndexOptions getIndexOptions(int fieldNumber);

    boolean hasPayloads(int fieldNumber);
  }

  record TermDataInput(IndexInput metadataInput, IndexInput dataInput) {}

  @FunctionalInterface
  interface TermDataInputProvider {

    TermDataInput getTermDataInputForType(TermType termType) throws IOException;
  }
}
