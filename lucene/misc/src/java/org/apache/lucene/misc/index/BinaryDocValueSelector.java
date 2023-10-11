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

package org.apache.lucene.misc.index;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

/**
 * Use this selector to rearrange an index where documents can be uniquely identified based on
 * {@link BinaryDocValues}
 */
public class BinaryDocValueSelector implements IndexRearranger.DocumentSelector, Serializable {

  private final String field;
  private final Set<BytesRef> keySet;

  public BinaryDocValueSelector(String field, Set<BytesRef> keySet) {
    this.field = field;
    this.keySet = keySet;
  }

  @Override
  public BitSet getFilteredDocs(CodecReader reader) throws IOException {
    BinaryDocValues binaryDocValues = reader.getBinaryDocValues(field);
    FixedBitSet bits = new FixedBitSet(reader.maxDoc());
    for (int docid = 0; docid < reader.maxDoc(); docid++) {
      if (binaryDocValues.advanceExact(docid) && keySet.contains(binaryDocValues.binaryValue())) {
        bits.set(docid);
      }
    }
    return bits;
  }

  /**
   * Create a selector for the deletes in an index, which can then be applied to a rearranged index
   *
   * @param field tells which {@link BinaryDocValues} are the unique key
   * @param directory where the original index is present
   * @return a deletes selector to be passed to {@link IndexRearranger}
   */
  public static IndexRearranger.DocumentSelector createDeleteSelectorFromIndex(
      String field, Directory directory) throws IOException {

    Set<BytesRef> keySet = new HashSet<>();

    try (IndexReader reader = DirectoryReader.open(directory)) {
      for (LeafReaderContext context : reader.leaves()) {
        Bits liveDocs = context.reader().getLiveDocs();
        if (liveDocs == null) {
          continue;
        }

        BinaryDocValues binaryDocValues = context.reader().getBinaryDocValues(field);
        for (int docid = 0; docid < context.reader().maxDoc(); docid++) {
          if (liveDocs.get(docid) == true) {
            continue;
          }

          if (binaryDocValues.advanceExact(docid)) {
            keySet.add(BytesRef.deepCopyOf(binaryDocValues.binaryValue()));
          } else {
            throw new AssertionError("Document " + docid + " doesn't have key " + field);
          }
        }
      }
    }
    return new BinaryDocValueSelector(field, keySet);
  }

  /**
   * Create a list of selectors that will reproduce the index geometry when used with {@link
   * IndexRearranger}
   *
   * @param field tells which {@link BinaryDocValues} are the unique key
   * @param directory where the original index is present
   * @return a list of selectors to be passed to {@link IndexRearranger}
   */
  public static List<IndexRearranger.DocumentSelector> createLiveSelectorsFromIndex(
      String field, Directory directory) throws IOException {

    List<IndexRearranger.DocumentSelector> selectors = new ArrayList<>();

    try (IndexReader reader = DirectoryReader.open(directory)) {
      for (LeafReaderContext context : reader.leaves()) {
        Set<BytesRef> keySet = new HashSet<>();
        BinaryDocValues binaryDocValues = context.reader().getBinaryDocValues(field);

        for (int docid = 0; docid < context.reader().maxDoc(); docid++) {
          if (binaryDocValues.advanceExact(docid)) {
            keySet.add(BytesRef.deepCopyOf(binaryDocValues.binaryValue()));
          } else {
            throw new AssertionError("Document " + docid + " doesn't have key " + field);
          }
        }
        selectors.add(new BinaryDocValueSelector(field, keySet));
      }
    }
    return selectors;
  }
}
