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

import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;

import java.util.Stack;

/**
 * Accumulator for documents that have a value for a field. This is optimized for the case that all
 * documents have a value.
 */
public final class DocsWithVectorsSet extends DocsWithFieldSet {

  protected static long BASE_RAM_BYTES_USED =
          RamUsageEstimator.shallowSizeOfInstance(DocsWithVectorsSet.class);

  protected Stack<Integer> vectorsPerDocument;
  protected int currentDocVectorsCount;
  protected int vectorsCount;

  protected boolean multiValued;

  /** Creates an empty DocsWithFieldSet. */
  public DocsWithVectorsSet() {
    super();
    multiValued = false;
  }

  /**
   * Add a document to the set
   *
   * @param docID – document ID to be added
   */
  @Override
  public void add(int docID) {
    if (multiValued || docID == lastDocId) {
      if (docID < lastDocId) {
        throw new IllegalArgumentException(
                "Out of order doc ids: last=" + lastDocId + ", next=" + docID);
      }
      if (!multiValued) {
        vectorsPerDocument = new Stack<>();
        for (int vector = 0; vector < cardinality-1; vector++) {
          vectorsPerDocument.push(1);
        }
        currentDocVectorsCount = 1;
        multiValued = true;
      }
      if (set == null) {
        set = new FixedBitSet(docID + 1);
        set.set(0, cardinality);
        set.set(docID);
      } else {
        set = FixedBitSet.ensureCapacity(set, docID);
        if (!set.getAndSet(docID)) {
          cardinality++; //this is the first vector for the docID
        }
      }
      if (docID != lastDocId) {//vector for lastDocId are finished
        vectorsPerDocument.push(currentDocVectorsCount);
        currentDocVectorsCount = 0;
      }
      currentDocVectorsCount++;
      lastDocId = docID;
    } else {
      super.add(docID);
    }
    vectorsCount++;
  }


  /** Return the number of vectors of this set. */
  public int getVectorsCount() {
    return vectorsCount;
  }

  public boolean isMultiValued() {
    return multiValued;
  }

  public int[] getVectorsPerDocument() {
    if(vectorsPerDocument != null) {
      int[] valuesPerDocumentArray = new int[cardinality];
      if (currentDocVectorsCount != 0) {
        vectorsPerDocument.push(currentDocVectorsCount);
        currentDocVectorsCount = 0;
      }
      for (int i = valuesPerDocumentArray.length - 1; i > -1; i--) {
        valuesPerDocumentArray[i] = vectorsPerDocument.pop();
      }
      return valuesPerDocumentArray;
    } else {
      return null;
    }
  }
}
