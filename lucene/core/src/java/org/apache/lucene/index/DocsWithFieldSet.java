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

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;

import java.util.Stack;

/**
 * Accumulator for documents that have a value for a field. This is optimized for the case that all
 * documents have a value.
 */
public final class DocsWithFieldSet extends DocIdSet {

  private static long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(DocsWithFieldSet.class);

  private FixedBitSet set;
  private int docsCount = 0;
  private int lastDocId = 0; // at a certain point in time this was changed to 0? why?
  
  private Stack<Integer> valuesPerDocuments;
  private int currentDocVectorsCount;
  private int vectorsCount;
  
  private boolean multiValued = false;

  /** Creates an empty DocsWithFieldSet. */
  public DocsWithFieldSet() {}

  /**
   * Add a document to the set
   *
   * @param docID – document ID to be added
   */
  public void add(int docID) {
    if (docID < lastDocId) {
      throw new IllegalArgumentException(
              "Out of order doc ids: last=" + lastDocId + ", next=" + docID);
    }
    if (set != null) {
      set = FixedBitSet.ensureCapacity(set, docID);
      set.set(docID);
    } else if (docID != docsCount) {
      // migrate to a sparse encoding using a bit set
      set = new FixedBitSet(docID + 1);
      set.set(0, docsCount);
      set.set(docID);
    }
    lastDocId = docID;
    docsCount++;
  }

  public void addMultiValue(int docID) {
    if (docID < lastDocId) {
      throw new IllegalArgumentException(
              "Out of order doc ids: last=" + lastDocId + ", next=" + docID);
    }
    if (set == null) { //first doc arrives
      valuesPerDocuments = new Stack<>();
      currentDocVectorsCount = 0;
      vectorsCount = 0;
      set = new FixedBitSet(docID + 1);
      set.set(0, docsCount);
      set.set(docID);
      docsCount++;
    } else {
      set = FixedBitSet.ensureCapacity(set, docID);
      if (!set.getAndSet(docID)) { 
        docsCount++; //this is the first vector for the docID
      } else {
        multiValued = true;
      }
    }
    if(docID != lastDocId){//vector for lastDocId are finished
      valuesPerDocuments.push(currentDocVectorsCount);
      currentDocVectorsCount = 0;
    }
    currentDocVectorsCount++;
    vectorsCount++;
    lastDocId = docID;
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + (set == null ? 0 : set.ramBytesUsed());
  }

  @Override
  public DocIdSetIterator iterator() {
    return set != null ? new BitSetIterator(set, docsCount) : DocIdSetIterator.all(docsCount);
  }

  /** Return the number of documents of this set. */
  public int cardinality() {
    return docsCount;
  }
  /** Return the number of vectors of this set. */
  public int getVectorsCount() {
    return vectorsCount;
  }

  public boolean isMultiValued() {
    return multiValued;
  }

  public int[] getValuesPerDocument() {
    if(valuesPerDocuments != null) {
      int[] valuesPerDocumentArray = new int[docsCount];
      if (currentDocVectorsCount != 0) {
        valuesPerDocuments.push(currentDocVectorsCount);
        currentDocVectorsCount = 0;
      }
      for (int i = valuesPerDocumentArray.length - 1; i > -1; i--) {
        valuesPerDocumentArray[i] = valuesPerDocuments.pop();
      }
      return valuesPerDocumentArray;
    } else {
      return null;
    }
  }
}
