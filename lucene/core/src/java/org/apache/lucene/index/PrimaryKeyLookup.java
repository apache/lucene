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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.IndexReader.CacheHelper;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/**
 * Class to do efficient primary-key lookups. Instances are not thread-safe. Each calling thread must own its own
 * instance, and an instance must only ever be used from the thread that constructed it. When assertions are
 * enabled this invariant is checked on every lookup and reopen call. Do not use this if a term may appear in more
 * than one document! It will only return the first one it finds.
 *
 * @lucene.experimental
 */
public class PrimaryKeyLookup {

  protected final String idFieldName;
  protected final TermsEnum[] termsEnums;
  protected final PostingsEnum[] postingsEnums;
  protected final Bits[] liveDocs;
  protected final int[] docBases;
  protected final int numEnums;
  protected final boolean hasDeletions;
  protected final Map<IndexReader.CacheKey, Integer> enumIndexes;

  // Thread-stickiness guard.
  private final long ownerThreadId;

  public PrimaryKeyLookup(IndexReader reader, String idFieldName) throws IOException {
    this(reader, idFieldName, Collections.emptyMap(), null, null);
  }

  /**
   * Construct a {@code PrimaryKeyLookup} instance.
   */
  protected PrimaryKeyLookup(
      IndexReader reader,
      String idFieldName,
      Map<IndexReader.CacheKey, Integer> prevEnumIndexes,
      TermsEnum[] reusableTermsEnums,
      PostingsEnum[] reusablePostingsEnums)
      throws IOException {
    this.idFieldName = idFieldName;
    this.ownerThreadId = Thread.currentThread().threadId();

    List<LeafReaderContext> leaves = new ArrayList<>(reader.leaves());
    // Larger segments are more likely to have the id, so we sort largest to smallest by numDocs:
    leaves.sort((c1, c2) -> c2.reader().numDocs() - c1.reader().numDocs());

    termsEnums = new TermsEnum[leaves.size()];
    postingsEnums = new PostingsEnum[leaves.size()];
    liveDocs = new Bits[leaves.size()];
    docBases = new int[leaves.size()];
    enumIndexes = new HashMap<>();
    int numEnums = 0;
    boolean hasDeletions = false;

    for (LeafReaderContext context : leaves) {
      LeafReader leafReader = context.reader();
      CacheHelper cacheHelper = leafReader.getCoreCacheHelper();
      IndexReader.CacheKey cacheKey = cacheHelper == null ? null : cacheHelper.getKey();

      if (cacheKey != null && prevEnumIndexes.containsKey(cacheKey)) {
        // Reuse termsEnum, postingsEnum.
        int seg = prevEnumIndexes.get(cacheKey);
        termsEnums[numEnums] = reusableTermsEnums[seg];
        postingsEnums[numEnums] = reusablePostingsEnums[seg];
      } else {
        // New or empty segment.
        Terms terms = leafReader.terms(idFieldName);
        if (terms != null) {
          termsEnums[numEnums] = terms.iterator();
          assert termsEnums[numEnums] != null;
        }
      }

      if (termsEnums[numEnums] != null) {
        if (cacheKey != null) {
          enumIndexes.put(cacheKey, numEnums);
        }

        docBases[numEnums] = context.docBase;
        liveDocs[numEnums] = leafReader.getLiveDocs();
        hasDeletions |= leafReader.hasDeletions();

        numEnums++;
      }
    }

    this.numEnums = numEnums;
    this.hasDeletions = hasDeletions;
  }

  /** Returns docID if found, else -1. */
  public int lookup(BytesRef id) throws IOException {
    assert assertOwnerThread();
    for (int seg = 0; seg < numEnums; seg++) {
      if (termsEnums[seg].seekExact(id)) {
        postingsEnums[seg] = termsEnums[seg].postings(postingsEnums[seg], 0);
        int docID;
        while ((docID = postingsEnums[seg].nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
          if (liveDocs[seg] == null || liveDocs[seg].get(docID)) {
            return docBases[seg] + docID;
          }
        }
        assert hasDeletions;
      }
    }

    return -1;
  }

  /**
   * Returns a new PrimaryKeyLookup instance bound reusing this instance per-segment TermsEnum/PostingsEnum etc.
   */
  public PrimaryKeyLookup reopen(IndexReader reader) throws IOException {
    assert assertOwnerThread();
    if (reader == null) {
      return null;
    }
    return new PrimaryKeyLookup(
        reader, this.idFieldName, this.enumIndexes, this.termsEnums, this.postingsEnums);
  }

  private boolean assertOwnerThread() {
    long current = Thread.currentThread().threadId();
    assert current == ownerThreadId
        : "PrimaryKeyLookup is not thread-safe: it was created by thread "
            + ownerThreadId
            + " but accessed from thread "
            + current;
    return true;
  }
}
