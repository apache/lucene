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
package org.apache.lucene.tests.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.CacheHelper;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/**
 * Utility class to do efficient primary-key (only 1 doc contains the given term) lookups by
 * segment, re-using the enums. This class is not thread safe, so it is the caller's job to create
 * and use one instance of this per thread. Do not use this if a term may appear in more than one
 * document! It will only return the first one it finds.
 */
public class PerThreadPKLookup {

  private final String idFieldName;
  protected final TermsEnum[] termsEnums;
  protected final PostingsEnum[] postingsEnums;
  protected final Bits[] liveDocs;
  protected final int[] docBases;
  protected final int numEnums;
  protected final boolean hasDeletions;
  private final Map<IndexReader.CacheKey, Integer> enumIndexes;

  public PerThreadPKLookup(IndexReader reader, String idFieldName) throws IOException {
    this(reader, idFieldName, Collections.emptyMap(), null, null);
  }

  private PerThreadPKLookup(
      IndexReader reader,
      String idFieldName,
      Map<IndexReader.CacheKey, Integer> prevEnumIndexes,
      TermsEnum[] reusableTermsEnums,
      PostingsEnum[] reusablePostingsEnums)
      throws IOException {
    this.idFieldName = idFieldName;

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

    for (int i = 0; i < leaves.size(); i++) {
      LeafReaderContext context = leaves.get(i);
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
    for (int seg = 0; seg < numEnums; seg++) {
      if (termsEnums[seg].seekExact(id)) {
        postingsEnums[seg] = termsEnums[seg].postings(postingsEnums[seg], 0);
        int docID = -1;
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

  /** Reuse previous PerThreadPKLookup's termsEnum and postingsEnum. */
  public PerThreadPKLookup reopen(IndexReader reader) throws IOException {
    if (reader == null) {
      return null;
    }
    return new PerThreadPKLookup(
        reader, this.idFieldName, this.enumIndexes, this.termsEnums, this.postingsEnums);
  }
}
