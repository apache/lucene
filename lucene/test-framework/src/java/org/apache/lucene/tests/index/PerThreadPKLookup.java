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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReader;
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

  private IndexReader reader;
  private final String idFieldName;
  protected TermsEnum[] termsEnums;
  protected PostingsEnum[] postingsEnums;
  protected Bits[] liveDocs;
  protected int[] docBases;
  protected int numSegs;
  protected boolean hasDeletions;
  protected Map<SegmentInfo, Integer> segmentInfoMap = new HashMap<>();

  public PerThreadPKLookup(IndexReader reader, String idFieldName) throws IOException {
    this.reader = reader;
    this.idFieldName = idFieldName;

    List<LeafReaderContext> leaves = new ArrayList<>(reader.leaves());

    // Larger segments are more likely to have the id, so we sort largest to smallest by numDocs:
    Collections.sort(
        leaves,
        new Comparator<LeafReaderContext>() {
          @Override
          public int compare(LeafReaderContext c1, LeafReaderContext c2) {
            return c2.reader().numDocs() - c1.reader().numDocs();
          }
        });

    termsEnums = new TermsEnum[leaves.size()];
    postingsEnums = new PostingsEnum[leaves.size()];
    liveDocs = new Bits[leaves.size()];
    docBases = new int[leaves.size()];
    numSegs = 0;
    hasDeletions = false;
    for (int i = 0; i < leaves.size(); i++) {
      LeafReader leafReader = leaves.get(i).reader();
      Terms terms = leafReader.terms(idFieldName);
      SegmentInfo segmentInfo = ((SegmentReader) leafReader).getSegmentInfo().info;
      if (terms != null) {
        termsEnums[numSegs] = terms.iterator();
        assert termsEnums[numSegs] != null;
        docBases[numSegs] = leaves.get(i).docBase;
        liveDocs[numSegs] = leafReader.getLiveDocs();
        hasDeletions |= leafReader.hasDeletions();
        segmentInfoMap.put(segmentInfo, numSegs);
        numSegs++;
      } else {
        segmentInfoMap.put(segmentInfo, -1);
      }
    }
  }

  /** Returns docID if found, else -1. */
  public int lookup(BytesRef id) throws IOException {
    for (int seg = 0; seg < numSegs; seg++) {
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

  /**
   * Reuse old segments termsEnum and postingsEnum.
   *
   * @return null if there are no changes; else, a new DirectoryReader instance which you must
   *     eventually close.
   * @throws IOException if there is a low-level IO error.
   */
  public DirectoryReader reopen() throws IOException {
    DirectoryReader newReader = DirectoryReader.openIfChanged((DirectoryReader) reader);
    if (newReader == null) {
      return null;
    }

    List<LeafReaderContext> leaves = new ArrayList<>(newReader.leaves());
    // Larger segments are more likely to have the id, so we sort largest to smallest by numDocs:
    Collections.sort(
        leaves,
        new Comparator<LeafReaderContext>() {
          @Override
          public int compare(LeafReaderContext c1, LeafReaderContext c2) {
            return c2.reader().numDocs() - c1.reader().numDocs();
          }
        });

    TermsEnum[] newTermsEnums = new TermsEnum[leaves.size()];
    PostingsEnum[] newPostingsEnums = new PostingsEnum[leaves.size()];
    Bits[] newLiveDocs = new Bits[leaves.size()];
    int[] newDocBases = new int[leaves.size()];
    Map<SegmentInfo, Integer> newReaderMap = new HashMap<>();

    numSegs = 0;
    hasDeletions = false;
    for (int i = 0; i < leaves.size(); i++) {
      LeafReader leafReader = leaves.get(i).reader();
      SegmentInfo segmentInfo = ((SegmentReader) leafReader).getSegmentInfo().info;
      if (segmentInfoMap.containsKey(segmentInfo)) {
        // Reuse termsEnum, postingsEnum.
        Integer seg = segmentInfoMap.get(segmentInfo);
        if (seg > -1) {
          newTermsEnums[numSegs] = termsEnums[seg];
          newPostingsEnums[numSegs] = postingsEnums[seg];
          assert newTermsEnums[numSegs] != null;
          // Update liveDocs.
          newDocBases[numSegs] = leaves.get(i).docBase;
          newLiveDocs[numSegs] = leafReader.getLiveDocs();
          hasDeletions |= leafReader.hasDeletions();
          newReaderMap.put(segmentInfo, numSegs);
          numSegs++;
        } else {
          // TermsEnum is always null.
          newReaderMap.put(segmentInfo, -1);
        }
      } else {
        // New segment.
        Terms terms = leafReader.terms(idFieldName);
        if (terms != null) {
          newTermsEnums[numSegs] = terms.iterator();
          assert newTermsEnums[numSegs] != null;
          newDocBases[numSegs] = leaves.get(i).docBase;
          newLiveDocs[numSegs] = leafReader.getLiveDocs();
          hasDeletions |= leafReader.hasDeletions();
          newReaderMap.put(segmentInfo, numSegs);
          numSegs++;
        } else {
          newReaderMap.put(segmentInfo, -1);
        }
      }
    }

    reader = newReader;
    termsEnums = newTermsEnums;
    postingsEnums = newPostingsEnums;
    liveDocs = newLiveDocs;
    docBases = newDocBases;
    segmentInfoMap = newReaderMap;

    return (DirectoryReader) reader;
  }
}
