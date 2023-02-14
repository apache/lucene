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
package org.apache.lucene.search.comparators;

import java.io.IOException;
import java.util.ArrayDeque;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.PriorityQueue;

/**
 * Sorts by field's natural Term sort order, using ordinals. This is functionally equivalent to
 * {@link org.apache.lucene.search.FieldComparator.TermValComparator}, but it first resolves the
 * string to their relative ordinal positions (using the index returned by {@link
 * org.apache.lucene.index.LeafReader#getSortedDocValues(String)}), and does most comparisons using
 * the ordinals. For medium to large results, this comparator will be much faster than {@link
 * org.apache.lucene.search.FieldComparator.TermValComparator}. For very small result sets it may be
 * slower.
 */
public class TermOrdValComparator extends FieldComparator<BytesRef> {

  /* Ords for each slot.
  @lucene.internal */
  final int[] ords;

  /* Values for each slot.
  @lucene.internal */
  final BytesRef[] values;
  private final BytesRefBuilder[] tempBRs;

  /* Which reader last copied a value into the slot. When
  we compare two slots, we just compare-by-ord if the
  readerGen is the same; else we must compare the
  values (slower).
  @lucene.internal */
  final int[] readerGen;

  /* Gen of current reader we are on.
  @lucene.internal */
  int currentReaderGen = -1;

  private final String field;
  private final boolean reverse;
  private final boolean sortMissingLast;

  /* Bottom value (same as values[bottomSlot] once
   bottomSlot is set).  Cached for faster compares.
  @lucene.internal */
  BytesRef bottomValue;

  /* Bottom slot, or -1 if queue isn't full yet */
  int bottomSlot = -1;

  /** Set by setTopValue. */
  BytesRef topValue;

  /** -1 if missing values are sorted first, 1 if they are sorted last */
  final int missingSortCmp;

  /** Whether this is the only comparator. */
  private boolean singleSort;

  /** Whether this comparator is allowed to skip documents. */
  private boolean canSkipDocuments = true;

  /** Whether the collector is done with counting hits so that we can start skipping documents. */
  private boolean hitsThresholdReached = false;

  /**
   * Creates this, with control over how missing values are sorted. Pass sortMissingLast=true to put
   * missing values at the end.
   */
  public TermOrdValComparator(
      int numHits, String field, boolean sortMissingLast, boolean reverse, boolean enableSkipping) {
    canSkipDocuments = enableSkipping;
    ords = new int[numHits];
    values = new BytesRef[numHits];
    tempBRs = new BytesRefBuilder[numHits];
    readerGen = new int[numHits];
    this.field = field;
    this.reverse = reverse;
    this.sortMissingLast = sortMissingLast;
    if (sortMissingLast) {
      missingSortCmp = 1;
    } else {
      missingSortCmp = -1;
    }
  }

  @Override
  public void disableSkipping() {
    canSkipDocuments = false;
  }

  @Override
  public void setSingleSort() {
    singleSort = true;
  }

  @Override
  public int compare(int slot1, int slot2) {
    if (readerGen[slot1] == readerGen[slot2]) {
      return ords[slot1] - ords[slot2];
    }

    final BytesRef val1 = values[slot1];
    final BytesRef val2 = values[slot2];
    if (val1 == null) {
      if (val2 == null) {
        return 0;
      }
      return missingSortCmp;
    } else if (val2 == null) {
      return -missingSortCmp;
    }
    return val1.compareTo(val2);
  }

  /** Retrieves the SortedDocValues for the field in this segment */
  protected SortedDocValues getSortedDocValues(LeafReaderContext context, String field)
      throws IOException {
    return DocValues.getSorted(context.reader(), field);
  }

  @Override
  public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
    currentReaderGen++;
    return new TermOrdValLeafComparator(context, getSortedDocValues(context, field));
  }

  @Override
  public void setTopValue(BytesRef value) {
    // null is fine: it means the last doc of the prior
    // search was missing this value
    topValue = value;
    // System.out.println("setTopValue " + topValue);
  }

  @Override
  public BytesRef value(int slot) {
    return values[slot];
  }

  @Override
  public int compareValues(BytesRef val1, BytesRef val2) {
    if (val1 == null) {
      if (val2 == null) {
        return 0;
      }
      return missingSortCmp;
    } else if (val2 == null) {
      return -missingSortCmp;
    }
    return val1.compareTo(val2);
  }

  private class TermOrdValLeafComparator implements LeafFieldComparator {

    /* Current reader's doc ord/values. */
    final SortedDocValues termsIndex;

    /* True if current bottom slot matches the current reader. */
    boolean bottomSameReader;

    /* Bottom ord (same as ords[bottomSlot] once bottomSlot is set).  Cached for faster compares. */
    int bottomOrd;

    final boolean topSameReader;
    final int topOrd;

    /** Which ordinal to use for a missing value. */
    final int missingOrd;

    private final CompetitiveIterator competitiveIterator;

    private final boolean dense;

    TermOrdValLeafComparator(LeafReaderContext context, SortedDocValues values) throws IOException {
      termsIndex = values;

      if (sortMissingLast) {
        missingOrd = Integer.MAX_VALUE;
      } else {
        missingOrd = -1;
      }

      if (topValue != null) {
        // Recompute topOrd/SameReader
        int ord = termsIndex.lookupTerm(topValue);
        if (ord >= 0) {
          topSameReader = true;
          topOrd = ord;
        } else {
          topSameReader = false;
          topOrd = -ord - 2;
        }
      } else {
        topOrd = missingOrd;
        topSameReader = true;
      }
      // System.out.println("  getLeafComparator topOrd=" + topOrd + " topSameReader=" +
      // topSameReader);

      if (bottomSlot != -1) {
        // Recompute bottomOrd/SameReader
        setBottom(bottomSlot);
      }

      final boolean enableSkipping;
      if (canSkipDocuments == false) {
        dense = false;
        enableSkipping = false;
      } else {
        FieldInfo fieldInfo = context.reader().getFieldInfos().fieldInfo(field);
        if (fieldInfo == null) {
          if (termsIndex.getValueCount() != 0) {
            throw new IllegalStateException("Field [" + field + "] cannot be found in field infos");
          }
          dense = false;
          enableSkipping = true;
        } else if (fieldInfo.getIndexOptions() == IndexOptions.NONE) {
          // No terms index
          dense = false;
          enableSkipping = false;
        } else {
          Terms terms = context.reader().terms(field);
          dense = terms != null && terms.getDocCount() == context.reader().maxDoc();
          if (dense || topValue != null) {
            enableSkipping = true;
          } else if (reverse == sortMissingLast) {
            // Missing values are always competitive, we can never skip
            enableSkipping = false;
          } else {
            enableSkipping = true;
          }
        }
      }
      if (enableSkipping) {
        competitiveIterator = new CompetitiveIterator(context, field, dense, values.termsEnum());
      } else {
        competitiveIterator = null;
      }
      updateCompetitiveIterator();
    }

    private int getOrdForDoc(int doc) throws IOException {
      if (termsIndex.advanceExact(doc)) {
        return termsIndex.ordValue();
      } else {
        return -1;
      }
    }

    @Override
    public void setHitsThresholdReached() throws IOException {
      hitsThresholdReached = true;
      updateCompetitiveIterator();
    }

    @Override
    public int compareBottom(int doc) throws IOException {
      assert bottomSlot != -1;
      int docOrd = getOrdForDoc(doc);
      if (docOrd == -1) {
        docOrd = missingOrd;
      }
      if (bottomSameReader) {
        // ord is precisely comparable, even in the equal case
        return bottomOrd - docOrd;
      } else if (bottomOrd >= docOrd) {
        // the equals case always means bottom is > doc
        // (because we set bottomOrd to the lower bound in
        // setBottom):
        return 1;
      } else {
        return -1;
      }
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
      int ord = getOrdForDoc(doc);
      if (ord == -1) {
        ord = missingOrd;
        values[slot] = null;
      } else {
        assert ord >= 0;
        if (tempBRs[slot] == null) {
          tempBRs[slot] = new BytesRefBuilder();
        }
        tempBRs[slot].copyBytes(termsIndex.lookupOrd(ord));
        values[slot] = tempBRs[slot].get();
      }
      ords[slot] = ord;
      readerGen[slot] = currentReaderGen;
    }

    @Override
    public void setBottom(final int bottom) throws IOException {
      bottomSlot = bottom;

      bottomValue = values[bottomSlot];
      if (currentReaderGen == readerGen[bottomSlot]) {
        bottomOrd = ords[bottomSlot];
        bottomSameReader = true;
      } else {
        if (bottomValue == null) {
          // missingOrd is null for all segments
          assert ords[bottomSlot] == missingOrd;
          bottomOrd = missingOrd;
          bottomSameReader = true;
          readerGen[bottomSlot] = currentReaderGen;
        } else {
          final int ord = termsIndex.lookupTerm(bottomValue);
          if (ord < 0) {
            bottomOrd = -ord - 2;
            bottomSameReader = false;
          } else {
            bottomOrd = ord;
            // exact value match
            bottomSameReader = true;
            readerGen[bottomSlot] = currentReaderGen;
            ords[bottomSlot] = bottomOrd;
          }
        }
      }

      updateCompetitiveIterator();
    }

    @Override
    public int compareTop(int doc) throws IOException {

      int ord = getOrdForDoc(doc);
      if (ord == -1) {
        ord = missingOrd;
      }

      if (topSameReader) {
        // ord is precisely comparable, even in the equal case
        // System.out.println("compareTop doc=" + doc + " ord=" + ord + " ret=" + (topOrd-ord));
        return topOrd - ord;
      } else if (ord <= topOrd) {
        // the equals case always means doc is < value
        // (because we set topOrd to the lower bound)
        return 1;
      } else {
        return -1;
      }
    }

    @Override
    public void setScorer(Scorable scorer) {}

    private void updateCompetitiveIterator() throws IOException {
      if (competitiveIterator == null || hitsThresholdReached == false || bottomSlot == -1) {
        return;
      }
      // This logic to figure out min and max ords is quite complex and verbose, can it be made
      // simpler?
      final int minOrd;
      final int maxOrd;
      if (reverse == false) {

        if (topValue != null) {
          if (topSameReader) {
            minOrd = topOrd;
          } else {
            // In the case when the top value doesn't exist in the segment, topOrd is set as the
            // previous ord, and we are only interested in values that compare strictly greater than
            // this.
            minOrd = topOrd + 1;
          }
        } else if (sortMissingLast || dense) {
          minOrd = 0;
        } else {
          // Missing values are still competitive.
          minOrd = -1;
        }

        if (bottomOrd == missingOrd) {
          // The queue still contains missing values.
          if (singleSort) {
            // If there is no tie breaker, we can start ignoring missing values from now on.
            maxOrd = termsIndex.getValueCount() - 1;
          } else {
            maxOrd = Integer.MAX_VALUE;
          }
        } else if (bottomSameReader) {
          // If there is no tie breaker, we can start ignoring values that compare equal to the
          // current top value too.
          maxOrd = singleSort ? bottomOrd - 1 : bottomOrd;
        } else {
          maxOrd = bottomOrd;
        }

      } else {

        if (bottomOrd == missingOrd) {
          // The queue still contains missing values.
          if (singleSort) {
            // If there is no tie breaker, we can start ignoring missing values from now on.
            minOrd = 0;
          } else {
            minOrd = -1;
          }
        } else if (bottomSameReader) {
          // If there is no tie breaker, we can start ignoring values that compare equal to the
          // current top value too.
          minOrd = singleSort ? bottomOrd + 1 : bottomOrd;
        } else {
          minOrd = bottomOrd + 1;
        }

        if (topValue != null) {
          maxOrd = topOrd;
        } else if (sortMissingLast == false || dense) {
          maxOrd = termsIndex.getValueCount() - 1;
        } else {
          maxOrd = Integer.MAX_VALUE;
        }
      }

      if (minOrd == -1 || maxOrd == Integer.MAX_VALUE) {
        // Missing values are still competitive, we can't skip yet.
        return;
      }
      assert minOrd >= 0;
      assert maxOrd < termsIndex.getValueCount();
      competitiveIterator.update(minOrd, maxOrd);
    }

    @Override
    public DocIdSetIterator competitiveIterator() {
      return competitiveIterator;
    }
  }

  private static class PostingsEnumAndOrd {
    private final PostingsEnum postings;
    private final int ord;

    PostingsEnumAndOrd(PostingsEnum postings, int ord) {
      this.postings = postings;
      this.ord = ord;
    }
  }

  private class CompetitiveIterator extends DocIdSetIterator {

    private static final int MAX_TERMS = 128;

    private final LeafReaderContext context;
    private final int maxDoc;
    private final String field;
    private final boolean dense;
    private final TermsEnum docValuesTerms;
    private int doc = -1;
    private ArrayDeque<PostingsEnumAndOrd> postings;
    private DocIdSetIterator docsWithField;
    private PriorityQueue<PostingsEnumAndOrd> disjunction;

    CompetitiveIterator(
        LeafReaderContext context, String field, boolean dense, TermsEnum docValuesTerms) {
      this.context = context;
      this.maxDoc = context.reader().maxDoc();
      this.field = field;
      this.dense = dense;
      this.docValuesTerms = docValuesTerms;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      return advance(docID() + 1);
    }

    @Override
    public int advance(int target) throws IOException {
      if (target >= maxDoc) {
        return doc = NO_MORE_DOCS;
      } else if (disjunction == null) {
        if (docsWithField != null) {
          // The field is sparse and we're only interested in documents that have a value.
          assert dense == false;
          return doc = docsWithField.advance(target);
        } else {
          // We haven't started skipping yet
          return doc = target;
        }
      } else {
        PostingsEnumAndOrd top = disjunction.top();
        if (top == null) {
          // priority queue is empty, none of the remaining documents are competitive
          return doc = NO_MORE_DOCS;
        }
        while (top.postings.docID() < target) {
          top.postings.advance(target);
          top = disjunction.updateTop();
        }
        return doc = top.postings.docID();
      }
    }

    @Override
    public long cost() {
      return context.reader().maxDoc();
    }

    /**
     * Update this iterator to only match postings whose term has an ordinal between {@code minOrd}
     * included and {@code maxOrd} included.
     */
    private void update(int minOrd, int maxOrd) throws IOException {
      final int maxTerms = Math.min(MAX_TERMS, IndexSearcher.getMaxClauseCount());
      final int size = Math.max(0, maxOrd - minOrd + 1);
      if (size > maxTerms) {
        if (dense == false && docsWithField == null) {
          docsWithField = getSortedDocValues(context, field);
        }
      } else if (postings == null) {
        init(minOrd, maxOrd);
      } else if (size < postings.size()) {
        // One or more ords got removed
        assert postings.isEmpty() || postings.getFirst().ord <= minOrd;
        while (postings.isEmpty() == false && postings.getFirst().ord < minOrd) {
          postings.removeFirst();
        }
        assert postings.isEmpty() || postings.getLast().ord >= maxOrd;
        while (postings.isEmpty() == false && postings.getLast().ord > maxOrd) {
          postings.removeLast();
        }
        disjunction.clear();
        disjunction.addAll(postings);
      }
    }

    /**
     * For the first time, this iterator is allowed to skip documents. It needs to pull {@link
     * PostingsEnum}s from the terms dictionary of the inverted index and create a priority queue
     * out of them.
     */
    private void init(int minOrd, int maxOrd) throws IOException {
      final int size = Math.max(0, maxOrd - minOrd + 1);
      postings = new ArrayDeque<>(size);
      if (size > 0) {
        docValuesTerms.seekExact(minOrd);
        BytesRef minTerm = docValuesTerms.term();
        TermsEnum terms = context.reader().terms(field).iterator();
        if (terms.seekExact(minTerm) == false) {
          throw new IllegalStateException(
              "Term " + minTerm + " exists in doc values but not in the terms index");
        }
        postings.add(new PostingsEnumAndOrd(terms.postings(null, PostingsEnum.NONE), minOrd));
        for (int ord = minOrd + 1; ord <= maxOrd; ++ord) {
          BytesRef next = terms.next();
          if (next == null) {
            throw new IllegalStateException(
                "Terms have more than "
                    + ord
                    + " unique terms while doc values have exactly "
                    + ord
                    + " terms");
          }
          assert docValuesTerms.seekExact(next) && docValuesTerms.ord() == ord;
          postings.add(new PostingsEnumAndOrd(terms.postings(null, PostingsEnum.NONE), ord));
        }
      }
      disjunction =
          new PriorityQueue<PostingsEnumAndOrd>(size) {
            @Override
            protected boolean lessThan(PostingsEnumAndOrd a, PostingsEnumAndOrd b) {
              return a.postings.docID() < b.postings.docID();
            }
          };
      disjunction.addAll(postings);
    }
  }
}
