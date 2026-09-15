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
package org.apache.lucene.search.grouping;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.internal.hppc.LongHashSet;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.UnicodeUtil;

/**
 * A {@link CollectorManager} for computing grouped facet counts based on term DocValues.
 *
 * <p>For each facet value, the result counts the number of distinct group values that have at least
 * one document with that facet value. Groups spanning multiple search slices are counted exactly
 * once (cross-slice deduplication is handled in {@link #reduce}).
 *
 * @lucene.experimental
 */
public class TermGroupFacetCollectorManager
    implements CollectorManager<TermGroupFacetCollectorManager.FacetCollector, GroupedFacetResult> {

  private final String groupField;
  private final String facetField;
  private final boolean facetFieldMultivalued;
  private final BytesRef facetPrefix;
  private final int size;
  private final int minCount;
  private final boolean orderByCount;

  /**
   * Creates a new TermGroupFacetCollectorManager.
   *
   * @param groupField the group field
   * @param facetField the facet field
   * @param facetFieldMultivalued whether the facet field has multiple values per document
   * @param facetPrefix only include facet entries with this prefix; may be null
   * @param size the maximum number of facet entries to include in the result (offset + limit)
   * @param minCount minimum count for a facet entry to be included
   * @param orderByCount whether to sort facet entries by count descending (vs. lexicographic)
   */
  public TermGroupFacetCollectorManager(
      String groupField,
      String facetField,
      boolean facetFieldMultivalued,
      BytesRef facetPrefix,
      int size,
      int minCount,
      boolean orderByCount) {
    this.groupField = groupField;
    this.facetField = facetField;
    this.facetFieldMultivalued = facetFieldMultivalued;
    this.facetPrefix = facetPrefix;
    this.size = size;
    this.minCount = minCount;
    this.orderByCount = orderByCount;
  }

  @Override
  public FacetCollector newCollector() {
    return new FacetCollector(
        groupField, facetField, facetFieldMultivalued, facetPrefix, minCount == 0);
  }

  @Override
  public GroupedFacetResult reduce(Collection<FacetCollector> collectors) throws IOException {
    // Globally deduplicate (group, facet) pairs across all per-slice collectors.
    Set<FacetCollector.GroupFacetPair> globalPairs = new HashSet<>();
    for (FacetCollector collector : collectors) {
      globalPairs.addAll(collector.groupFacetPairs);
    }

    Map<BytesRef, Integer> facetCounts = new HashMap<>();
    int totalCount = 0;
    int missingCount = 0;
    for (FacetCollector.GroupFacetPair pair : globalPairs) {
      if (pair.facetValue() == null) {
        missingCount++;
      } else {
        totalCount++;
        facetCounts.merge(pair.facetValue(), 1, Integer::sum);
      }
    }

    // Terms must be presented to addFacetCount in ascending byte order: GroupedFacetResult's
    // internal currentMin optimization assumes sorted input (as mergeSegmentResults guarantees
    // via its priority queue). TreeSet gives us that order for free.
    Set<BytesRef> sortedTerms = new TreeSet<>(facetCounts.keySet());
    if (minCount == 0) {
      // Union in-range terms from all collectors so that zero-count terms are included.
      for (FacetCollector collector : collectors) {
        sortedTerms.addAll(collector.allFacetTermsInRange);
      }
    }

    GroupedFacetResult result =
        new GroupedFacetResult(size, minCount, orderByCount, totalCount, missingCount);
    for (BytesRef term : sortedTerms) {
      result.addFacetCount(term, facetCounts.getOrDefault(term, 0));
    }
    return result;
  }

  /**
   * Per-slice collector. During collection, (groupOrd, facetOrd) pairs are tracked as packed longs
   * to avoid {@link BytesRef} allocations in the hot path. Ordinals are translated to term values
   * once per segment at {@code finish()} time. Cross-slice deduplication is performed by {@link
   * TermGroupFacetCollectorManager#reduce}.
   */
  static class FacetCollector implements Collector {

    private final String groupField;
    private final String facetField;
    private final boolean facetFieldMultivalued;
    private final BytesRef facetPrefix;
    private final boolean needAllFacetTerms;

    // Accumulated across all segments processed by this collector's slice.
    final Set<GroupFacetPair> groupFacetPairs = new HashSet<>();
    // Populated only when needAllFacetTerms=true (minCount==0), for zero-count term support.
    final Set<BytesRef> allFacetTermsInRange = new HashSet<>();

    FacetCollector(
        String groupField,
        String facetField,
        boolean facetFieldMultivalued,
        BytesRef facetPrefix,
        boolean needAllFacetTerms) {
      this.groupField = groupField;
      this.facetField = facetField;
      this.facetFieldMultivalued = facetFieldMultivalued;
      this.facetPrefix = facetPrefix;
      this.needAllFacetTerms = needAllFacetTerms;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      return facetFieldMultivalued ? new MVLeafCollector(context) : new SVLeafCollector(context);
    }

    @Override
    public ScoreMode scoreMode() {
      return ScoreMode.COMPLETE_NO_SCORES;
    }

    // Packs (groupOrd, facetOrd) into a single long. Adding 1 to each maps the missing sentinel
    // (-1) to 0, making the encoding unambiguous for non-negative ordinal values.
    private static long encodePair(int groupOrd, int facetOrd) {
      return ((long) (groupOrd + 1) << 32) | (facetOrd + 1);
    }

    // Single-valued facet field implementation.
    private class SVLeafCollector implements LeafCollector {

      private final SortedDocValues groupDV;
      private final SortedDocValues facetDV;
      private final int startFacetOrd; // -1 when no prefix, to allow missing facet value through
      private final int endFacetOrd;
      private final LongHashSet segmentPairs = new LongHashSet();

      SVLeafCollector(LeafReaderContext context) throws IOException {
        groupDV = DocValues.getSorted(context.reader(), groupField);
        facetDV = DocValues.getSorted(context.reader(), facetField);
        int sf, ef;
        if (facetPrefix != null) {
          sf = facetDV.lookupTerm(facetPrefix);
          if (sf < 0) {
            sf = -sf - 1;
          }
          BytesRefBuilder end = new BytesRefBuilder();
          end.append(facetPrefix);
          end.append(UnicodeUtil.BIG_TERM);
          ef = facetDV.lookupTerm(end.get());
          assert ef < 0;
          ef = -ef - 1;
        } else {
          sf = -1;
          ef = facetDV.getValueCount();
        }
        startFacetOrd = sf;
        endFacetOrd = ef;
      }

      @Override
      public void setScorer(Scorable scorer) {}

      @Override
      public void collect(int doc) throws IOException {
        if (doc > facetDV.docID()) {
          facetDV.advance(doc);
        }
        int facetOrd = doc == facetDV.docID() ? facetDV.ordValue() : -1;
        if (facetOrd < startFacetOrd || facetOrd >= endFacetOrd) {
          return;
        }
        if (doc > groupDV.docID()) {
          groupDV.advance(doc);
        }
        int groupOrd = doc == groupDV.docID() ? groupDV.ordValue() : -1;
        segmentPairs.add(encodePair(groupOrd, facetOrd));
      }

      @Override
      public void finish() throws IOException {
        for (var cursor : segmentPairs) {
          int groupOrd = (int) (cursor.value >>> 32) - 1;
          int facetOrd = (int) (cursor.value & 0xFFFFFFFFL) - 1;
          BytesRef groupValue =
              groupOrd < 0 ? null : BytesRef.deepCopyOf(groupDV.lookupOrd(groupOrd));
          BytesRef facetValue =
              facetOrd < 0 ? null : BytesRef.deepCopyOf(facetDV.lookupOrd(facetOrd));
          groupFacetPairs.add(new GroupFacetPair(groupValue, facetValue));
        }
        segmentPairs.clear();
        if (needAllFacetTerms) {
          int start = startFacetOrd < 0 ? 0 : startFacetOrd;
          if (start < endFacetOrd) {
            TermsEnum tenum = facetDV.termsEnum();
            tenum.seekExact(start);
            for (int i = start; i < endFacetOrd; i++) {
              BytesRef term = tenum.term();
              if (!allFacetTermsInRange.contains(term)) {
                allFacetTermsInRange.add(BytesRef.deepCopyOf(term));
              }
              if (i + 1 < endFacetOrd) {
                tenum.next();
              }
            }
          }
        }
      }
    }

    // Multi-valued facet field implementation.
    private class MVLeafCollector implements LeafCollector {

      private final SortedDocValues groupDV;
      private final SortedSetDocValues facetDV;
      private final int facetFieldNumTerms;
      private final int startFacetOrd;
      private final int endFacetOrd;
      private final LongHashSet segmentPairs = new LongHashSet();

      MVLeafCollector(LeafReaderContext context) throws IOException {
        groupDV = DocValues.getSorted(context.reader(), groupField);
        facetDV = DocValues.getSortedSet(context.reader(), facetField);
        facetFieldNumTerms = (int) facetDV.getValueCount();
        int sf, ef;
        if (facetPrefix != null) {
          if (facetFieldNumTerms == 0) {
            sf = 0;
            ef = 0;
          } else {
            TermsEnum tenum = facetDV.termsEnum();
            TermsEnum.SeekStatus status = tenum.seekCeil(facetPrefix);
            if (status == TermsEnum.SeekStatus.END) {
              sf = 0;
              ef = 0;
            } else {
              sf = (int) tenum.ord();
              BytesRefBuilder end = new BytesRefBuilder();
              end.append(facetPrefix);
              end.append(UnicodeUtil.BIG_TERM);
              status = tenum.seekCeil(end.get());
              ef = (status == TermsEnum.SeekStatus.END) ? facetFieldNumTerms : (int) tenum.ord();
            }
          }
        } else {
          sf = 0;
          ef = facetFieldNumTerms;
        }
        startFacetOrd = sf;
        endFacetOrd = ef;
      }

      @Override
      public void setScorer(Scorable scorer) {}

      @Override
      public void collect(int doc) throws IOException {
        if (doc > facetDV.docID()) {
          facetDV.advance(doc);
        }
        boolean hasFacetDocValues = doc == facetDV.docID();
        if (hasFacetDocValues) {
          int groupOrd = Integer.MIN_VALUE; // lazily fetched on first in-range facet ord
          for (int i = 0; i < facetDV.docValueCount(); i++) {
            int facetOrd = (int) facetDV.nextOrd();
            if (facetOrd >= startFacetOrd && facetOrd < endFacetOrd) {
              if (groupOrd == Integer.MIN_VALUE) {
                if (doc > groupDV.docID()) {
                  groupDV.advance(doc);
                }
                groupOrd = doc == groupDV.docID() ? groupDV.ordValue() : -1;
              }
              segmentPairs.add(encodePair(groupOrd, facetOrd));
            }
          }
        } else if (facetPrefix == null) {
          // No facet values at all and no prefix: count as missing (facetOrd = -1).
          if (doc > groupDV.docID()) {
            groupDV.advance(doc);
          }
          int groupOrd = doc == groupDV.docID() ? groupDV.ordValue() : -1;
          segmentPairs.add(encodePair(groupOrd, -1));
        }
      }

      @Override
      public void finish() throws IOException {
        for (var cursor : segmentPairs) {
          int groupOrd = (int) (cursor.value >>> 32) - 1;
          int facetOrd = (int) (cursor.value & 0xFFFFFFFFL) - 1;
          BytesRef groupValue =
              groupOrd < 0 ? null : BytesRef.deepCopyOf(groupDV.lookupOrd(groupOrd));
          BytesRef facetValue =
              facetOrd < 0 ? null : BytesRef.deepCopyOf(facetDV.lookupOrd((long) facetOrd));
          groupFacetPairs.add(new GroupFacetPair(groupValue, facetValue));
        }
        segmentPairs.clear();
        if (needAllFacetTerms && startFacetOrd < endFacetOrd) {
          TermsEnum tenum = facetDV.termsEnum();
          tenum.seekExact(startFacetOrd);
          for (int i = startFacetOrd; i < endFacetOrd; i++) {
            BytesRef term = tenum.term();
            if (!allFacetTermsInRange.contains(term)) {
              allFacetTermsInRange.add(BytesRef.deepCopyOf(term));
            }
            if (i + 1 < endFacetOrd) {
              tenum.next();
            }
          }
        }
      }
    }

    record GroupFacetPair(BytesRef groupValue, BytesRef facetValue) {}
  }
}
