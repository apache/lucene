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
package org.apache.lucene.search.matchhighlight;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

/**
 * An example highlighter that combines several lower-level utility classes in this package into a
 * fully featured, ready-to-use component.
 *
 * <p>Note: if you need to customize or tweak the details of highlighting, it is better to assemble
 * your own highlighter using those low-level building blocks, rather than extend or modify this
 * one.
 */
public class MatchHighlighter {
  private final IndexSearcher searcher;
  private final OffsetsRetrievalStrategySupplier offsetsRetrievalStrategies;
  private final Analyzer analyzer;

  private final HashSet<String> fieldsAlwaysReturned = new HashSet<>();
  private final List<FieldValueHighlighter> fieldHighlighters = new ArrayList<>();

  /**
   * Actual per-field highlighter. Field highlighters are probed whether they are applicable to a
   * particular combination of (field, hasMatches) pair. If a highlighter declares it is applicable,
   * its {@link #format} method is invoked and the result is returned as the field's value.
   *
   * @see FieldValueHighlighters
   */
  public interface FieldValueHighlighter {
    /**
     * Check if this highlighter can be applied to a given field.
     *
     * @param field Field name
     * @param hasMatches {@code true} if the field has a non-empty set of match regions.
     */
    boolean isApplicable(String field, boolean hasMatches);

    /** Format field values into a list of final "highlights". */
    List<String> format(
        String field,
        List<String> values,
        String contiguousValue,
        List<OffsetRange> valueRanges,
        List<QueryOffsetRange> matchOffsets);

    /**
     * @return Returns a set of fields that must be loaded from each document, regardless of whether
     *     they had matches or not. This is useful to load and return certain fields that should
     *     always be included (identifiers, document titles, etc.).
     */
    default Collection<String> alwaysFetchedFields() {
      return Collections.emptyList();
    }

    /** Returns a new field value highlighter that is a combination of this one and another one. */
    default FieldValueHighlighter or(FieldValueHighlighter other) {
      FieldValueHighlighter first = this;
      FieldValueHighlighter second = other;

      HashSet<String> fieldUnion = new HashSet<>();
      fieldUnion.addAll(first.alwaysFetchedFields());
      fieldUnion.addAll(second.alwaysFetchedFields());

      return new FieldValueHighlighter() {
        @Override
        public boolean isApplicable(String field, boolean hasMatches) {
          return first.isApplicable(field, hasMatches) || second.isApplicable(field, hasMatches);
        }

        @Override
        public List<String> format(
            String field,
            List<String> values,
            String contiguousValue,
            List<OffsetRange> valueRanges,
            List<QueryOffsetRange> matchOffsets) {
          FieldValueHighlighter delegate =
              first.isApplicable(field, matchOffsets != null && !matchOffsets.isEmpty())
                  ? first
                  : second;
          return delegate.format(field, values, contiguousValue, valueRanges, matchOffsets);
        }

        @Override
        public Collection<String> alwaysFetchedFields() {
          return fieldUnion;
        }
      };
    }
  }

  /**
   * Append a new highlighter to field highlighters chain. The order of field highlighters is
   * important (first-matching wins).
   */
  public MatchHighlighter appendFieldHighlighter(FieldValueHighlighter highlighter) {
    fieldHighlighters.add(highlighter);
    fieldsAlwaysReturned.addAll(highlighter.alwaysFetchedFields());
    return this;
  }

  /** Always fetch the given set of fields for all input documents. */
  public void alwaysFetchFields(String... fields) {
    for (String fld : fields) {
      fieldsAlwaysReturned.add(Objects.requireNonNull(fld));
    }
  }

  /** Single document's highlights. */
  public static class DocHighlights {
    public final int docId;
    public final Map<String, List<String>> fields = new LinkedHashMap<>();

    public DocHighlights(int docId) {
      this.docId = docId;
    }
  }

  /** An {@link OffsetRange} of a match, together with the source query that caused it. */
  public static class QueryOffsetRange extends OffsetRange {
    public final Query query;

    public QueryOffsetRange(Query query, int from, int to) {
      super(from, to);
      this.query = query;
    }

    @Override
    public QueryOffsetRange slice(int from, int to) {
      return new QueryOffsetRange(query, from, to);
    }
  }

  private static class DocHit {
    final int docId;
    private final LinkedHashMap<String, List<QueryOffsetRange>> matchRanges = new LinkedHashMap<>();
    private final LinkedHashMap<String, List<String>> fieldValues = new LinkedHashMap<>();

    DocHit(int docId, MatchRegionRetriever.FieldValueProvider fieldValueProvider) {
      this.docId = docId;
      for (var fieldName : fieldValueProvider) {
        fieldValues.put(fieldName, fieldValueProvider.getValues(fieldName));
      }
    }

    void addMatches(Query query, Map<String, List<OffsetRange>> hits) {
      hits.forEach(
          (field, offsets) -> {
            List<QueryOffsetRange> target =
                matchRanges.computeIfAbsent(field, (_) -> new ArrayList<>());
            offsets.forEach(o -> target.add(new QueryOffsetRange(query, o.from, o.to)));
          });
    }
  }

  public MatchHighlighter(IndexSearcher searcher, Analyzer analyzer) {
    this(
        searcher,
        analyzer,
        MatchRegionRetriever.computeOffsetRetrievalStrategies(searcher.getIndexReader(), analyzer));
  }

  public MatchHighlighter(
      IndexSearcher searcher,
      Analyzer analyzer,
      OffsetsRetrievalStrategySupplier offsetsRetrievalStrategies) {
    this.searcher = searcher;
    this.offsetsRetrievalStrategies = offsetsRetrievalStrategies;
    this.analyzer = analyzer;
  }

  public Stream<DocHighlights> highlight(TopDocs topDocs, Query... queries) throws IOException {
    // We want to preserve topDocs document ordering and MatchRegionRetriever is optimized
    // for streaming, so we'll just populate the map in proper order.
    LinkedHashMap<Integer, DocHit> docHits = new LinkedHashMap<>();
    for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
      docHits.put(scoreDoc.doc, null);
    }

    Predicate<String> fieldsToLoadUnconditionally = fieldsAlwaysReturned::contains;
    Predicate<String> fieldsToLoadIfWithHits =
        fieldName -> {
          // We're interested in any fields for which existing highlighters are applicable (with or
          // without hits).
          return fieldHighlighters.stream()
              .anyMatch(
                  highlighter ->
                      highlighter.isApplicable(fieldName, true)
                          || highlighter.isApplicable(fieldName, false));
        };

    // Collect match ranges for each query and associate each range to the origin query.
    for (Query q : queries) {
      MatchRegionRetriever highlighter =
          new MatchRegionRetriever(
              searcher,
              searcher.rewrite(q),
              offsetsRetrievalStrategies,
              fieldsToLoadUnconditionally,
              fieldsToLoadIfWithHits);

      highlighter.highlightDocuments(
          topDocs,
          (int docId,
              LeafReader _,
              int _,
              MatchRegionRetriever.FieldValueProvider fieldValueProvider,
              Map<String, List<OffsetRange>> hits) -> {
            DocHit docHit = docHits.get(docId);
            if (docHit == null) {
              docHit = new DocHit(docId, fieldValueProvider);
              docHits.put(docId, docHit);
            }
            docHit.addMatches(q, hits);
          });
    }

    return docHits.values().stream()
        .filter(Objects::nonNull) // This should always the case?
        .map(this::computeDocFieldValues);
  }

  private DocHighlights computeDocFieldValues(DocHit docHit) {
    DocHighlights docHighlights = new DocHighlights(docHit.docId);

    for (var e : docHit.fieldValues.entrySet()) {
      String field = e.getKey();
      List<String> values = e.getValue();
      String contiguousValue = contiguousFieldValue(field, values);
      List<OffsetRange> valueRanges = computeValueRanges(field, values);
      List<QueryOffsetRange> offsets = docHit.matchRanges.get(field);

      List<String> formattedValues =
          fieldValueHighlighter(field, offsets != null)
              .format(field, values, contiguousValue, valueRanges, offsets);

      if (formattedValues != null) {
        docHighlights.fields.put(field, formattedValues);
      }
    }

    return docHighlights;
  }

  private List<OffsetRange> computeValueRanges(String field, List<String> values) {
    ArrayList<OffsetRange> valueRanges = new ArrayList<>();
    int offset = 0;
    for (CharSequence v : values) {
      valueRanges.add(new OffsetRange(offset, offset + v.length()));
      offset += v.length();
      offset += analyzer.getOffsetGap(field);
    }
    return valueRanges;
  }

  private String contiguousFieldValue(String field, List<String> values) {
    String value;
    if (values.size() == 1) {
      value = values.get(0);
    } else {
      // TODO: This can be inefficient if offset gap is large but the logic
      // of applying offsets would get much more complicated so leaving for now
      // (would have to recalculate all offsets to omit gaps).
      String fieldGapPadding = " ".repeat(analyzer.getOffsetGap(field));
      value = String.join(fieldGapPadding, values);
    }
    return value;
  }

  private FieldValueHighlighter fieldValueHighlighter(String field, boolean hasMatches) {
    for (FieldValueHighlighter highlighter : fieldHighlighters) {
      if (highlighter.isApplicable(field, hasMatches)) {
        return highlighter;
      }
    }
    throw new RuntimeException("No field highlighter could be matched to field: " + field);
  }
}
