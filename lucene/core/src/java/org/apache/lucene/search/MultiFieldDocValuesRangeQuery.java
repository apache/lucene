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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;

/**
 * A query that efficiently evaluates multiple numeric doc-values range filters in conjunction by
 * coordinating their {@link DocValuesSkipper} instances. When one field's skip list determines a
 * block has no matching documents, all other fields skip that block without reading their skip
 * metadata.
 *
 * <p>This is equivalent to a BooleanQuery with FILTER clauses for each range, but avoids redundant
 * skip-list reads and per-document evaluations for blocks eliminated by any single field.
 *
 * <p>When a segment lacks skip indices for any field, this query falls back to the original
 * individual range queries run as a conjunction.
 *
 * @lucene.experimental
 */
public class MultiFieldDocValuesRangeQuery extends Query {

  /** A single field's range predicate. */
  public record FieldRange(String field, long lowerValue, long upperValue) {
    public FieldRange {
      Objects.requireNonNull(field, "field must not be null");
      if (lowerValue > upperValue) {
        throw new IllegalArgumentException(
            "lowerValue must be <= upperValue, got " + lowerValue + " > " + upperValue);
      }
    }
  }

  private final FieldRange[] fieldRanges;
  private final Query fallbackQuery;

  /**
   * Create a multi-field range query.
   *
   * @param fieldRanges the per-field range predicates (must have at least 2)
   * @param fallbackQuery a conjunction of the individual range queries, used when skip coordination
   *     is not possible for a segment (e.g., a field lacks a skip index)
   */
  public MultiFieldDocValuesRangeQuery(List<FieldRange> fieldRanges, Query fallbackQuery) {
    if (fieldRanges.size() < 2) {
      throw new IllegalArgumentException("Need at least 2 field ranges, got " + fieldRanges.size());
    }
    this.fieldRanges = fieldRanges.toArray(new FieldRange[0]);
    this.fallbackQuery = Objects.requireNonNull(fallbackQuery, "fallbackQuery must not be null");
  }

  /** Returns the field ranges this query filters on. */
  public List<FieldRange> getFieldRanges() {
    return List.of(fieldRanges);
  }

  /** Returns the fallback query used when skip coordination is not possible. */
  public Query getFallbackQuery() {
    return fallbackQuery;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    Weight fallbackWeight = fallbackQuery.createWeight(searcher, scoreMode, boost);

    return new ConstantScoreWeight(this, boost) {

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return fallbackWeight.isCacheable(ctx);
      }

      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        LeafReader reader = context.reader();
        int maxDoc = reader.maxDoc();

        // Check if all fields have skip indices in this segment
        boolean allHaveSkippers = true;
        for (FieldRange fr : fieldRanges) {
          if (reader.getFieldInfos().fieldInfo(fr.field()) == null
              || reader.getDocValuesSkipper(fr.field()) == null) {
            allHaveSkippers = false;
            break;
          }
        }

        if (!allHaveSkippers) {
          // Fall back to the original conjunction of individual range queries
          return fallbackWeight.scorerSupplier(context);
        }

        // Build coordinated skip evaluation
        List<FieldState> states = new ArrayList<>(fieldRanges.length);
        for (FieldRange fr : fieldRanges) {
          DocValuesSkipper skipper = reader.getDocValuesSkipper(fr.field());

          // Global min/max check
          if (skipper.minValue() > fr.upperValue() || skipper.maxValue() < fr.lowerValue()) {
            return null; // entire segment out of range for this field
          }

          SortedNumericDocValues values = DocValues.getSortedNumeric(reader, fr.field());
          NumericDocValues singleton = DocValues.unwrapSingleton(values);
          TwoPhaseIterator twoPhase;
          if (singleton != null) {
            twoPhase = buildSingletonTwoPhase(singleton, fr);
          } else {
            twoPhase = buildMultiValueTwoPhase(values, fr);
          }

          long fieldSpan = skipper.maxValue() - skipper.minValue();
          long querySpan = fr.upperValue() - fr.lowerValue();
          double selectivity = fieldSpan == 0 ? 1.0 : Math.min(1.0, (double) querySpan / fieldSpan);
          states.add(new FieldState(fr, skipper, twoPhase, selectivity));
        }

        // Sort by selectivity: most selective first
        states.sort(Comparator.comparingDouble(s -> s.selectivity));
        FieldState[] sorted = states.toArray(new FieldState[0]);
        DocIdSetIterator iterator = new CoordinatedIterator(sorted, maxDoc);
        return ConstantScoreScorerSupplier.fromIterator(iterator, score(), scoreMode, maxDoc);
      }
    };
  }

  private static TwoPhaseIterator buildSingletonTwoPhase(
      NumericDocValues singleton, FieldRange fr) {
    return new TwoPhaseIterator(singleton) {
      @Override
      public boolean matches() throws IOException {
        long value = singleton.longValue();
        return value >= fr.lowerValue() && value <= fr.upperValue();
      }

      @Override
      public float matchCost() {
        return 2;
      }
    };
  }

  private static TwoPhaseIterator buildMultiValueTwoPhase(
      SortedNumericDocValues values, FieldRange fr) {
    return new TwoPhaseIterator(values) {
      @Override
      public boolean matches() throws IOException {
        for (int i = 0, count = values.docValueCount(); i < count; ++i) {
          long value = values.nextValue();
          if (value < fr.lowerValue()) continue;
          return value <= fr.upperValue();
        }
        return false;
      }

      @Override
      public float matchCost() {
        return 2;
      }
    };
  }

  /** Per-field state held during query execution. */
  private static class FieldState {
    final FieldRange range;
    final DocValuesSkipper skipper;
    final TwoPhaseIterator twoPhase;
    final double selectivity;
    int blockMaxDocID = -1;

    FieldState(
        FieldRange range, DocValuesSkipper skipper, TwoPhaseIterator twoPhase, double selectivity) {
      this.range = range;
      this.skipper = skipper;
      this.twoPhase = twoPhase;
      this.selectivity = selectivity;
    }

    DocValuesRangeIterator.Match classify() {
      return classifyAtLevel(0);
    }

    DocValuesRangeIterator.Match classifyAtLevel(int level) {
      long minVal = skipper.minValue(level);
      long maxVal = skipper.maxValue(level);
      if (minVal > range.upperValue() || maxVal < range.lowerValue()) {
        return DocValuesRangeIterator.Match.NO;
      } else if (minVal >= range.lowerValue() && maxVal <= range.upperValue()) {
        if (skipper.docCount(level) == skipper.maxDocID(level) - skipper.minDocID(level) + 1) {
          return DocValuesRangeIterator.Match.YES;
        } else {
          return DocValuesRangeIterator.Match.IF_DOC_HAS_VALUE;
        }
      }
      return DocValuesRangeIterator.Match.MAYBE;
    }
  }

  /**
   * The coordinated iterator. Advances all fields' skippers together, short-circuiting on the first
   * field that classifies a block as NO.
   */
  private static class CoordinatedIterator extends DocIdSetIterator {
    private final FieldState[] fields;
    private final int maxDoc;
    private int doc = -1;

    private int blockStart;
    private int blockEnd;
    private DocValuesRangeIterator.Match[] matches;

    CoordinatedIterator(FieldState[] fields, int maxDoc) {
      this.fields = fields;
      this.maxDoc = maxDoc;
      this.matches = new DocValuesRangeIterator.Match[fields.length];
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
      while (target < maxDoc) {
        // Phase 1: Advance lead field's skipper (most selective)
        FieldState lead = fields[0];
        if (target > lead.blockMaxDocID) {
          lead.skipper.advance(target);
          if (lead.skipper.minDocID(0) == NO_MORE_DOCS) {
            return doc = NO_MORE_DOCS;
          }
          lead.blockMaxDocID = lead.skipper.maxDocID(0);
        }
        target = Math.max(target, lead.skipper.minDocID(0));

        // Classify lead field
        matches[0] = lead.classify();
        if (matches[0] == DocValuesRangeIterator.Match.NO) {
          int skipTo = lead.blockMaxDocID;
          for (int level = 1; level < lead.skipper.numLevels(); level++) {
            if (lead.classifyAtLevel(level) == DocValuesRangeIterator.Match.NO) {
              skipTo = lead.skipper.maxDocID(level);
            } else {
              break;
            }
          }
          target = skipTo + 1;
          continue;
        }

        // Phase 2: Check remaining fields, short-circuit on first NO
        blockStart = lead.skipper.minDocID(0);
        blockEnd = lead.blockMaxDocID;
        boolean anyNo = false;
        boolean allFullMatch = isFullMatch(matches[0]);

        for (int f = 1; f < fields.length; f++) {
          FieldState fs = fields[f];
          if (target > fs.blockMaxDocID) {
            fs.skipper.advance(target);
            if (fs.skipper.minDocID(0) == NO_MORE_DOCS) {
              return doc = NO_MORE_DOCS;
            }
            fs.blockMaxDocID = fs.skipper.maxDocID(0);
          }

          blockStart = Math.max(blockStart, fs.skipper.minDocID(0));
          blockEnd = Math.min(blockEnd, fs.blockMaxDocID);

          if (blockStart > blockEnd) {
            target = blockEnd + 1;
            anyNo = true;
            break;
          }

          matches[f] = fs.classify();
          if (matches[f] == DocValuesRangeIterator.Match.NO) {
            anyNo = true;
            target = blockEnd + 1;
            break;
          }
          if (!isFullMatch(matches[f])) {
            allFullMatch = false;
          }
        }

        if (anyNo) {
          continue;
        }

        target = Math.max(target, blockStart);

        // Level promotion: widen block range if all fields agree
        if (allFullMatch) {
          int nextLevel = 1;
          while (true) {
            boolean allSameAtLevel = true;
            int promotedEnd = Integer.MAX_VALUE;
            for (int f = 0; f < fields.length; f++) {
              if (nextLevel >= fields[f].skipper.numLevels()) {
                allSameAtLevel = false;
                break;
              }
              DocValuesRangeIterator.Match higherMatch = fields[f].classifyAtLevel(nextLevel);
              if (!isFullMatch(higherMatch)) {
                allSameAtLevel = false;
                break;
              }
              promotedEnd = Math.min(promotedEnd, fields[f].skipper.maxDocID(nextLevel));
            }
            if (allSameAtLevel && promotedEnd > blockEnd) {
              blockEnd = promotedEnd;
              nextLevel++;
            } else {
              break;
            }
          }
        }

        if (allFullMatch) {
          boolean allTrueYes = true;
          for (int f = 0; f < fields.length; f++) {
            if (matches[f] != DocValuesRangeIterator.Match.YES) {
              allTrueYes = false;
              break;
            }
          }
          if (allTrueYes) {
            return doc = target;
          }
          int result = evaluatePerDoc(target, blockEnd);
          if (result != NO_MORE_DOCS) {
            return doc = result;
          }
          target = blockEnd + 1;
          continue;
        }

        // Phase 3: MAYBE — per-doc evaluation
        int result = evaluatePerDoc(target, blockEnd);
        if (result != NO_MORE_DOCS) {
          return doc = result;
        }
        target = blockEnd + 1;
      }
      return doc = NO_MORE_DOCS;
    }

    private static boolean isFullMatch(DocValuesRangeIterator.Match m) {
      return m == DocValuesRangeIterator.Match.YES
          || m == DocValuesRangeIterator.Match.IF_DOC_HAS_VALUE;
    }

    private int evaluatePerDoc(int start, int end) throws IOException {
      int leadIdx = 0;
      for (int f = 0; f < fields.length; f++) {
        if (matches[f] == DocValuesRangeIterator.Match.MAYBE) {
          leadIdx = f;
          break;
        }
        if (matches[f] == DocValuesRangeIterator.Match.IF_DOC_HAS_VALUE) {
          leadIdx = f;
        }
      }

      DocIdSetIterator leadApprox = fields[leadIdx].twoPhase.approximation();
      int candidate = leadApprox.docID();
      if (candidate < start) {
        candidate = leadApprox.advance(start);
      }

      while (candidate != NO_MORE_DOCS && candidate <= end) {
        int result = tryMatchAllFields(candidate, end, leadIdx, leadApprox);
        if (result == candidate) {
          return candidate;
        }
        candidate = result;
      }
      return NO_MORE_DOCS;
    }

    private int tryMatchAllFields(int candidate, int end, int leadIdx, DocIdSetIterator leadApprox)
        throws IOException {
      for (int f = 0; f < fields.length; f++) {
        if (matches[f] == DocValuesRangeIterator.Match.YES) {
          continue;
        }

        DocIdSetIterator approx = fields[f].twoPhase.approximation();
        int approxDoc = approx.docID();

        if (approxDoc < candidate) {
          approxDoc = approx.advance(candidate);
        }

        if (approxDoc != candidate) {
          return advanceToNext(approxDoc, end, leadApprox);
        }

        if (matches[f] == DocValuesRangeIterator.Match.MAYBE) {
          if (!fields[f].twoPhase.matches()) {
            int next = leadApprox.advance(candidate + 1);
            if (next > end || next == NO_MORE_DOCS) {
              return NO_MORE_DOCS;
            }
            return next;
          }
        }
      }
      return candidate;
    }

    private int advanceToNext(int target, int end, DocIdSetIterator leadApprox) throws IOException {
      if (target > end || target == NO_MORE_DOCS) {
        return NO_MORE_DOCS;
      }
      if (leadApprox.docID() < target) {
        target = leadApprox.advance(target);
        if (target > end || target == NO_MORE_DOCS) {
          return NO_MORE_DOCS;
        }
      }
      return target;
    }

    @Override
    public long cost() {
      long minCost = Long.MAX_VALUE;
      for (FieldState fs : fields) {
        minCost = Math.min(minCost, fs.twoPhase.approximation().cost());
      }
      return minCost;
    }
  }

  @Override
  public String toString(String defaultField) {
    StringBuilder sb = new StringBuilder("MultiFieldDocValuesRange(");
    for (int i = 0; i < fieldRanges.length; i++) {
      if (i > 0) sb.append(" AND ");
      FieldRange fr = fieldRanges[i];
      sb.append(fr.field())
          .append(":[")
          .append(fr.lowerValue())
          .append(" TO ")
          .append(fr.upperValue())
          .append("]");
    }
    sb.append(")");
    return sb.toString();
  }

  @Override
  public void visit(QueryVisitor visitor) {
    for (FieldRange fr : fieldRanges) {
      if (visitor.acceptField(fr.field())) {
        visitor.visitLeaf(this);
        return;
      }
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof MultiFieldDocValuesRangeQuery other)) return false;
    return Arrays.equals(fieldRanges, other.fieldRanges)
        && Objects.equals(fallbackQuery, other.fallbackQuery);
  }

  @Override
  public int hashCode() {
    return Objects.hash(classHash(), Arrays.hashCode(fieldRanges), fallbackQuery);
  }
}
