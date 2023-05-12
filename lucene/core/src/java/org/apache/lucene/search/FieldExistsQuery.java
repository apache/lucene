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
import java.util.Objects;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.Terms;

/**
 * A {@link Query} that matches documents that contain either a {@link KnnFloatVectorField}, {@link
 * org.apache.lucene.document.KnnByteVectorField} or a field that indexes norms or doc values.
 */
public class FieldExistsQuery extends Query {
  private String field;

  /** Create a query that will match that have a value for the given {@code field}. */
  public FieldExistsQuery(String field) {
    this.field = Objects.requireNonNull(field);
  }

  /**
   * Returns a {@link DocIdSetIterator} from the given field or null if the field doesn't exist in
   * the reader or if the reader has no doc values for the field.
   */
  public static DocIdSetIterator getDocValuesDocIdSetIterator(String field, LeafReader reader)
      throws IOException {
    FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field);
    final DocIdSetIterator iterator;
    if (fieldInfo != null) {
      switch (fieldInfo.getDocValuesType()) {
        case NONE:
          iterator = null;
          break;
        case NUMERIC:
          iterator = reader.getNumericDocValues(field);
          break;
        case BINARY:
          iterator = reader.getBinaryDocValues(field);
          break;
        case SORTED:
          iterator = reader.getSortedDocValues(field);
          break;
        case SORTED_NUMERIC:
          iterator = reader.getSortedNumericDocValues(field);
          break;
        case SORTED_SET:
          iterator = reader.getSortedSetDocValues(field);
          break;
        default:
          throw new AssertionError();
      }
      return iterator;
    }
    return null;
  }

  public String getField() {
    return field;
  }

  @Override
  public String toString(String field) {
    return "FieldExistsQuery [field=" + this.field + "]";
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
    }
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) && field.equals(((FieldExistsQuery) other).field);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int hash = classHash();
    hash = prime * hash + field.hashCode();
    return hash;
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    IndexReader reader = indexSearcher.getIndexReader();
    boolean allReadersRewritable = true;

    for (LeafReaderContext context : reader.leaves()) {
      LeafReader leaf = context.reader();
      FieldInfos fieldInfos = leaf.getFieldInfos();
      FieldInfo fieldInfo = fieldInfos.fieldInfo(field);

      if (fieldInfo == null) {
        allReadersRewritable = false;
        break;
      }

      if (fieldInfo.hasNorms()) { // the field indexes norms
        if (reader.getDocCount(field) != reader.maxDoc()) {
          allReadersRewritable = false;
          break;
        }
      } else if (fieldInfo.getVectorDimension() != 0) { // the field indexes vectors
        final int numVectors;
        switch (fieldInfo.getVectorEncoding()) {
          case FLOAT32:
            numVectors = leaf.getFloatVectorValues(field).size();
            break;
          case BYTE:
            numVectors = leaf.getByteVectorValues(field).size();
            break;
          default:
            throw new IllegalArgumentException(
                "unknown vector encoding=" + fieldInfo.getVectorEncoding());
        }
        if (numVectors != leaf.maxDoc()) {
          allReadersRewritable = false;
          break;
        }
      } else if (fieldInfo.getDocValuesType()
          != DocValuesType.NONE) { // the field indexes doc values or points

        // This optimization is possible due to LUCENE-9334 enforcing a field to always uses the
        // same data structures (all or nothing). Since there's no index statistic to detect when
        // all documents have doc values for a specific field, FieldExistsQuery can only be
        // rewritten to MatchAllDocsQuery for doc values field, when that same field also indexes
        // terms or point values which do have index statistics, and those statistics confirm that
        // all documents in this segment have values terms or point values.
        if (hasStrictlyConsistentFieldInfos(context) == false) {
          allReadersRewritable = false;
          break;
        }

        Terms terms = leaf.terms(field);
        PointValues pointValues = leaf.getPointValues(field);

        if ((terms == null || terms.getDocCount() != leaf.maxDoc())
            && (pointValues == null || pointValues.getDocCount() != leaf.maxDoc())) {
          allReadersRewritable = false;
          break;
        }
      } else if (hasStrictlyConsistentFieldInfos(context)) {
        throw new IllegalStateException(buildErrorMsg(fieldInfo));
      }
    }
    if (allReadersRewritable) {
      return new MatchAllDocsQuery();
    }
    return super.rewrite(indexSearcher);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
    return new ConstantScoreWeight(this, boost) {
      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        FieldInfos fieldInfos = context.reader().getFieldInfos();
        FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
        DocIdSetIterator iterator = null;

        if (fieldInfo == null) {
          return null;
        }

        if (fieldInfo.hasNorms()) { // the field indexes norms
          iterator = context.reader().getNormValues(field);
        } else if (fieldInfo.getVectorDimension() != 0) { // the field indexes vectors
          switch (fieldInfo.getVectorEncoding()) {
            case FLOAT32:
              iterator = context.reader().getFloatVectorValues(field);
              break;
            case BYTE:
              iterator = context.reader().getByteVectorValues(field);
              break;
            default:
              throw new IllegalArgumentException(
                  "unknown vector encoding=" + fieldInfo.getVectorEncoding());
          }
        } else if (fieldInfo.getDocValuesType()
            != DocValuesType.NONE) { // the field indexes doc values
          switch (fieldInfo.getDocValuesType()) {
            case NUMERIC:
              iterator = context.reader().getNumericDocValues(field);
              break;
            case BINARY:
              iterator = context.reader().getBinaryDocValues(field);
              break;
            case SORTED:
              iterator = context.reader().getSortedDocValues(field);
              break;
            case SORTED_NUMERIC:
              iterator = context.reader().getSortedNumericDocValues(field);
              break;
            case SORTED_SET:
              iterator = context.reader().getSortedSetDocValues(field);
              break;
            case NONE:
            default:
              throw new AssertionError();
          }
        } else if (hasStrictlyConsistentFieldInfos(context)) {
          throw new IllegalStateException(buildErrorMsg(fieldInfo));
        }

        if (iterator == null) {
          return null;
        }
        return new ConstantScoreScorer(this, score(), scoreMode, iterator);
      }

      @Override
      public int count(LeafReaderContext context) throws IOException {
        LeafReader reader = context.reader();
        FieldInfos fieldInfos = reader.getFieldInfos();
        FieldInfo fieldInfo = fieldInfos.fieldInfo(field);

        if (fieldInfo == null) {
          return 0;
        }

        if (fieldInfo.hasNorms()) { // the field indexes norms
          // If every field has a value then we can shortcut
          if (reader.getDocCount(field) == reader.maxDoc()) {
            return reader.numDocs();
          }

          return super.count(context);
        } else if (fieldInfo.getVectorDimension() != 0) { // the field indexes vectors
          return super.count(context);
        } else if (fieldInfo.getDocValuesType()
            != DocValuesType.NONE) { // the field indexes doc values
          if (reader.hasDeletions() == false) {
            if (fieldInfo.getPointDimensionCount() > 0) {
              PointValues pointValues = reader.getPointValues(field);
              return pointValues == null ? 0 : pointValues.getDocCount();
            } else if (fieldInfo.getIndexOptions() != IndexOptions.NONE) {
              Terms terms = reader.terms(field);
              return terms == null ? 0 : terms.getDocCount();
            }
          }

          return super.count(context);
        }
        if (hasStrictlyConsistentFieldInfos(context)) {
          throw new IllegalStateException(buildErrorMsg(fieldInfo));
        } else {
          return super.count(context);
        }
      }

      @Override
      public boolean isCacheable(LeafReaderContext context) {
        FieldInfos fieldInfos = context.reader().getFieldInfos();
        FieldInfo fieldInfo = fieldInfos.fieldInfo(field);

        if (fieldInfo != null && fieldInfo.getDocValuesType() != DocValuesType.NONE) {
          return DocValues.isCacheable(context, field);
        }

        return true;
      }
    };
  }

  private boolean hasStrictlyConsistentFieldInfos(LeafReaderContext context) {
    return context.reader().getMetaData() != null
        && context.reader().getMetaData().getCreatedVersionMajor() >= 9;
  }

  private String buildErrorMsg(FieldInfo fieldInfo) {
    return "FieldExistsQuery requires that the field indexes doc values, norms or vectors, but field '"
        + fieldInfo.name
        + "' exists and indexes neither of these data structures";
  }
}
