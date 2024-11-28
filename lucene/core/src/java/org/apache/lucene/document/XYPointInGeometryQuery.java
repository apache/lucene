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
package org.apache.lucene.document;

import static org.apache.lucene.search.TotalHits.Relation.EQUAL_TO;
import static org.apache.lucene.search.TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.XYEncodingUtils;
import org.apache.lucene.geo.XYGeometry;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.DocIdSetBuilder;

/**
 * Finds all previously indexed points that fall within the specified XY geometries.
 *
 * <p>The field must be indexed with using {@link XYPointField} added per document.
 *
 * @lucene.experimental
 */
final class XYPointInGeometryQuery extends Query {
  final String field;
  final XYGeometry[] xyGeometries;

  XYPointInGeometryQuery(String field, XYGeometry... xyGeometries) {
    if (field == null) {
      throw new IllegalArgumentException("field must not be null");
    }
    if (xyGeometries == null) {
      throw new IllegalArgumentException("geometries must not be null");
    }
    if (xyGeometries.length == 0) {
      throw new IllegalArgumentException("geometries must not be empty");
    }
    this.field = field;
    this.xyGeometries = xyGeometries.clone();
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
    }
  }

  private IntersectVisitor getIntersectVisitor(DocIdSetBuilder result, Component2D tree) {
    return new IntersectVisitor() {
      DocIdSetBuilder.BulkAdder adder;

      @Override
      public void grow(int count) {
        adder = result.grow(count);
      }

      @Override
      public void visit(int docID) {
        adder.add(docID);
      }

      @Override
      public void visit(DocIdSetIterator iterator) throws IOException {
        adder.add(iterator);
      }

      @Override
      public void visit(int docID, byte[] packedValue) {
        double x = XYEncodingUtils.decode(packedValue, 0);
        double y = XYEncodingUtils.decode(packedValue, Integer.BYTES);
        if (tree.contains(x, y)) {
          visit(docID);
        }
      }

      @Override
      public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
        double x = XYEncodingUtils.decode(packedValue, 0);
        double y = XYEncodingUtils.decode(packedValue, Integer.BYTES);
        if (tree.contains(x, y)) {
          adder.add(iterator);
        }
      }

      @Override
      public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
        double cellMinX = XYEncodingUtils.decode(minPackedValue, 0);
        double cellMinY = XYEncodingUtils.decode(minPackedValue, Integer.BYTES);
        double cellMaxX = XYEncodingUtils.decode(maxPackedValue, 0);
        double cellMaxY = XYEncodingUtils.decode(maxPackedValue, Integer.BYTES);
        return tree.relate(cellMinX, cellMaxX, cellMinY, cellMaxY);
      }
    };
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {

    final Component2D tree = XYGeometry.create(xyGeometries);

    return new ConstantScoreWeight(this, boost) {

      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        LeafReader reader = context.reader();
        PointValues values = reader.getPointValues(field);
        if (values == null) {
          // No docs in this segment had any points fields
          return null;
        }
        FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field);
        if (fieldInfo == null) {
          // No docs in this segment indexed this field at all
          return null;
        }
        XYPointField.checkCompatible(fieldInfo);

        return new ScorerSupplier() {

          TotalHits estimatedCount;
          DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), values, field);
          final IntersectVisitor visitor = getIntersectVisitor(result, tree);

          @Override
          public Scorer get(long leadCost) throws IOException {
            values.intersect(visitor);
            return new ConstantScoreScorer(score(), scoreMode, result.build().iterator());
          }

          @Override
          public long cost() {
            if (estimatedCount == null || estimatedCount.relation() == GREATER_THAN_OR_EQUAL_TO) {
              // Computing the cost may be expensive, so only do it if necessary
              estimatedCount =
                  new TotalHits(values.estimateDocCount(visitor, Long.MAX_VALUE), EQUAL_TO);
              assert estimatedCount.value() >= 0;
            }
            return estimatedCount.value();
          }

          @Override
          public TotalHits isEstimatedPointCountGreaterThanOrEqualTo(long upperBound) {
            if (estimatedCount == null
                || (estimatedCount.value() < upperBound
                    && estimatedCount.relation() == GREATER_THAN_OR_EQUAL_TO)) {
              long cost = values.estimateDocCount(visitor, upperBound);
              if (cost < upperBound) {
                estimatedCount = new TotalHits(cost, EQUAL_TO);
              } else if (estimatedCount == null || cost > estimatedCount.value()) {
                estimatedCount = new TotalHits(cost, GREATER_THAN_OR_EQUAL_TO);
              }
              assert estimatedCount.value() >= 0;
            }
            return estimatedCount;
          }
        };
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return true;
      }
    };
  }

  /** Returns the query field */
  public String getField() {
    return field;
  }

  /** Returns a copy of the internal geometries array */
  public XYGeometry[] getGeometries() {
    return xyGeometries.clone();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = classHash();
    result = prime * result + field.hashCode();
    result = prime * result + Arrays.hashCode(xyGeometries);
    return result;
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) && equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(XYPointInGeometryQuery other) {
    return field.equals(other.field) && Arrays.equals(xyGeometries, other.xyGeometries);
  }

  @Override
  public String toString(String field) {
    final StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName());
    sb.append(':');
    if (this.field.equals(field) == false) {
      sb.append(" field=");
      sb.append(this.field);
      sb.append(':');
    }
    sb.append(Arrays.toString(xyGeometries));
    return sb.toString();
  }
}
