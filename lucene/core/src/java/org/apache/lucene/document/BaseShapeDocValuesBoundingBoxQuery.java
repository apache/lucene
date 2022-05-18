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

import java.io.IOException;
import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;

/**
 * Base doc values bounding box query for {@link LatLonShapeDocValuesBoundingBoxQuery} and {@link
 * XYShapeDocValuesBoundingBoxQuery}
 */
abstract class BaseShapeDocValuesBoundingBoxQuery extends Query {

  protected final String field;
  protected final QueryRelation queryRelation;
  protected final int minX;
  protected final int maxX;
  protected final int minY;
  protected final int maxY;

  BaseShapeDocValuesBoundingBoxQuery(
      String field, QueryRelation queryRelation, int minX, int maxX, int minY, int maxY) {
    if (field == null) {
      throw new IllegalArgumentException("field must not be null");
    }
    this.field = field;
    this.queryRelation = validateRelation(queryRelation);
    this.minX = minX;
    this.maxX = maxX;
    this.minY = minY;
    this.maxY = maxY;
  }

  private static QueryRelation validateRelation(QueryRelation queryRelation) {
    if (queryRelation == QueryRelation.CONTAINS) {
      throw new IllegalArgumentException(
          "ShapeDocValuesBoundingBoxQuery does not yet support CONTAINS queries");
    }
    return queryRelation;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return new ConstantScoreWeight(this, boost) {
      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        final LeafReader reader = context.reader();
        final BinaryDocValues values = context.reader().getBinaryDocValues(field);
        if (values == null) {
          return null;
        }
        final FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field);
        if (fieldInfo == null) {
          // No docs in this segment indexed this field at all
          return null;
        }

        final TwoPhaseIterator iterator =
            new TwoPhaseIterator(values) {
              @Override
              public boolean matches() throws IOException {
                ShapeDocValuesField shapeDVField =
                    new ShapeDocValuesField(field, values.binaryValue());
                return match(shapeDVField);
              }

              @Override
              public float matchCost() {
                return BaseShapeDocValuesBoundingBoxQuery.this.matchCost();
              }
            };
        return new ConstantScoreScorer(this, boost, scoreMode, iterator);
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return DocValues.isCacheable(ctx, field);
      }
    };
  }

  public boolean match(ShapeDocValuesField shapeDocValues) throws IOException {
    boolean result = matchesBox(shapeDocValues, queryRelation, minX, maxX, minY, maxY);
    if (queryRelation == QueryRelation.DISJOINT) {
      return result == false;
    }
    return result;
  }

  public float matchCost() {
    return 60 * 100;
  }

  protected boolean matchesBox(
      final ShapeDocValuesField shapeDVField,
      final ShapeField.QueryRelation queryRelation,
      final int minX,
      final int maxX,
      final int minY,
      final int maxY)
      throws IOException {
    PointValues.Relation r = shapeDVField.relate(minX, maxX, minY, maxY);
    if (r != PointValues.Relation.CELL_OUTSIDE_QUERY) {
      if (queryRelation == ShapeField.QueryRelation.WITHIN) {
        return r == PointValues.Relation.CELL_INSIDE_QUERY;
      }
      return true;
    }
    return false;
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (sameClassAs(obj) == false) {
      return false;
    }
    BaseShapeDocValuesBoundingBoxQuery other = (BaseShapeDocValuesBoundingBoxQuery) obj;
    return field.equals(other.field)
        && queryRelation == other.queryRelation
        && minX == other.minX
        && maxX == other.maxX
        && minY == other.minY
        && maxY == other.maxY;
  }

  @Override
  public int hashCode() {
    int h = classHash();
    h = 31 * h + field.hashCode();
    h = 31 * h + queryRelation.hashCode();
    h = 31 * h + Integer.hashCode(minX);
    h = 31 * h + Integer.hashCode(maxX);
    h = 31 * h + Integer.hashCode(minY);
    h = 31 * h + Integer.hashCode(maxY);
    return h;
  }

  @Override
  public String toString(String field) {
    StringBuilder sb = new StringBuilder();
    if (this.field.equals(field) == false) {
      sb.append(this.field);
      sb.append(':');
    }
    sb.append("box(minX=").append(minX);
    sb.append(", maxX=").append(maxX);
    sb.append(", minY=").append(minY);
    sb.append(", maxY=").append(maxY);
    return sb.append("), queryRelation=" + queryRelation).toString();
  }
}
