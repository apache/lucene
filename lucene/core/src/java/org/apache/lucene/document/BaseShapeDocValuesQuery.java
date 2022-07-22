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
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.Geometry;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.util.BytesRef;

/**
 * Base query class for ShapeDocValues queries. Concrete implementations include: {@link
 * LatLonShapeDocValuesQuery} and {@link XYShapeDocValuesQuery}
 *
 * @lucene.experimental
 */
abstract class BaseShapeDocValuesQuery extends SpatialQuery {

  BaseShapeDocValuesQuery(String field, QueryRelation queryRelation, Geometry... geometries) {
    super(field, validateRelation(queryRelation), geometries);
  }

  private static QueryRelation validateRelation(QueryRelation queryRelation) {
    if (queryRelation == QueryRelation.CONTAINS) {
      throw new IllegalArgumentException(
          "ShapeDocValuesBoundingBoxQuery does not yet support CONTAINS queries");
    }
    return queryRelation;
  }

  protected abstract ShapeDocValues getShapeDocValues(BytesRef binaryValue);

  @Override
  protected ScorerSupplier getScorerSupplier(
      LeafReader reader,
      SpatialVisitor spatialVisitor,
      ScoreMode scoreMode,
      ConstantScoreWeight weight,
      float boost,
      float score)
      throws IOException {
    final BinaryDocValues values = reader.getBinaryDocValues(field);
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
            return match(getShapeDocValues(values.binaryValue()));
          }

          @Override
          public float matchCost() {
            return BaseShapeDocValuesQuery.this.matchCost();
          }
        };
    return new ScorerSupplier() {

      @Override
      public Scorer get(long leadCost) {
        return new ConstantScoreScorer(weight, boost, scoreMode, iterator);
      }

      @Override
      public long cost() {
        return reader.maxDoc();
      }
    };
  }

  /** matches the doc value to the query; overridable to provide custom query logic */
  protected boolean match(ShapeDocValues shapeDocValues) throws IOException {
    boolean result = matchesComponent(shapeDocValues, queryRelation, queryComponent2D);
    if (queryRelation == QueryRelation.DISJOINT) {
      return result == false;
    }
    return result;
  }

  /** compute the cost of the query; overrideable */
  protected float matchCost() {
    // multiply comparisons (estimated 60) by number of terms (averaged at 100)
    // todo: revisit
    return 60 * 100;
  }

  protected boolean matchesComponent(
      final ShapeDocValues dv,
      final ShapeField.QueryRelation queryRelation,
      final Component2D component)
      throws IOException {
    Relation r = dv.relate(component);
    if (r != Relation.CELL_OUTSIDE_QUERY) {
      if (queryRelation == ShapeField.QueryRelation.WITHIN) {
        return r == Relation.CELL_INSIDE_QUERY;
      }
      return true;
    }
    return false;
  }
}
