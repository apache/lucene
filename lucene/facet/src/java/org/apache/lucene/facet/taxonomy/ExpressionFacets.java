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
package org.apache.lucene.facet.taxonomy;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.lucene.expressions.Bindings;
import org.apache.lucene.expressions.SimpleBindings;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.FixedBitSet;

/**
 * Facet by a provided expression string. Variables in the expression string are expected to be of
 * the form: {@code val_<binding_name>_[sum|max]}, where {@code <binding_name>} must be a valid
 * reference in the provided {@link Bindings}.
 *
 * <p>nocommit: This is a simple demo and is incomplete and not tested.
 */
public class ExpressionFacets extends FloatTaxonomyFacets {
  private static final Pattern RE = Pattern.compile("(?=(val_.+_(max|sum)))");
  private static final Map<String, AssociationAggregationFunction> F_MAP = new HashMap<>();

  static {
    F_MAP.put("sum", AssociationAggregationFunction.SUM);
    F_MAP.put("max", AssociationAggregationFunction.MAX);
  }

  public ExpressionFacets(
      String indexFieldName,
      TaxonomyReader taxoReader,
      FacetsConfig config,
      FacetsCollector facetsCollector,
      String expression,
      Bindings bindings)
      throws IOException, ParseException {
    super(indexFieldName, taxoReader, AssociationAggregationFunction.SUM, config);
    Set<FieldAggregation> aggregations = parseAggregations(expression);
    SimpleBindings aggregationBindings = new SimpleBindings();
    aggregate(expression, bindings, aggregationBindings, aggregations, facetsCollector);
  }

  /**
   * Identify the component aggregations needed by the expression. String parsing is not my strong
   * suit, so I'm sure this is clunky at best and buggy at worst.
   */
  private static Set<FieldAggregation> parseAggregations(String expression) {
    Set<FieldAggregation> result = new HashSet<>();
    Matcher m = RE.matcher(expression);
    while (m.find()) {
      int start = m.start();
      int sumPos = expression.indexOf("sum", start);
      if (sumPos == -1) {
        sumPos = Integer.MAX_VALUE;
      }
      int maxPos = expression.indexOf("max", start);
      if (maxPos == -1) {
        maxPos = Integer.MAX_VALUE;
      }
      int end = Math.min(sumPos, maxPos);
      if (end == Integer.MAX_VALUE) {
        throw new IllegalArgumentException("Invalid syntax");
      }
      end += 3;
      String component = expression.substring(start, end);
      String[] tokens = component.split("_");
      if (tokens.length < 3 || "val".equals(tokens[0]) == false) {
        throw new IllegalArgumentException("Invalid syntax");
      }
      AssociationAggregationFunction func =
          F_MAP.get(tokens[tokens.length - 1].toLowerCase(Locale.ROOT));
      String ref;
      if (tokens.length > 3) {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i < tokens.length - 1; i++) {
          sb.append(tokens[i]);
          if (i < tokens.length - 2) {
            sb.append("_");
          }
        }
        ref = sb.toString();
      } else {
        ref = tokens[1];
      }
      result.add(new FieldAggregation(component, ref, func));
    }

    return result;
  }

  private void aggregate(
      String expression,
      Bindings bindings,
      SimpleBindings aggregationBindings,
      Set<FieldAggregation> aggregations,
      FacetsCollector facetsCollector)
      throws IOException, ParseException {
    // Compute component aggregations:
    for (FieldAggregation fa : aggregations) {
      // Leverage association faceting to compute each individual component aggregation:
      DoubleValuesSource dvs = bindings.getDoubleValuesSource(fa.valueSource);
      TaxonomyFacetFloatAssociations f =
          new TaxonomyFacetFloatAssociations(
              indexFieldName, taxoReader, config, facetsCollector, fa.aggregationFunction, dvs);
      // Wrap the computed aggregation values in a DoubleValuesSource that's ordinal-based and
      // register it with the bindings keyed on the original expression binding name:
      float[] values = f.values;
      AggregationBinding binding = new AggregationBinding(fa.id, values);
      aggregationBindings.add(fa.id, binding);
    }

    // Find all the unique ordinals represented in the facets collector:
    FixedBitSet ords = new FixedBitSet(taxoReader.getSize());
    for (FacetsCollector.MatchingDocs hits : facetsCollector.getMatchingDocs()) {
      SortedNumericDocValues dv = DocValues.getSortedNumeric(hits.context.reader(), indexFieldName);
      DocIdSetIterator it = ConjunctionUtils.intersectIterators(List.of(dv, hits.bits.iterator()));
      for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
        for (int i = 0; i < dv.docValueCount(); i++) {
          ords.set((int) dv.nextValue());
        }
      }
    }

    // Assign a new value to each ordinal observed in our facets collector by evaluating the
    // expression:
    // Ordinal-based double values sources; global (null context is ok):
    DoubleValues expressionValues =
        JavascriptCompiler.compile(expression)
            .getDoubleValuesSource(aggregationBindings)
            .getValues(null, null);
    int ord = -1;
    while (true) {
      ord = ords.nextSetBit(ord + 1);
      if (ord == DocIdSetIterator.NO_MORE_DOCS) {
        break;
      }
      if (expressionValues.advanceExact(ord)) {
        values[ord] = (float) expressionValues.doubleValue();
      }
    }
  }

  private record FieldAggregation(
      String id, String valueSource, AssociationAggregationFunction aggregationFunction) {}

  private static class AggregationBinding extends DoubleValuesSource {
    private final String name;
    private final float[] values;

    AggregationBinding(String name, float[] values) {
      this.name = name;
      this.values = values;
    }

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      return new AggregationBindingDoubleValues(values);
    }

    @Override
    public boolean needsScores() {
      return false;
    }

    @Override
    public DoubleValuesSource rewrite(IndexSearcher reader) throws IOException {
      return this;
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, Arrays.hashCode(values));
    }

    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      }
      if (other == null) {
        return false;
      }
      if (other instanceof AggregationBinding == false) {
        return false;
      }
      AggregationBinding o = (AggregationBinding) other;
      return name.equals(o.name) && Arrays.equals(values, o.values);
    }

    @Override
    public String toString() {
      return "AggregationBinding: " + name;
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }
  }

  private static class AggregationBindingDoubleValues extends DoubleValues {
    private final float[] values;
    private int ord;

    AggregationBindingDoubleValues(float[] values) {
      this.values = values;
    }

    @Override
    public double doubleValue() throws IOException {
      return values[ord];
    }

    @Override
    public boolean advanceExact(int ord) throws IOException {
      this.ord = ord;
      return ord < values.length;
    }
  }
}
