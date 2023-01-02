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
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;

/**
 * Aggregates sum of values from {@link DoubleValues#doubleValue()}, for each facet label.
 *
 * @lucene.experimental
 * @deprecated This class is being deprecated in favor of {@link TaxonomyFacetFloatAssociations},
 *     which provides more flexible aggregation functionality beyond just "sum"
 */
@Deprecated
public class TaxonomyFacetSumValueSource extends TaxonomyFacetFloatAssociations {

  /**
   * Aggregates double facet values from the provided {@link DoubleValuesSource}, pulling ordinals
   * from the default indexed facet field {@link FacetsConfig#DEFAULT_INDEX_FIELD_NAME}.
   */
  public TaxonomyFacetSumValueSource(
      TaxonomyReader taxoReader,
      FacetsConfig config,
      FacetsCollector fc,
      DoubleValuesSource valueSource)
      throws IOException {
    this(FacetsConfig.DEFAULT_INDEX_FIELD_NAME, taxoReader, config, fc, valueSource);
  }

  /**
   * Aggregates double facet values from the provided {@link DoubleValuesSource}, pulling ordinals
   * from the specified indexed facet field.
   */
  public TaxonomyFacetSumValueSource(
      String indexField,
      TaxonomyReader taxoReader,
      FacetsConfig config,
      FacetsCollector fc,
      DoubleValuesSource valueSource)
      throws IOException {
    super(indexField, taxoReader, config, fc, AssociationAggregationFunction.SUM, valueSource);
  }

  /**
   * Aggregates float facet values from the provided {@link DoubleValuesSource}, and pulls ordinals
   * from the provided {@link OrdinalsReader}.
   *
   * @deprecated Custom binary encodings for taxonomy ordinals are no longer supported starting with
   *     Lucene 9
   */
  @Deprecated
  public TaxonomyFacetSumValueSource(
      OrdinalsReader ordinalsReader,
      TaxonomyReader taxoReader,
      FacetsConfig config,
      FacetsCollector fc,
      DoubleValuesSource vs)
      throws IOException {
    super(ordinalsReader, taxoReader, config, fc, AssociationAggregationFunction.SUM, vs);
  }
}
