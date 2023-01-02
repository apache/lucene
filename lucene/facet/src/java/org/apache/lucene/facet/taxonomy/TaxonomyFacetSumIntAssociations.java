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

/**
 * Aggregates sum of int values previously indexed with {@link IntAssociationFacetField}, assuming
 * the default encoding.
 *
 * @lucene.experimental
 * @deprecated This class is being deprecated in favor of {@link TaxonomyFacetIntAssociations},
 *     which provides more flexible aggregation functionality beyond just "sum"
 */
@Deprecated
public class TaxonomyFacetSumIntAssociations extends TaxonomyFacetIntAssociations {

  /** Create {@code TaxonomyFacetSumIntAssociations} against the default index field. */
  public TaxonomyFacetSumIntAssociations(
      TaxonomyReader taxoReader, FacetsConfig config, FacetsCollector fc) throws IOException {
    this(FacetsConfig.DEFAULT_INDEX_FIELD_NAME, taxoReader, config, fc);
  }

  /** Create {@code TaxonomyFacetSumIntAssociations} against the specified index field. */
  public TaxonomyFacetSumIntAssociations(
      String indexFieldName, TaxonomyReader taxoReader, FacetsConfig config, FacetsCollector fc)
      throws IOException {
    super(indexFieldName, taxoReader, config, fc, AssociationAggregationFunction.SUM);
  }
}
