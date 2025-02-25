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
/**
 * Sandbox faceting: utility classes to make it easier to use the new API for standard use cases.
 *
 * <p>Create a {@link org.apache.lucene.sandbox.facet.utils.FacetBuilder} for each facet request you
 * have, use {@link org.apache.lucene.sandbox.facet.utils.FacetOrchestrator} or {@link
 * org.apache.lucene.sandbox.facet.utils.DrillSidewaysFacetOrchestrator} to complete facet requests
 * for a query, and call {@link org.apache.lucene.sandbox.facet.utils.FacetBuilder#getResult()} to
 * get final results.
 *
 * <p>Which {@link org.apache.lucene.sandbox.facet.utils.FacetBuilder} to use?
 *
 * <ul>
 *   <li>Use implementation specific to your use case, e.g. {@link
 *       org.apache.lucene.sandbox.facet.utils.TaxonomyFacetBuilder} for taxonomy fields ({@link
 *       org.apache.lucene.facet.FacetField}), {@link
 *       org.apache.lucene.sandbox.facet.utils.RangeFacetBuilderFactory} for range facets, {@link
 *       org.apache.lucene.sandbox.facet.utils.LongValueFacetBuilder} for numeric fields.
 *   <li>Use {@link org.apache.lucene.sandbox.facet.utils.CommonFacetBuilder} for other cases where
 *       you can provide {@link org.apache.lucene.sandbox.facet.cutters.FacetCutter} and {@link
 *       org.apache.lucene.sandbox.facet.labels.OrdToLabel}.
 * </ul>
 *
 * <p>There is no implementation for your use case?
 *
 * <ul>
 *   <li>Implement {@link org.apache.lucene.sandbox.facet.cutters.FacetCutter}, {@link
 *       org.apache.lucene.sandbox.facet.labels.OrdToLabel} and/or {@link
 *       org.apache.lucene.sandbox.facet.recorders.FacetRecorder} for your use case. See {@link
 *       org.apache.lucene.sandbox.facet} for low level API overview.
 *   <li>Implement {@link org.apache.lucene.sandbox.facet.utils.FacetBuilder} for your use case
 *       unless {@link org.apache.lucene.sandbox.facet.utils.CommonFacetBuilder} is sufficient, or
 *       use low level API directly.
 * </ul>
 *
 * <p>See SandboxFacetsExample for examples.
 *
 * @lucene.experimental
 */
package org.apache.lucene.sandbox.facet.utils;
