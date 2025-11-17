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
 * Sandbox faceting - Collectors that compute facets.
 *
 * <p>See {@link org.apache.lucene.sandbox.facet.utils} for simple API for most common use cases.
 *
 * <p>Lower level API is based on following concepts:
 *
 * <ul>
 *   <li>Facet Ordinals/Ids: Each doc may have different facets and therefore, different facet
 *       ordinals. For example a book can have Author, Publish Date, Page Count etc. as facets. The
 *       specific value for each of these Facets for a book can be mapped to an ordinal. Facet
 *       ordinals may be common across different book documents.
 *   <li>Facet Cutter: Can interpret Facets of a specific type for a doc type and output all the
 *       Facet Ordinals for the type for the doc.
 *   <li>Facet Recorders: record data per ordinal. Some recorders may compute aggregations and
 *       record per ordinal data aggregated across an index.
 * </ul>
 *
 * <p>See SandboxFacetsExample for examples.
 *
 * @lucene.experimental
 */
package org.apache.lucene.sandbox.facet;
