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

/** Faceted indexing and search capabilities */
@SuppressWarnings({"requires-automatic"})
module org.apache.lucene.facet {
  requires com.carrotsearch.hppc;
  requires org.apache.lucene.core;

  exports org.apache.lucene.facet;
  exports org.apache.lucene.facet.range;
  exports org.apache.lucene.facet.sortedset;
  exports org.apache.lucene.facet.taxonomy;
  exports org.apache.lucene.facet.taxonomy.directory;
  exports org.apache.lucene.facet.taxonomy.writercache;
  exports org.apache.lucene.facet.facetset;
}
