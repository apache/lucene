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

/** Geospatial search */
@SuppressWarnings({"requires-automatic"})
module org.apache.lucene.spatial_extras {
  requires spatial4j;
  requires s2.geometry.library.java;
  requires org.apache.lucene.core;
  requires org.apache.lucene.spatial3d;

  exports org.apache.lucene.spatial;
  exports org.apache.lucene.spatial.bbox;
  exports org.apache.lucene.spatial.composite;
  exports org.apache.lucene.spatial.prefix;
  exports org.apache.lucene.spatial.prefix.tree;
  exports org.apache.lucene.spatial.query;
  exports org.apache.lucene.spatial.serialized;
  exports org.apache.lucene.spatial.spatial4j;
  exports org.apache.lucene.spatial.util;
  exports org.apache.lucene.spatial.vector;
}
