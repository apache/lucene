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
package org.apache.lucene.search;

import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.util.Bits;

/**
 * Uses {@link KnnVectorsReader#search(String, float[], KnnCollector, Bits)} to perform nearest
 * neighbour search.
 *
 * <p>This query also allows for performing a kNN search subject to a filter. In this case, it first
 * executes the filter for each leaf, then chooses a strategy dynamically:
 *
 * <ul>
 *   <li>If the filter cost is less than k, just execute an exact search
 *   <li>Otherwise run a kNN search subject to the filter
 *   <li>If the kNN search visits too many vectors without completing, stop and run an exact search
 * </ul>
 *
 * @deprecated use {@link KnnFloatVectorQuery} instead
 */
@Deprecated
public class KnnVectorQuery extends KnnFloatVectorQuery {

  public KnnVectorQuery(String field, float[] target, int k) {
    super(field, target, k);
  }

  public KnnVectorQuery(String field, float[] target, int k, Query filter) {
    super(field, target, k, filter);
  }
}
