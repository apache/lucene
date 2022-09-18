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
package org.apache.lucene.facet.sortedset;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;

/**
 * Get reader states after a refresh. Example call: readerStates =
 * searcherManager.maybeRefreshAndReturnState(new SSDVReaderStatesCalculator(), config);
 */
public class SSDVReaderStatesCalculator
    implements ReferenceManager.StateCalculator<
        List<SortedSetDocValuesReaderState>, IndexSearcher, FacetsConfig> {

  /** No arguments needed */
  public SSDVReaderStatesCalculator() {}

  @Override
  public List<SortedSetDocValuesReaderState> calculate(IndexSearcher current, FacetsConfig config) {
    return config.getDimConfigs().keySet().stream()
        .map(
            field -> {
              try {
                return new DefaultSortedSetDocValuesReaderState(
                    current.getIndexReader(), field, config);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            })
        .collect(Collectors.toList());
  }
}
