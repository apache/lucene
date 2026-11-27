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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.index.IndexReader;

/**
 * This class demonstrates how Lucene could help a user build SSDV ordinal maps eagerly, on refresh.
 * This class offers:
 *
 * <p>1. A centralized store of SSDV reader states.
 *
 * <p>2. A mechanism to rebuild all reader states the user needs.
 */
public class SsdvReaderStatesManager {
  /** Use the {@link FacetsConfig} to find out which fields are used for faceting. */
  FacetsConfig facetsConfig;

  /**
   * The values in this map are mappings of fields to reader states. Each reader state will be a
   * {@link DefaultSortedSetDocValuesReaderState} with an ordinal map for the field. These mappings
   * are keyed by {@link IndexReader} to account for multiple active index readers during refresh.
   */
  private final Map<IndexReader, Map<String, SortedSetDocValuesReaderState>> readerStates =
      new HashMap<>();

  /** Accept a {@link FacetsConfig}. */
  public SsdvReaderStatesManager(FacetsConfig facetsConfig) {
    this.facetsConfig = facetsConfig;
  }

  /**
   * Retrieve and, if necessary, create reader states for all the fields in the {@link
   * FacetsConfig}.
   */
  public Set<SortedSetDocValuesReaderState> getReaderStates(IndexReader indexReader)
      throws IOException {
    Set<SortedSetDocValuesReaderState> states = new HashSet<>();
    for (String field : facetsConfig.getFieldNames()) {
      states.add(getReaderState(indexReader, field));
    }
    return states;
  }

  /** Retrieve and, if necessary, create the reader state for the default field. */
  public SortedSetDocValuesReaderState getReaderState(IndexReader indexReader) throws IOException {
    return getReaderState(indexReader, FacetsConfig.DEFAULT_INDEX_FIELD_NAME);
  }

  /**
   * Retrieve and, if necessary, create the reader state for a named field. Store the reader state
   * for re-use.
   */
  public SortedSetDocValuesReaderState getReaderState(IndexReader indexReader, String field)
      throws IOException {
    Map<String, SortedSetDocValuesReaderState> states = readerStates.get(indexReader);
    if (states == null) {
      states = new HashMap<>();
      readerStates.put(indexReader, states);
    }

    SortedSetDocValuesReaderState state = states.get(field);
    if (state == null) {
      state = new DefaultSortedSetDocValuesReaderState(indexReader, field, facetsConfig);
      states.put(field, state);
    }
    return state;
  }
}
