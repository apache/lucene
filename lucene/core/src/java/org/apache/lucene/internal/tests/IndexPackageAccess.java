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
package org.apache.lucene.internal.tests;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Impacts;
import org.apache.lucene.index.IndexReader;

/**
 * Access to {@link org.apache.lucene.index} package internals exposed to the test framework.
 *
 * @lucene.internal
 */
public interface IndexPackageAccess {
  IndexReader.CacheKey newCacheKey();

  void setIndexWriterMaxDocs(int limit);

  FieldInfosBuilder newFieldInfosBuilder(String softDeletesFieldName);

  void checkImpacts(Impacts impacts, int max);

  /** Public type exposing {@link FieldInfo} internal builders. */
  interface FieldInfosBuilder {
    FieldInfosBuilder add(FieldInfo fi);

    FieldInfos finish();
  }
}
