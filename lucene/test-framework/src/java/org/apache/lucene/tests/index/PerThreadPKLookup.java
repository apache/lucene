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
package org.apache.lucene.tests.index;

import java.io.IOException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.PrimaryKeyLookup;

/**
 * Utility class to do efficient primary-key (only 1 doc contains the given term) lookups by
 * segment, re-using the enums. This class is not thread safe, so it is the caller's job to create
 * and use one instance of this per thread. Do not use this if a term may appear in more than one
 * document! It will only return the first one it finds.
 *
 * @deprecated Use {@link PrimaryKeyLookup} instead.
 */
@Deprecated
public class PerThreadPKLookup extends PrimaryKeyLookup {

  /** Construct a {@code PerThreadPKLookup} bound to {@code reader}. */
  public PerThreadPKLookup(IndexReader reader, String idFieldName) throws IOException {
    super(reader, idFieldName);
  }

  private PerThreadPKLookup(IndexReader reader, PerThreadPKLookup prev) throws IOException {
    super(reader, prev.idFieldName, prev.enumIndexes, prev.termsEnums, prev.postingsEnums);
  }

  /** Narrows {@link PrimaryKeyLookup#reopen} to keep the historical return type. */
  @Override
  public PerThreadPKLookup reopen(IndexReader reader) throws IOException {
    if (reader == null) {
      return null;
    }
    return new PerThreadPKLookup(reader, this);
  }
}
