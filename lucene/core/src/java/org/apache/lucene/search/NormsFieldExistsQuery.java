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

import org.apache.lucene.document.StringField;

/**
 * A {@link Query} that matches documents that have a value for a given field as reported by field
 * norms. This will not work for fields that omit norms, e.g. {@link StringField}.
 *
 * @deprecated Use {@link org.apache.lucene.search.FieldExistsQuery} instead.
 */
@Deprecated
public final class NormsFieldExistsQuery extends FieldExistsQuery {

  /** Create a query that will match that have a value for the given {@code field}. */
  public NormsFieldExistsQuery(String field) {
    super(field);
  }
}
