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

package org.apache.lucene.queries.spans;

import static org.apache.lucene.queries.spans.SpanTestUtil.spanFirstQuery;
import static org.apache.lucene.queries.spans.SpanTestUtil.spanNearOrderedQuery;
import static org.apache.lucene.queries.spans.SpanTestUtil.spanNearUnorderedQuery;
import static org.apache.lucene.queries.spans.SpanTestUtil.spanNotQuery;
import static org.apache.lucene.queries.spans.SpanTestUtil.spanOrQuery;
import static org.apache.lucene.queries.spans.SpanTestUtil.spanTermQuery;

import org.apache.lucene.tests.search.BaseExplanationTestCase;

public abstract class BaseSpanExplanationTestCase extends BaseExplanationTestCase {

  /** MACRO for SpanTermQuery */
  public SpanQuery st(String s) {
    return spanTermQuery(FIELD, s);
  }

  /** MACRO for SpanNotQuery */
  public SpanQuery snot(SpanQuery i, SpanQuery e) {
    return spanNotQuery(i, e);
  }

  /** MACRO for SpanOrQuery containing two SpanTerm queries */
  public SpanQuery sor(String s, String e) {
    return spanOrQuery(FIELD, s, e);
  }

  /** MACRO for SpanOrQuery containing two SpanQueries */
  public SpanQuery sor(SpanQuery s, SpanQuery e) {
    return spanOrQuery(s, e);
  }

  /** MACRO for SpanOrQuery containing three SpanTerm queries */
  public SpanQuery sor(String s, String m, String e) {
    return spanOrQuery(FIELD, s, m, e);
  }

  /** MACRO for SpanOrQuery containing two SpanQueries */
  public SpanQuery sor(SpanQuery s, SpanQuery m, SpanQuery e) {
    return spanOrQuery(s, m, e);
  }

  /** MACRO for SpanNearQuery containing two SpanTerm queries */
  public SpanQuery snear(String s, String e, int slop, boolean inOrder) {
    return snear(st(s), st(e), slop, inOrder);
  }

  /** MACRO for SpanNearQuery containing two SpanQueries */
  public SpanQuery snear(SpanQuery s, SpanQuery e, int slop, boolean inOrder) {
    if (inOrder) {
      return spanNearOrderedQuery(slop, s, e);
    } else {
      return spanNearUnorderedQuery(slop, s, e);
    }
  }

  /** MACRO for SpanNearQuery containing three SpanTerm queries */
  public SpanQuery snear(String s, String m, String e, int slop, boolean inOrder) {
    return snear(st(s), st(m), st(e), slop, inOrder);
  }

  /** MACRO for SpanNearQuery containing three SpanQueries */
  public SpanQuery snear(SpanQuery s, SpanQuery m, SpanQuery e, int slop, boolean inOrder) {
    if (inOrder) {
      return spanNearOrderedQuery(slop, s, m, e);
    } else {
      return spanNearUnorderedQuery(slop, s, m, e);
    }
  }

  /** MACRO for SpanFirst(SpanTermQuery) */
  public SpanQuery sf(String s, int b) {
    return spanFirstQuery(st(s), b);
  }
}
