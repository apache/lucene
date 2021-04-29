package org.apache.lucene.queries.spans;

import static org.apache.lucene.queries.spans.SpanTestUtil.spanFirstQuery;
import static org.apache.lucene.queries.spans.SpanTestUtil.spanNearOrderedQuery;
import static org.apache.lucene.queries.spans.SpanTestUtil.spanNearUnorderedQuery;
import static org.apache.lucene.queries.spans.SpanTestUtil.spanNotQuery;
import static org.apache.lucene.queries.spans.SpanTestUtil.spanOrQuery;
import static org.apache.lucene.queries.spans.SpanTestUtil.spanTermQuery;

import org.apache.lucene.search.BaseExplanationTestCase;

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
