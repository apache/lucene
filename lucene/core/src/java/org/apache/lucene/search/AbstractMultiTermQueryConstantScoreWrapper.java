package org.apache.lucene.search;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;
import java.util.List;

/**
 * Contains functionality common to both {@link MultiTermQueryConstantScoreBlendedWrapper} and
 * {@link MultiTermQueryConstantScoreWrapper}. Internal implementation detail only. Not meant
 * as an extension point for users.
 *
 * @lucene.internal
 */
abstract class AbstractMultiTermQueryConstantScoreWrapper<Q extends MultiTermQuery> extends Query
    implements Accountable {
  // mtq that matches 16 terms or less will be executed as a regular disjunction
  private static final int BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD = 16;

  protected final Q query;

  AbstractMultiTermQueryConstantScoreWrapper(Q query) {
    this.query = query;
  }

  @Override
  public long ramBytesUsed() {
    if (query instanceof Accountable) {
      return RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
          + RamUsageEstimator.NUM_BYTES_OBJECT_REF
          + ((Accountable) query).ramBytesUsed();
    }
    return RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
        + RamUsageEstimator.NUM_BYTES_OBJECT_REF
        + RamUsageEstimator.QUERY_DEFAULT_RAM_BYTES_USED;
  }

  @Override
  public String toString(String field) {
    // query.toString should be ok for the filter, too, if the query boost is 1.0f
    return query.toString(field);
  }

  @Override
  public boolean equals(final Object other) {
    return sameClassAs(other)
        && query.equals(((AbstractMultiTermQueryConstantScoreWrapper<?>) other).query);
  }

  @Override
  public int hashCode() {
    return 31 * classHash() + query.hashCode();
  }

  /** Returns the encapsulated query */
  public Q getQuery() {
    return query;
  }

  /** Returns the field name for this query */
  public String getField() {
    return query.getField();
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(getField())) {
      query.visit(visitor.getSubVisitor(BooleanClause.Occur.FILTER, this));
    }
  }

  protected static final class TermAndState {
    final BytesRef term;
    final TermState state;
    final int docFreq;
    final long totalTermFreq;

    TermAndState(BytesRef term, TermState state, int docFreq, long totalTermFreq) {
      this.term = term;
      this.state = state;
      this.docFreq = docFreq;
      this.totalTermFreq = totalTermFreq;
    }
  }

  protected static abstract class WrapperWeight extends ConstantScoreWeight {
    private final MultiTermQuery q;
    private final ScoreMode scoreMode;

    WrapperWeight(MultiTermQuery q, float boost, ScoreMode scoreMode) {
      super(q, boost);
      this.q = q;
      this.scoreMode = scoreMode;
    }

    protected boolean collectTerms(int fieldDocCount, TermsEnum termsEnum, List<TermAndState> terms)
        throws IOException {
      final int threshold =
          Math.min(BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD, IndexSearcher.getMaxClauseCount());
      for (int i = 0; i < threshold; ++i) {
        final BytesRef term = termsEnum.next();
        if (term == null) {
          return true;
        }
        TermState state = termsEnum.termState();
        int docFreq = termsEnum.docFreq();
        TermAndState termAndState =
            new TermAndState(
                BytesRef.deepCopyOf(term), state, docFreq, termsEnum.totalTermFreq());
        if (fieldDocCount == docFreq) {
          // If the term contains every document with a value for the field, we can ignore all
          // other terms:
          terms.clear();
          terms.add(termAndState);
          return true;
        }
        terms.add(termAndState);
      }
      return termsEnum.next() == null;
    }

    @Override
    public Matches matches(LeafReaderContext context, int doc) throws IOException {
      final Terms terms = context.reader().terms(q.field);
      if (terms == null) {
        return null;
      }
      return MatchesUtils.forField(
          q.field,
          () ->
              DisjunctionMatchesIterator.fromTermsEnum(
                  context, doc, q, q.field, q.getTermsEnum(terms)));
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return true;
    }
  }
}
