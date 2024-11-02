package org.apache.lucene.sandbox.search;

import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongBitSet;

import java.io.IOException;
import java.util.*;

public class SortedSetMultiRangeQuery extends Query {
    private final String field;
    private final int bytesPerDim;
    List<MultiRangeQuery.RangeClause> rangeClauses;

    SortedSetMultiRangeQuery(String name, List<MultiRangeQuery.RangeClause> clauses, int bytes) {
        this.field = name;
        this.rangeClauses = clauses;
        this.bytesPerDim = bytes;
    }

    public static class Builder {
        final private String name;
        protected final List<MultiRangeQuery.RangeClause> clauses = new ArrayList<>();
        private final int bytes;
        private final ArrayUtil.ByteArrayComparator comparator;

        public Builder(String name, int bytes) {
            this.name = Objects.requireNonNull(name);
            this.bytes = bytes; // TODO assrt positive
            this.comparator = ArrayUtil.getUnsignedComparator(bytes);
        }

        public Builder add(      BytesRef lowerValue,
                                 BytesRef upperValue){ //TODO inc (yes),exc (nope)?
            byte[] low = lowerValue.clone().bytes;
            byte[] up = upperValue.clone().bytes;
            if (this.comparator.compare(low,0,up, 0) > 0) {
                throw new IllegalArgumentException("lowerValue must be <= upperValue");
            } else {
                clauses.add(new MultiRangeQuery.RangeClause(low, up));
            }
            return this;
        }
        public Query build() {
            if(clauses.isEmpty()) {
                return new BooleanQuery.Builder().build();
            }
            if (clauses.size()==1) {
                return SortedSetDocValuesField.newSlowRangeQuery(name, new BytesRef(clauses.get(0).lowerValue), new BytesRef(clauses.get(0).upperValue), true,true);
            }
            clauses.sort(
                    new Comparator<MultiRangeQuery.RangeClause>() {
                        @Override
                        public int compare(MultiRangeQuery.RangeClause o1, MultiRangeQuery.RangeClause o2) {
                            int result = comparator.compare(o1.lowerValue, 0, o2.lowerValue, 0);
                            //if (result == 0) {
                            //    return comparator.compare(o1.upperValue, 0, o2.upperValue, 0);
                            //} else {
                            return result;
                            //}
                        }
                    });
            return new SortedSetMultiRangeQuery(name, clauses, this.bytes);
        }
    }

    @Override
    public String toString(String fld) {
        return "SortedSetMultiRangeQuery{" +
                "field='" + fld + '\'' +
                ", rangeClauses=" + rangeClauses +
                '}';
    }

    //    /**
//     * Merges the overlapping ranges and returns unconnected ranges by calling {@link
//     * #mergeOverlappingRanges}
//     */
//    @Override
//    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
//        if (numDims != 1) {
//            return this;
//        }
//        List<MultiRangeQuery.RangeClause> mergedRanges = mergeOverlappingRanges(rangeClauses, bytesPerDim);
//        if (mergedRanges != rangeClauses) {
//            try {
//                MultiRangeQuery clone = (MultiRangeQuery) super.clone();
//                clone.rangeClauses = mergedRanges;
//                return clone;
//            } catch (CloneNotSupportedException e) {
//                throw new AssertionError(e);
//            }
//        } else {
//            return this;
//        }
//    }
    // what TODO with reverse ranges ???
    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
            throws IOException {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                if (context.reader().getFieldInfos().fieldInfo(field) == null) {
                    return null;
                }
                DocValuesSkipper skipper = context.reader().getDocValuesSkipper(field);
                SortedSetDocValues values = DocValues.getSortedSet(context.reader(), field);
                ArrayUtil.ByteArrayComparator comparator = ArrayUtil.getUnsignedComparator(bytesPerDim);
                // implement ScorerSupplier, since we do some expensive stuff to make a scorer
                return new ScorerSupplier() {
                    @Override
                    public Scorer get(long leadCost) throws IOException {
                        if (rangeClauses.isEmpty()) {
                            return empty();
                        }
                        TermsEnum termsEnum = values.termsEnum();
                        LongBitSet matchingOrdsShifted=null;
                        long minOrd=0, maxOrd=values.getValueCount()-1;
                        long lastMinOrd = values.getValueCount(); // it's last range goes to maxOrd, by default - no match
                        long maxSeenOrd=values.getValueCount();
                        TermsEnum.SeekStatus seekStatus = TermsEnum.SeekStatus.NOT_FOUND;
                        for (int r = 0; r<rangeClauses.size() && seekStatus!= TermsEnum.SeekStatus.END; r++) {
                            MultiRangeQuery.RangeClause range = rangeClauses.get(r);
                            long startingOrd;
                            seekStatus = termsEnum.seekCeil(new BytesRef(range.lowerValue));
                            if (matchingOrdsShifted == null) { // first iter
                                if (seekStatus== TermsEnum.SeekStatus.END) {
                                    return empty(); // no bitset yet, give up
                                }
                                minOrd = termsEnum.ord();
                                if (skipper != null){
                                    minOrd = Math.max(minOrd, skipper.minValue());
                                    maxOrd = Math.min(maxOrd, skipper.maxValue());
                                }
                                if (maxOrd<minOrd) {
                                    return empty();
                                }
                                startingOrd = minOrd;
                            } else {
                                if (seekStatus == TermsEnum.SeekStatus.END) {
                                    break; // ranges - we are done, terms are exhausted
                                } else {
                                    startingOrd = termsEnum.ord();
                                }
                            }
                            byte[] upper = range.upperValue;
                            // looking for overlap
                            for (int overlap = r+1; overlap<rangeClauses.size(); overlap++, r++) {
                                MultiRangeQuery.RangeClause mayOverlap = rangeClauses.get(overlap);
                                assert comparator.compare(range.lowerValue,0, mayOverlap.lowerValue,0)<=0 : "since they are sorted";
                                if (comparator.compare(mayOverlap.lowerValue,0, upper,0)<=0) {
                                    // overlap, expand if needed
                                    if (comparator.compare(upper,0, mayOverlap.upperValue,0)<0) {
                                        upper= mayOverlap.upperValue;
                                    }
                                    continue; // skip overlapping rng
                                } else {
                                    break; // no r++
                                }
                            }
                            seekStatus = termsEnum.seekCeil(new BytesRef(upper));

                            if (seekStatus==TermsEnum.SeekStatus.END) {
                                //maxSeenOrd = maxOrd; // not really necessary
                                lastMinOrd = startingOrd;
                                break; // no need to create bitset
                            }
                            maxSeenOrd = seekStatus == TermsEnum.SeekStatus.FOUND ? termsEnum.ord() : termsEnum.ord()-1; // floor

                            if (matchingOrdsShifted==null) {
                                matchingOrdsShifted = new LongBitSet(maxOrd+1-minOrd);
                            }
                            matchingOrdsShifted.set(startingOrd-minOrd, maxSeenOrd-minOrd+1); // up is exclisive
                        }
                        ///ranges are over, there might be no set!!

//                        if (skipper != null && (minOrd > skipper.maxValue() || maxOrd < skipper.minValue())) {
//                            return new ConstantScoreScorer(score(), scoreMode, DocIdSetIterator.empty());
//                        }

//                        final long minOrd;
//                        if (lowerValue == null) {
//                            minOrd = 0;
//                        } else {
//                            final long ord = values.lookupTerm(lowerValue);
//                            if (ord < 0) {
//                                minOrd = -1 - ord;
//                            } else if (lowerInclusive) {
//                                minOrd = ord;
//                            } else {
//                                minOrd = ord + 1;
//                            }
//                        }
//
//                        final long maxOrd;
//                        if (upperValue == null) {
//                            maxOrd = values.getValueCount() - 1;
//                        } else {
//                            final long ord = values.lookupTerm(upperValue);
//                            if (ord < 0) {
//                                maxOrd = -2 - ord;
//                            } else if (upperInclusive) {
//                                maxOrd = ord;
//                            } else {
//                                maxOrd = ord - 1;
//                            }
//                        }

                        // no terms matched in this segment
//                        if (minOrd > maxOrd
//                                || (skipper != null
//                                && (minOrd > skipper.maxValue() || maxOrd < skipper.minValue()))) {
//                            return new ConstantScoreScorer(score(), scoreMode, DocIdSetIterator.empty());
//                        }

                        // all terms matched in this segment
/*                        if (skipper != null
                                && skipper.docCount() == context.reader().maxDoc()
                                && skipper.minValue() >= minOrd
                                && skipper.maxValue() <= maxOrd) {
                            return new ConstantScoreScorer(
                                    score(), scoreMode, DocIdSetIterator.all(skipper.docCount()));
                        }*/

//                        final SortedDocValues singleton = DocValues.unwrapSingleton(values);
                        TwoPhaseIterator iterator;
//                        if (singleton != null) {
//                            if (skipper != null) {
//                                final DocIdSetIterator psIterator =
//                                        getDocIdSetIteratorOrNullForPrimarySort(
//                                                context.reader(), singleton, skipper, minOrd, maxOrd);
//                                if (psIterator != null) {
//                                    return new ConstantScoreScorer(score(), scoreMode, psIterator);
//                                }
//                            }
//                            iterator =
//                                    new TwoPhaseIterator(singleton) {
//                                        @Override
//                                        public boolean matches() throws IOException {
//                                            final long ord = singleton.ordValue();
//                                            return ord >= minOrd && ord <= maxOrd;
//                                        }
//
//                                        @Override
//                                        public float matchCost() {
//                                            return 2; // 2 comparisons
//                                        }
//                                    };
//                        } else {
                        long finalLastMinOrd = lastMinOrd;
                        LongBitSet finalMatchingOrdsShifted = matchingOrdsShifted;
                        long finalMinOrd = minOrd;
                        iterator =
                                    new TwoPhaseIterator(values) {
                                        @Override
                                        public boolean matches() throws IOException {
                                            // TODO collect all ords, check if cached
                                            for (int i = 0; i < values.docValueCount(); i++) {
                                                long ord = values.nextOrd();
                                                if (ord >=finalMinOrd && (ord>= finalLastMinOrd || finalMatchingOrdsShifted.get(ord- finalMinOrd))) {
                                                    return true;
                                                }
//                                                BytesRef termVal = values.lookupOrd(ord);
//                                                byte[] term = BytesRef.deepCopyOf(termVal).bytes;
//                                                int pos = Collections.binarySearch(rangeClauses, new MultiRangeQuery.RangeClause(term, term), new Comparator<MultiRangeQuery.RangeClause>(){
//                                                    @Override
//                                                    public int compare(MultiRangeQuery.RangeClause rangeClause, MultiRangeQuery.RangeClause t1) {
//                                                        return comparator.compare(rangeClause.lowerValue,0, t1.lowerValue,0);
//                                                    }
//                                                });
//                                                // contained in the list; otherwise, (-(insertion point) - 1). T
//                                                if (pos<0) {
//                                                    pos = -pos-2; //prev to next
//                                                }
//                                                for(int r=0; r<=pos; r++) {
//                                                    if (comparator.compare(term, 0,rangeClauses.get(r).upperValue,0)<=0) {
//                                                        return true;
//                                                    }
//                                                }
                                            }
                                            return false; // all ords were < minOrd
                                        }

                                        @Override
                                        public float matchCost() {
                                            return 2; // 2 comparisons
                                        }
                                    };
//                        }
                        if (skipper != null) {
                            iterator = new DocValuesRangeIterator(iterator, skipper, minOrd, maxSeenOrd//values.getValueCount()
                                    , matchingOrdsShifted!=null);
                        }
                        return new ConstantScoreScorer(score(), scoreMode, iterator);
                    }

                    private ConstantScoreScorer empty() {
                        return new ConstantScoreScorer(score(), scoreMode, DocIdSetIterator.empty());
                    }

                    @Override
                    public long cost() {
                        return values.cost();
                    }
                };
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return DocValues.isCacheable(ctx, field);
            }
        };
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(field)) {
            visitor.visitLeaf(this);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SortedSetMultiRangeQuery that = (SortedSetMultiRangeQuery) o;
        return Objects.equals(field, that.field) && Objects.equals(rangeClauses, that.rangeClauses);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, rangeClauses);
    }
}
