package org.apache.lucene.facet.sandbox.aggregations;

/**
 * Binary operation for longs.
 */
public abstract class LongAggregationFunction {

    /** TODO */
    protected LongAggregationFunction() {
    }

    abstract long aggregate(long existingVal, long newVal);

    /**
     * Aggregation that computes the maximum value
     */
    public static final LongAggregationFunction MAX =
            new LongAggregationFunction() {
                public long aggregate(long existingVal, long newVal) {
                    return Math.max(existingVal, newVal);
                }
            };

    /** Aggregation that computes the sum */
    public static final LongAggregationFunction SUM =
            new LongAggregationFunction() {
                public long aggregate(long existingVal, long newVal) {
                    try {
                        return Math.addExact(existingVal, newVal);
                    } catch (ArithmeticException ae) {
                        // TODO: std exception UndefinedFieldException.throwWhenStrict("Aggregation sum values out of bounds",
                        //        "existing value:" + existingVal + " + " + "new value: " + newVal, null);
                    }
                    return Long.MAX_VALUE;
                }
            };
}
