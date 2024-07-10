package org.apache.lucene.facet.sandbox.aggregations;

/**
 * Binary operation for longs.
 */
public abstract class AssociationLongAggregationFunction {
    protected AssociationLongAggregationFunction() {
    }

    abstract long aggregateLong(long existingVal, long newVal);

    /**
     * Aggregation that computes the maximum value
     */
    public static final AssociationLongAggregationFunction MAX =
            new AssociationLongAggregationFunction() {
                public long aggregateLong(long existingVal, long newVal) {
                    return Math.max(existingVal, newVal);
                }
            };

    /** Aggregation that computes the sum */
    public static final AssociationLongAggregationFunction SUM =
            new AssociationLongAggregationFunction() {
                public long aggregateLong(long existingVal, long newVal) {
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
