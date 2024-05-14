package org.apache.lucene.facet.sandbox.aggregations.context;

public interface Reducer {

    default int reduce(int a, int b) {
        throw new UnsupportedOperationException("Incompatible types");
    };
    default long reduce(long a, long b) {
        throw new UnsupportedOperationException("Incompatible types");
    };
    default float reduce(float a, float b) {
        throw new UnsupportedOperationException("Incompatible types");
    };
    default double reduce(double a, double b) {
        throw new UnsupportedOperationException("Incompatible types");
    };

    public static final Reducer MAX = new Reducer() {
        @Override
        public long reduce(long a, long b) {
            return Math.max(a, b);
        }
    };

    public static final Reducer SUM = new Reducer() {
        @Override
        public long reduce(long a, long b) {
            return Math.addExact(a, b);
        }
    };
}
