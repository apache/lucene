package org.apache.lucene.sandbox.facet.abstracts;

/** Reducer for numeric values. **/
public interface Reducer {

    /** Int values reducer. **/
    default int reduce(int a, int b) {
        throw new UnsupportedOperationException("Incompatible types");
    };

    /** Long values reducer. **/
    default long reduce(long a, long b) {
        throw new UnsupportedOperationException("Incompatible types");
    };

    /** Float values reducer. **/
    default float reduce(float a, float b) {
        throw new UnsupportedOperationException("Incompatible types");
    };

    /** Double values reducer. **/
    default double reduce(double a, double b) {
        throw new UnsupportedOperationException("Incompatible types");
    };

    /** Reducer that returns MAX of two values. **/
    public static final Reducer MAX = new Reducer() {
        @Override
        public long reduce(long a, long b) {
            return Math.max(a, b);
        }
    };

    /** Reducer that returns SUM of two values. **/
    public static final Reducer SUM = new Reducer() {
        @Override
        public long reduce(long a, long b) {
            return Math.addExact(a, b);
        }


    };
}
