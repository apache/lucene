package org.apache.lucene.sandbox.facet.aggregations;

/**TODO: add doc**/
public interface Reducer {

    /**TODO: add doc**/
    default int reduce(int a, int b) {
        throw new UnsupportedOperationException("Incompatible types");
    };

    /**TODO: add doc**/
    default long reduce(long a, long b) {
        throw new UnsupportedOperationException("Incompatible types");
    };
    /**TODO: add doc**/
    default float reduce(float a, float b) {
        throw new UnsupportedOperationException("Incompatible types");
    };
    /**TODO: add doc**/
    default double reduce(double a, double b) {
        throw new UnsupportedOperationException("Incompatible types");
    };

    /**TODO: add doc**/
    public static final Reducer MAX = new Reducer() {
        @Override
        public long reduce(long a, long b) {
            return Math.max(a, b);
        }
    };

    /**TODO: add doc**/
    public static final Reducer SUM = new Reducer() {
        @Override
        public long reduce(long a, long b) {
            return Math.addExact(a, b);
        }


    };
}
