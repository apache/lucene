package org.apache.lucene.util.hppc;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Constants for primitive maps.
 */
public class HashContainers {

    public static final int DEFAULT_EXPECTED_ELEMENTS = 4;

    public static final float DEFAULT_LOAD_FACTOR = 0.75f;

    /** Minimal sane load factor (99 empty slots per 100). */
    public static final float MIN_LOAD_FACTOR = 1 / 100.0f;

    /** Maximum sane load factor (1 empty slot per 100). */
    public static final float MAX_LOAD_FACTOR = 99 / 100.0f;

    /** Minimum hash buffer size. */
    public static final int MIN_HASH_ARRAY_LENGTH = 4;

    /**
     * Maximum array size for hash containers (power-of-two and still allocable in Java, not a
     * negative int).
     */
    public static final int MAX_HASH_ARRAY_LENGTH = 0x80000000 >>> 1;

    static final AtomicInteger ITERATION_SEED = new AtomicInteger();
}
