package org.apache.lucene.util.packed;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import org.apache.lucene.util.packed.PackedInts.Mutable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@Fork(1)
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class Packed64Benchmark {

    @Param({"10240"})
    private int size;
    @Param({"1", "4", "5", "8", "11", "16", "23", "25", "31", "32", "47", "59", "61", "64"})
    private int bpv;
    long[] values;
    Packed64 packed64;
    Packed64VHLongLong packed64vhll;
    Packed64VHLongAndByte packed64vhlb;

    @Setup
    public void setup() {
        var max = PackedInts.maxValue(bpv);
        values = new long[size];
        var r = new Random(size);
        for (int i = 0; i < values.length; i++) {
            values[i] = RandomNumbers.randomLongBetween(r, 0, max);
        }
        // field initialization
        packed64 = new Packed64(size, bpv);
        packed64vhll = new Packed64VHLongLong(size, bpv);
        packed64vhlb = new Packed64VHLongAndByte(size, bpv);
    }

    private final Mutable consecutiveReadWrite(Mutable mutable) {
        for (int i = 0; i < values.length; i++) {
            mutable.set(i, values[i]);
        }
        for (int i = 0; i < values.length; i++) {
            mutable.get(i);
        }
        return mutable;
    }

    private final Mutable sparseReadWrite(Mutable mutable) {
        int middle = values.length / 2;
        for (int i = 0; i < middle; i++) {
            mutable.set(i, values[i]);
            mutable.set(values.length - i - 1, values[values.length - i - 1]);
        }
        for (int i = 0; i < middle; i++) {
            mutable.get(i);
            mutable.get(values.length - i - 1);
        }
        return mutable;
    }

    @Benchmark
    public Mutable packed64_Consecutive() {
        return consecutiveReadWrite(packed64);
    }

    @Benchmark
    public Mutable packed64_Sparse() {
        return sparseReadWrite(packed64);
    }

    @Benchmark
    public Mutable packed64VarHandleLongLong_Consecutive() {
        return consecutiveReadWrite(packed64vhll);
    }

    @Benchmark
    public Mutable packed64VarHandleLongLong_Sparse() {
        return sparseReadWrite(packed64vhll);
    }

    @Benchmark
    public Mutable packed64VarHandleLongByte_Consecutive() {
        return consecutiveReadWrite(packed64vhlb);
    }

    @Benchmark
    public Mutable packed64VarHandleLongByte_Sparse() {
        return sparseReadWrite(packed64vhlb);
    }
}