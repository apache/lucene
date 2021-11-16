package org.apache.lucene.util.packed;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

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
    long[] baseLine;
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
        baseLine = new long[size];
        packed64 = new Packed64(size, bpv);
        packed64vhll = new Packed64VHLongLong(size, bpv);
        packed64vhlb = new Packed64VHLongAndByte(size, bpv);
    }

    @Benchmark
    public void packed64(Blackhole bh) {
        for (int i = 0; i < values.length; i++) {
            packed64.set(i, values[i]);
        }
        bh.consume(packed64.get(0));
    }

    @Benchmark
    public void packed64VarHandleDoubleLong(Blackhole bh) {
        for (int i = 0; i < values.length; i++) {
            packed64vhll.set(i, values[i]);
        }
        bh.consume(packed64vhll.get(0));
    }

    @Benchmark
    public void packed64VarHandleLongAndByte(Blackhole bh) {
        for (int i = 0; i < values.length; i++) {
            packed64vhlb.set(i, values[i]);
        }
        bh.consume(packed64vhlb.get(0));
    }
}
