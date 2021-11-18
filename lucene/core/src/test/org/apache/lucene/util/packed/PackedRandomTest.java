package org.apache.lucene.util.packed;

import org.apache.lucene.util.packed.Packed64VHLongAndByte;
import org.apache.lucene.util.packed.PackedInts;

import java.util.SplittableRandom;

public class PackedRandomTest {
    private static long DUMMY = 0;

    public static void main(String[] args) {
        int valueCount = 1 << 10;
        int bitsPerValue = 11;
        PackedInts.Mutable mut = new Packed64VHLongAndByte(valueCount, bitsPerValue);
        //PackedInts.Mutable mut = new Packed64(valueCount, bitsPerValue);
        SplittableRandom r = new SplittableRandom(0L);
        for (int i = 0; i < valueCount; ++i) {
            mut.set(i, r.nextInt(1 << bitsPerValue));
        }
        long min = Long.MAX_VALUE;
        for (int iter = 0; iter < 1000; ++iter) {
            long start = System.nanoTime();
            long sum = 0;
            int index = 42;
            for (int i = 0; i < 1_000_000; ++i) {
                sum += mut.get(index);
                index = (index + 47) & (valueCount - 1);
            }
            DUMMY = sum;
            min = Math.min(System.nanoTime() - start, min);
        }
        System.out.println(min);
    }
}
