package org.apache.lucene.util.packed;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PackedTest {

    @Test
    public void testPacked() throws Exception {
        int count = 1024;
        int bits = 57;
        long val = PackedInts.maxValue(bits) - 1;
        Packed64VHLongAndByte vh = new Packed64VHLongAndByte(count, bits);
        for (int i = 0; i < count; i++) {
            vh.set(i, val);
            // test right away
            assertEquals("Incorrect value set at " + i, val, vh.get(i));
        }
        // test the list again
        for (int i = 0; i < count; i++) {
            assertEquals("Incorrect value set at " + i, val, vh.get(i));
        }
    }

    @Test
    public void testFailure() throws Exception {
        int count = 1111;
        int bits = 61;
        long val = 177_687_460_430_102_016L;
        Packed64VHLongAndByte vh = new Packed64VHLongAndByte(count, bits);

        vh.set(0, 1);
        vh.set(1, 1);
        vh.set(2, 1);

        assertEquals(1, vh.get(0));
        assertEquals(1, vh.get(1));
        vh.set(1, val);
        assertEquals(1, vh.get(2));
    }

    @Test
    public void testFill() throws Exception {
        int count = 1111;
        int bits = 61;
        long val = 177_687_460_430_102_016L;
        int from = 456, to=1039;
        Packed64VHLongAndByte vh = new Packed64VHLongAndByte(count, bits);
        vh.fill(0, vh.size(), 1);
        vh.fill(from, to, val);
        for (int i = 0; i < vh.size(); i++) {
            var v = vh.get(i);
            var expected = 1L;
            if (i >= from && i < to) {
                expected = val;
            }
            assertEquals("Incorrect value set at " + i, expected, vh.get(i));
        }
    }

    @Test
    public void testOverflow() throws Exception {
        int count = 128;
        int bits = 63;
        long val = PackedInts.maxValue(bits);
        Packed64VHLongAndByte vh = new Packed64VHLongAndByte(count, bits);
        vh.fill(0, vh.size(), val);
        for (int i = 0; i < vh.size(); i++) {
            assertEquals("Incorrect value set at " + i, val, vh.get(i));
        }
    }
}
