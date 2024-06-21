package org.apache.lucene.util.bkd.docIds;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;

public class Bit21WithArrEncoder implements DocIdEncoder {

    private final int[] BASE_FOR_21_BITS = new int[]{
            0, 3, 6, 9, 12, 15, 18, 21, 24, 27, 30, 33, 36, 39, 42, 45, 48, 51, 54, 57, 60, 63, 66,
            69, 72, 75, 78, 81, 84, 87, 90, 93, 96, 99, 102, 105, 108, 111, 114, 117, 120, 123, 126, 129, 132, 135, 138, 141, 144, 147, 150,
            153, 156, 159, 162, 165, 168, 171, 174, 177, 180, 183, 186, 189, 192, 195, 198, 201, 204, 207, 210, 213, 216, 219, 222, 225, 228,
            231, 234, 237, 240, 243, 246, 249, 252, 255, 258, 261, 264, 267, 270, 273, 276, 279, 282, 285, 288, 291, 294, 297, 300, 303, 306,
            309, 312, 315, 318, 321, 324, 327, 330, 333, 336, 339, 342, 345, 348, 351, 354, 357, 360, 363, 366, 369, 372, 375, 378, 381, 384,
            387, 390, 393, 396, 399, 402, 405, 408, 411, 414, 417, 420, 423, 426, 429, 432, 435, 438, 441, 444, 447, 450, 453, 456, 459, 462,
            465, 468, 471, 474, 477, 480, 483, 486, 489, 492, 495, 498, 501, 504, 507, 510
    };

    @Override
    public void encode(IndexOutput out, int start, int count, int[] docIds) throws IOException {
        encodeInternal(out, start, count, docIds, BASE_FOR_21_BITS);
    }

    private void encodeInternal(IndexOutput out, int start, int count, int[] docIds, int[] bases) throws IOException {
        int longsRequired = count / 3;
        int base;
        for (int i = 0; i < longsRequired; i++) {
            base = bases[i];
            long packedLong = ((docIds[base] & 0x001FFFFFL) << 42) |
                    ((docIds[base + 1] & 0x001FFFFFL) << 21) |
                    (docIds[base + 2] & 0x001FFFFFL);
            out.writeLong(packedLong);
        }
        base = bases[longsRequired];
        for (int i = 0; i < (count % 3); i++) {
            out.writeInt(docIds[base + i]);
        }
    }

    @Override
    public void decode(IndexInput in, int start, int count, int[] docIDs) throws IOException {
        decodeInternal(in, start, count, docIDs, BASE_FOR_21_BITS);
    }

    private void decodeInternal(IndexInput in, int start, int count, int[] docIDs, int[] bases) throws IOException {
        int longsRequired = count / 3;
        int base;
        for (int i = 0; i < longsRequired; i++) {
            base = bases[i];
            long packedLong = in.readLong();
            docIDs[base] = (int) (packedLong >>> 42);
            docIDs[base + 1] = (int) ((packedLong & 0x000003FFFFE00000L) >>> 21);
            docIDs[base + 2] = (int) (packedLong & 0x001FFFFFL);
        }
        base = bases[longsRequired];
        for (int i = 0; i < (count % 3); i++) {
            docIDs[base + i] = in.readInt();
        }
    }
}
