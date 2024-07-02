package org.apache.lucene.util.bkd.docIds;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;

public class Bit21With2StepsAddAndShortByteEncoder implements DocIdEncoder {
    @Override
    public void encode(IndexOutput out, int start, int count, int[] docIds) throws IOException {
        int i = 0;
        for (; i < count - 2; i += 3) {
            long packedLong = ((docIds[i] & 0x001FFFFFL) << 42) |
                    ((docIds[i + 1] & 0x001FFFFFL) << 21) |
                    (docIds[i + 2] & 0x001FFFFFL);
            out.writeLong(packedLong);
        }
        for (; i < count; i++) {
            out.writeShort((short) (docIds[i] >>> 8));
            out.writeByte((byte) docIds[i]);
        }
    }

    @Override
    public void decode(IndexInput in, int start, int count, int[] docIDs) throws IOException {
        int i = 0;
        for (; i < count - 2; i += 3) {
            long packedLong = in.readLong();
            docIDs[i] = (int) (packedLong >>> 42);
            docIDs[i + 1] = (int) ((packedLong & 0x000003FFFFE00000L) >>> 21);
            docIDs[i + 2] = (int) (packedLong & 0x001FFFFFL);
        }
        for (; i < count; i++) {
            docIDs[i] = (Short.toUnsignedInt(in.readShort()) << 8) | Byte.toUnsignedInt(in.readByte());
        }
    }
}
