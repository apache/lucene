package org.apache.lucene.util.bkd.docIds;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;

public class Bit24Encoder implements DocIdEncoder {
    @Override
    public void encode(IndexOutput out, int start, int count, int[] docIds) throws IOException {
        int i;
        for (i = 0; i < count - 7; i += 8) {
            int doc1 = docIds[i];
            int doc2 = docIds[i + 1];
            int doc3 = docIds[i + 2];
            int doc4 = docIds[i + 3];
            int doc5 = docIds[i + 4];
            int doc6 = docIds[i + 5];
            int doc7 = docIds[i + 6];
            int doc8 = docIds[i + 7];
            long l1 = (doc1 & 0xffffffL) << 40 | (doc2 & 0xffffffL) << 16 | ((doc3 >>> 8) & 0xffffL);
            long l2 =
                    (doc3 & 0xffL) << 56
                            | (doc4 & 0xffffffL) << 32
                            | (doc5 & 0xffffffL) << 8
                            | ((doc6 >> 16) & 0xffL);
            long l3 = (doc6 & 0xffffL) << 48 | (doc7 & 0xffffffL) << 24 | (doc8 & 0xffffffL);
            out.writeLong(l1);
            out.writeLong(l2);
            out.writeLong(l3);
        }
        for (; i < count; ++i) {
            out.writeShort((short) (docIds[i] >>> 8));
            out.writeByte((byte) docIds[i]);
        }
    }

    @Override
    public void decode(IndexInput in, int start, int count, int[] docIDs) throws IOException {
        int i;
        for (i = 0; i < count - 7; i += 8) {
            long l1 = in.readLong();
            long l2 = in.readLong();
            long l3 = in.readLong();
            docIDs[i] = (int) (l1 >>> 40);
            docIDs[i + 1] = (int) (l1 >>> 16) & 0xffffff;
            docIDs[i + 2] = (int) (((l1 & 0xffff) << 8) | (l2 >>> 56));
            docIDs[i + 3] = (int) (l2 >>> 32) & 0xffffff;
            docIDs[i + 4] = (int) (l2 >>> 8) & 0xffffff;
            docIDs[i + 5] = (int) (((l2 & 0xff) << 16) | (l3 >>> 48));
            docIDs[i + 6] = (int) (l3 >>> 24) & 0xffffff;
            docIDs[i + 7] = (int) l3 & 0xffffff;
        }
        for (; i < count; ++i) {
            docIDs[i] = (Short.toUnsignedInt(in.readShort()) << 8) | Byte.toUnsignedInt(in.readByte());
        }
    }
}
