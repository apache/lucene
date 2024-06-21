package org.apache.lucene.util.bkd.docIds;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;

    public class Bit21With3StepsSingleLoopAddEncoder implements DocIdEncoder {

    @Override
    public void encode(IndexOutput out, int start, int count, int[] docIds) throws IOException {
        int i = 0;
        for (; i < count; i += 9) {
            int doc1 = docIds[i];
            int doc2 = docIds[i + 1];
            int doc3 = docIds[i + 2];
            int doc4 = docIds[i + 3];
            int doc5 = docIds[i + 4];
            int doc6 = docIds[i + 5];
            int doc7 = docIds[i + 6];
            int doc8 = docIds[i + 7];
            int doc9 = docIds[i + 8];
            long l1 = ((doc1 & 0x001FFFFFL) << 42) | ((doc2 & 0x001FFFFFL) << 21) | (doc3 & 0x001FFFFFL);
            long l2 = ((doc4 & 0x001FFFFFL) << 42) | ((doc5 & 0x001FFFFFL) << 21) | (doc6 & 0x001FFFFFL);
            long l3 = ((doc7 & 0x001FFFFFL) << 42) | ((doc8 & 0x001FFFFFL) << 21) | (doc9 & 0x001FFFFFL);
            out.writeLong(l1);
            out.writeLong(l2);
            out.writeLong(l3);
        }
    }

    @Override
    public void decode(IndexInput in, int start, int count, int[] docIDs) throws IOException {
        int i = 0;
        for (; i < count; i += 9) {
            long l1 = in.readLong();
            long l2 = in.readLong();
            long l3 = in.readLong();
            docIDs[i] = (int) (l1 >>> 42);
            docIDs[i + 1] = (int) ((l1 & 0x000003FFFFE00000L) >>> 21);
            docIDs[i + 2] = (int) (l1 & 0x001FFFFFL);
            docIDs[i + 3] = (int) (l2 >>> 42);
            docIDs[i + 4] = (int) ((l2 & 0x000003FFFFE00000L) >>> 21);
            docIDs[i + 5] = (int) (l2 & 0x001FFFFFL);
            docIDs[i + 6] = (int) (l3 >>> 42);
            docIDs[i + 7] = (int) ((l3 & 0x000003FFFFE00000L) >>> 21);
            docIDs[i + 8] = (int) (l3 & 0x001FFFFFL);
        }
    }
}
