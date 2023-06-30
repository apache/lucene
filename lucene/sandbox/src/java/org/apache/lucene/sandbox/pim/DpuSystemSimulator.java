package org.apache.lucene.sandbox.pim;

import org.apache.lucene.index.Term;
import org.apache.lucene.store.ByteArrayCircularDataInput;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.List;

import static org.apache.lucene.sandbox.pim.PimSystemManager2.QueryBuffer;

class DpuSystemSimulator implements PimQueriesExecutor {

    private PimIndexSearcher pimSearcher;

    @Override
    public void setPimIndex(PimIndexInfo pimIndexInfo) {
        // create a new PimIndexSearcher for this index
        // TODO copy the PIM index files here to mimic transfer
        // to DPU and be safe searching it while the index is overwritten
        pimSearcher = new PimIndexSearcher(pimIndexInfo);
    }

    @Override
    public void executeQueries(ByteBufferBoundedQueue.ByteBuffers queryBatch, PimSystemManager.ResultReceiver resultReceiver)
            throws IOException {
        ByteArrayCircularDataInput input = new ByteArrayCircularDataInput(queryBatch.getBuffer(),
                queryBatch.getStartIndex(), queryBatch.getSize());

        for (int q = 0; q < queryBatch.getNbElems(); ++q) {

            // rebuild a query object for PimIndexSearcher
            int segment = input.readVInt();
            byte type = input.readByte();
            assert type == DpuConstants.PIM_PHRASE_QUERY_TYPE;
            int fieldSz = input.readVInt();
            byte[] fieldBytes = new byte[fieldSz];
            input.readBytes(fieldBytes, 0, fieldSz);
            BytesRef field = new BytesRef(fieldBytes);
            PimPhraseQuery.Builder builder = new PimPhraseQuery.Builder();
            int nbTerms = input.readVInt();
            for (int i = 0; i < nbTerms; ++i) {
                int termByteSize = input.readVInt();
                byte[] termBytes = new byte[termByteSize];
                input.readBytes(termBytes, 0, termByteSize);
                builder.add(new Term(field.utf8ToString(), new BytesRef(termBytes)));
            }

            // use PimIndexSearcher to handle the query (software model)
            List<PimMatch> matches = pimSearcher.searchPhrase(segment, builder.build());

            // write the results in the results queue
            ByteCountDataOutput countOut = new ByteCountDataOutput();
            countOut.writeVInt(matches.size());
            for (PimMatch m : matches) {
                countOut.writeVInt(m.docId);
                countOut.writeVInt((int) m.score);
            }
            byte[] matchesByteArr = new byte[Math.toIntExact(countOut.getByteCount())];
            ByteArrayDataOutput byteOut = new ByteArrayDataOutput(matchesByteArr);
            byteOut.writeVInt(matches.size());
            for (PimMatch m : matches) {
                byteOut.writeVInt(m.docId);
                byteOut.writeVInt((int) m.score);
            }

            resultReceiver.startResultBatch();
            try {
                resultReceiver.addResults(queryBatch.getUniqueIdOf(q), new ByteArrayDataInput(matchesByteArr));
            } finally {
                resultReceiver.endResultBatch();
            }
        }
    }

    @Override
    public void executeQueries(List<QueryBuffer> queryBuffers) throws IOException {
        for (QueryBuffer queryBuffer : queryBuffers) {
            DataInput input = queryBuffer.getDataInput();

            // rebuild a query object for PimIndexSearcher
            int segment = input.readVInt();
            byte type = input.readByte();
            assert type == DpuConstants.PIM_PHRASE_QUERY_TYPE;
            int fieldSz = input.readVInt();
            byte[] fieldBytes = new byte[fieldSz];
            input.readBytes(fieldBytes, 0, fieldSz);
            BytesRef field = new BytesRef(fieldBytes);
            PimPhraseQuery.Builder builder = new PimPhraseQuery.Builder();
            int nbTerms = input.readVInt();
            for (int i = 0; i < nbTerms; ++i) {
                int termByteSize = input.readVInt();
                byte[] termBytes = new byte[termByteSize];
                input.readBytes(termBytes, 0, termByteSize);
                builder.add(new Term(field.utf8ToString(), new BytesRef(termBytes)));
            }

            // use PimIndexSearcher to handle the query (software model)
            List<PimMatch> matches = pimSearcher.searchPhrase(segment, builder.build());

            // write the results in the results queue
            ByteCountDataOutput countOut = new ByteCountDataOutput();
            countOut.writeVInt(matches.size());
            for (PimMatch m : matches) {
                countOut.writeVInt(m.docId);
                countOut.writeVInt((int) m.score);
            }
            byte[] matchesByteArr = new byte[Math.toIntExact(countOut.getByteCount())];
            ByteArrayDataOutput byteOut = new ByteArrayDataOutput(matchesByteArr);
            byteOut.writeVInt(matches.size());
            for (PimMatch m : matches) {
                byteOut.writeVInt(m.docId);
                byteOut.writeVInt((int) m.score);
            }

            queryBuffer.addResults(new ByteArrayDataInput(matchesByteArr));
        }
    }
}
