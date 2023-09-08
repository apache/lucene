package org.apache.lucene.sandbox.pim;

import static org.apache.lucene.sandbox.pim.PimSystemManager.QueryBuffer;

import java.io.IOException;
import java.util.List;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.BytesRef;

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

      byte[] matchesByteArr = new byte[Math.toIntExact(matches.size() * 2 * Integer.BYTES)];
      ByteArrayDataOutput byteOut = new ByteArrayDataOutput(matchesByteArr);
      for (PimMatch m : matches) {
        byteOut.writeInt(m.docId);
        byteOut.writeInt((int) m.score);
      }

      queryBuffer.addResults(new DpuResultsArrayInput(new ByteArrayDataInput(matchesByteArr)));
    }
  }
}
