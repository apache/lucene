package org.apache.lucene.sandbox.pim;

import java.io.IOException;
import org.apache.lucene.search.LeafSimScorer;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

/** Interface to be implemented by all PIM queries */
public interface PimQuery {

  /**
   * Write this query to PIM This defines the format of the query to be sent to the PIM system
   *
   * @param output the output to be written
   * @throws IOException
   */
  public void writeToPim(DataOutput output) throws IOException;

  /**
   * Reads the PIM query result, performs the scoring and return a PimMatch object This functions
   * specifies how results returned by the PIM system should be interpreted and scored.
   *
   * @param input
   * @param scorer
   * @return
   * @throws IOException
   */
  PimMatch readResult(DataInput input, LeafSimScorer scorer) throws IOException;
}
