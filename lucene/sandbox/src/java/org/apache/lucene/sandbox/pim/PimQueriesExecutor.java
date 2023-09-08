package org.apache.lucene.sandbox.pim;

import com.upmem.dpu.DpuException;
import java.io.IOException;
import java.util.List;

interface PimQueriesExecutor {

  void setPimIndex(PimIndexInfo pimIndexInfo) throws DpuException, IOException;

  void executeQueries(List<PimSystemManager.QueryBuffer> queryBuffers)
      throws IOException, DpuException;

  default void dumpDpuStream() {}
}
