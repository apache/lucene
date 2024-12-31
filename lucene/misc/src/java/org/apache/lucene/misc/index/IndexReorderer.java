package org.apache.lucene.misc.index;

import java.io.IOException;
import java.util.concurrent.Executor;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.store.Directory;

public interface IndexReorderer {
  Sorter.DocMap computeDocMap(CodecReader reader, Directory tempDir, Executor executor)
      throws IOException;
}
