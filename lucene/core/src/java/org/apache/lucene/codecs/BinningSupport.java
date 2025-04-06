package org.apache.lucene.codecs;

import java.io.IOException;

public interface BinningSupport {
  void finalizeBinning() throws IOException;
}
