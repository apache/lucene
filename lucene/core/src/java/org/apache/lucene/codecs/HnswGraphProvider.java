package org.apache.lucene.codecs;

import java.io.IOException;
import org.apache.lucene.util.hnsw.HnswGraph;

/**
 * An interface that provides an HNSW graph. This interface is useful when gathering multiple HNSW
 * graphs to bootstrap segment merging. The graph may be off the JVM heap.
 *
 * @lucene.experimental
 */
public interface HnswGraphProvider {
  /**
   * Return the stored HnswGraph for the given field.
   *
   * @param field the field containing the graph
   * @return the HnswGraph for the given field if found
   * @throws IOException when reading potentially off-heap graph fails
   */
  HnswGraph getGraph(String field) throws IOException;
}
