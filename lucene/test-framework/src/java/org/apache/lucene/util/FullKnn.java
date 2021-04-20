/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import org.apache.lucene.index.VectorValues;

/**
 * A utility class to calculate the Full KNN / Exact KNN over a set of query vectors and document
 * vectors.
 */
public class FullKnn {

  private final int dim;
  private final int topK;
  private final VectorValues.SearchStrategy searchStrategy;
  private final boolean quiet;

  public FullKnn(int dim, int topK, VectorValues.SearchStrategy searchStrategy, boolean quiet) {
    this.dim = dim;
    this.topK = topK;
    this.searchStrategy = searchStrategy;
    this.quiet = quiet;
  }

  /**
   * computes the exact KNN match for each query vector in queryPath for all the document vectors in
   * docPath
   *
   * @param docPath : path to the file containing the float 32 document vectors in bytes with
   *     little-endian byte order
   * @param queryPath : path to the file containing the containing 32-bit floating point vectors in
   *     little-endian byte order
   * @return : returns an int 2D array ( int matches[][]) of size 'numIters x topK'. matches[i] is
   *     an array containing the indexes of the topK most similar document vectors to the ith query
   *     vector, and is sorted by similarity, with the most similar vector first. Similarity is
   *     defined by the searchStrategy used to construct this FullKnn.
   * @throws IllegalArgumentException : if topK is greater than number of documents in docPath file
   *     IOException : In case of IO exception while reading files.
   */
  public int[][] computeNN(Path docPath, Path queryPath) throws IOException {
    final int numDocs = (int) (Files.size(docPath) / (dim * Float.BYTES));
    final int numQueries = (int) (Files.size(docPath) / (dim * Float.BYTES));

    if (!quiet) {
      System.out.println("computing true nearest neighbors of " + numQueries + " target vectors");
    }

    try (FileChannel docInput = FileChannel.open(docPath);
        FileChannel queryInput = FileChannel.open(queryPath)) {
      return doFullKnn(
          numDocs,
          numQueries,
          new FileChannelBufferProvider(docInput),
          new FileChannelBufferProvider(queryInput));
    }
  }

  int[][] doFullKnn(int numDocs, int numQueries, BufferProvider docInput, BufferProvider queryInput)
      throws IOException {
    if (numDocs < topK) {
      throw new IllegalArgumentException(
          String.format(
              Locale.ROOT,
              "topK (%d) cannot be greater than number of docs in docPath (%d)",
              topK,
              numDocs));
    }

    int[][] result = new int[numQueries][];
    if (!quiet) {
      System.out.println("computing true nearest neighbors of " + numQueries + " target vectors");
    }
    FloatBuffer queries =
        queryInput
            .getBuffer(0, numQueries * dim * Float.BYTES)
            .order(ByteOrder.LITTLE_ENDIAN)
            .asFloatBuffer();
    float[] vector = new float[dim];
    float[] query = new float[dim];
    for (int i = 0; i < numQueries; i++) {
      queries.get(query);
      long totalBytes = (long) numDocs * dim * Float.BYTES;
      int
          blockSize =
              (int)
                  Math.min(
                      totalBytes, (Integer.MAX_VALUE / (dim * Float.BYTES)) * (dim * Float.BYTES)),
          offset = 0;
      int j = 0;
      LongHeap queue = null;
      if (searchStrategy.reversed) {
        queue = LongHeap.create(LongHeap.Order.MAX, topK);
      } else {
        queue = LongHeap.create(LongHeap.Order.MIN, topK);
      }

      while (j < numDocs) {
        FloatBuffer vectors =
            docInput.getBuffer(offset, blockSize).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer();
        offset += blockSize;
        for (; j < numDocs && vectors.hasRemaining(); j++) {
          vectors.get(vector);
          float d = searchStrategy.compare(query, vector);
          queue.insertWithOverflow(encodeNodeIdAndScore(j, d));
        }
      }
      result[i] = new int[topK];
      for (int k = topK - 1; k >= 0; k--) {
        result[i][k] = popNodeId(queue);
      }
      if (!quiet && (i + 1) % 10 == 0) {
        System.out.print(" " + (i + 1));
        System.out.flush();
      }
    }
    return result;
  }

  /**
   * pops the queue and returns the last 4 bytes of long where nodeId is stored.
   *
   * @param queue queue from which to pop the node id
   * @return the node id
   */
  private int popNodeId(LongHeap queue) {
    return (int) queue.pop();
  }

  /**
   * encodes the score and nodeId into a single long with score in first 4 bytes, to make it
   * sortable by score.
   *
   * @param node node id
   * @param score float score of the node wrt incoming node/query
   * @return a score sortable long that can be used in heap
   */
  private static long encodeNodeIdAndScore(int node, float score) {
    return (((long) NumericUtils.floatToSortableInt(score)) << 32) | node;
  }

  interface BufferProvider {
    ByteBuffer getBuffer(int offset, int blockSize) throws IOException;
  }

  private static class FileChannelBufferProvider implements BufferProvider {
    private FileChannel fileChannel;

    FileChannelBufferProvider(FileChannel fileChannel) {
      this.fileChannel = fileChannel;
    }

    @Override
    public ByteBuffer getBuffer(int offset, int blockSize) throws IOException {
      return fileChannel
          .map(FileChannel.MapMode.READ_ONLY, offset, blockSize)
          .order(ByteOrder.LITTLE_ENDIAN);
    }
  }
}
