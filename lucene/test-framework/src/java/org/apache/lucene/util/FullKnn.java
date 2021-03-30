package org.apache.lucene.util;

import org.apache.lucene.index.VectorValues;
import org.apache.lucene.util.hnsw.NeighborQueue;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * A utility class to calculate the Full KNN / Exact KNN over a set of query vectors and document vectors.
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
   * internal object to track KNN calculation for one query
   */
  private static class KnnJob {
    public int currDocIndex;
    float[] queryVector;
    float[] currDocVector;
    int queryIndex;
    NeighborQueue queue;
    FloatBuffer docVectors;
    VectorValues.SearchStrategy searchStrategy;

    public KnnJob(int queryIndex, float[] queryVector, int topK, VectorValues.SearchStrategy searchStrategy) {
      this.queryIndex = queryIndex;
      this.queryVector = queryVector;
      this.currDocVector = new float[queryVector.length];
      queue = new NeighborQueue(topK, searchStrategy.reversed);
      this.searchStrategy = searchStrategy;
    }

    public void execute() {
      while (this.docVectors.hasRemaining()) {
        this.docVectors.get(this.currDocVector);
        float d = this.searchStrategy.compare(this.queryVector, this.currDocVector);
        this.queue.insertWithOverflow(this.currDocIndex, d);
        this.currDocIndex++;
      }
    }
  }

  /**
   * computes the exact KNN match for each query vector in queryPath for all the document vectors in docPath
   *
   * @param docPath   : path to the file containing the float 32 document vectors in bytes with little-endian byte order
   *                  Throws exception if topK is greater than number of documents in this file
   * @param numDocs   : number of vectors in the document vector file at docPath
   * @param queryPath : path to the file containing the containing 32-bit floating point vectors in little-endian byte order
   * @param numIters  : number of vectors in the query vector file at queryPath
   * @param numThreads : create numThreads to parallelize work
   * @return : returns an int 2D array ( int matches[][]) of size 'numIters x topK'. matches[i] is an array containing
   * the indexes of the topK most similar document vectors to the ith query vector, and is sorted by similarity, with
   * the most similar vector first. Similarity is defined by the searchStrategy used to construct this FullKnn.
   * @throws IOException : if topK is greater than number of documents in docPath file
   */
  public int[][] computeNN(Path docPath, int numDocs, Path queryPath, int numIters, int numThreads) throws IOException {
    assert numThreads > 0;

    int[][] result = new int[numIters][];
    if (!quiet) {
      System.out.println("computing true nearest neighbors of " + numIters + " target vectors using " + numThreads + " threads.");
    }

    final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    try (FileChannel docInput = FileChannel.open(docPath);
         FileChannel queryInput = FileChannel.open(queryPath)) {
      FloatBuffer queries = queryInput.map(FileChannel.MapMode.READ_ONLY, 0, numIters * dim * Float.BYTES)
          .order(ByteOrder.LITTLE_ENDIAN)
          .asFloatBuffer();
      float[] query = new float[dim];
      List<KnnJob> jobList = new ArrayList<>(numThreads);
      for (int i = 0; i < numIters; ) {

        for (int j = 0; j < numThreads && i < numIters; i++, j++) {
          queries.get(query);
          jobList.add(new KnnJob(i, Arrays.copyOf(query, query.length), topK, searchStrategy));
        }

        long maxBufferSize = (Integer.MAX_VALUE / (dim * Float.BYTES)) * (dim * Float.BYTES);
        int docsLeft = numDocs;
        int currDocIndex = 0;
        int offset = 0;
        while (docsLeft > 0) {
          long totalBytes = (long) docsLeft * dim * Float.BYTES;
          int blockSize = (int) Math.min(totalBytes, maxBufferSize);

          FloatBuffer docVectors = docInput.map(FileChannel.MapMode.READ_ONLY, offset, blockSize)
              .order(ByteOrder.LITTLE_ENDIAN)
              .asFloatBuffer();
          offset += blockSize;

          final List<CompletableFuture<Void>> completableFutures = jobList.stream()
              .peek(job -> job.docVectors = docVectors.duplicate())
              .peek(job -> job.currDocIndex = currDocIndex).map(job ->
                  CompletableFuture.runAsync(() -> job.execute(), executorService)).collect(Collectors.toList());

          CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture<?>[completableFutures.size()])).join();
          docsLeft -= (blockSize / (dim * Float.BYTES));
        }

        jobList.forEach(job -> {
          result[job.queryIndex] = new int[topK];
          for (int k = topK - 1; k >= 0; k--) {
            result[job.queryIndex][k] = job.queue.pop();
            //System.out.print(" " + n);
          }
          if (!quiet && (job.queryIndex + 1) % 10 == 0) {
            System.out.print(" " + (job.queryIndex + 1));
            System.out.flush();
          }
        });

        jobList.clear();
      }
    }

    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
        executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      executorService.shutdownNow();
      throw new RuntimeException("Exception occured while waiting for executor service to finish.", e);
    }

    return result;
  }
}
