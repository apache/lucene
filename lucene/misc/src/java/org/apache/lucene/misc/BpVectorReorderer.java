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
package org.apache.lucene.misc;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.RecursiveAction;

import org.apache.lucene.codecs.lucene912.Lucene912Codec;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.lucene95.OffHeapFloatVectorValues;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.SortingCodecReader;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.CloseableThreadLocal;
import org.apache.lucene.util.IntroSelector;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.apache.lucene.util.hnsw.HnswGraphBuilder.DEFAULT_BEAM_WIDTH;

/**
 * Implementation of "recursive graph bisection", also called "bipartite graph partitioning" and
 * often abbreviated BP, an approach to doc ID assignment that aims at reducing the sum of the log
 * gap between consecutive neighbor node ids. See BPIndexReorderer in misc module.
 *
 * <p>TODO: use when flushing/merging support maximum inner product support bytes
 */
public class BpVectorReorderer {

  /** Minimum problem size that will result in tasks being split. */
  private static final int FORK_THRESHOLD = 8192;

  /**
   * Minimum size of partitions. The algorithm will stop recursing when reaching partitions below
   * this number of nodes: 32.
   */
  public static final int DEFAULT_MIN_PARTITION_SIZE = 32;

  /**
   * Default maximum number of iterations per recursion level: 20. Higher numbers of iterations
   * typically don't help significantly.
   */
  public static final int DEFAULT_MAX_ITERS = 20;

  private int minPartitionSize;
  private int maxIters;
  private ForkJoinPool forkJoinPool;
  private double ramBudgetMB;
  private VectorSimilarityFunction vectorScore;

  /** Constructor. */
  public BpVectorReorderer() {
    setMinPartitionSize(DEFAULT_MIN_PARTITION_SIZE);
    setMaxIters(DEFAULT_MAX_ITERS);
    setForkJoinPool(null);
    // 10% of the available heap size by default
    setRAMBudgetMB(Runtime.getRuntime().totalMemory() / 1024d / 1024d / 10d);
    setVectorSimilarity(VectorSimilarityFunction.EUCLIDEAN);
  }

  /** Set the minimum partition size, when the algorithm stops recursing, 32 by default. */
  public void setMinPartitionSize(int minPartitionSize) {
    if (minPartitionSize < 1) {
      throw new IllegalArgumentException(
          "minPartitionSize must be at least 1, got " + minPartitionSize);
    }
    this.minPartitionSize = minPartitionSize;
  }

  /**
   * Set the maximum number of iterations on each recursion level, 20 by default. Experiments
   * suggests that values above 20 do not help much. However, values below 20 can be used to trade
   * effectiveness for faster reordering.
   */
  public void setMaxIters(int maxIters) {
    if (maxIters < 1) {
      throw new IllegalArgumentException("maxIters must be at least 1, got " + maxIters);
    }
    this.maxIters = maxIters;
  }

  /**
   * The function used to compare the vectors. This function must have higher values for vectors
   * that are more similar to each other, and must preserve inequalities over sums of vectors. More
   * formally it must have the property:
   *
   * <p>This property enables us to use the centroid of a collection of vectors to represent the
   * collection. For Euclidean and inner product score functions, the centroid is the point that
   * minimizes the sum of distances from all the points (thus maximizing the score).
   *
   * <p>sum((c0 - v)^2) = n * c0^2 - 2 * c0 * sum(v) + sum(v^2) taking derivative w.r.t. c0 and
   * setting to 0 we get sum(v) = n * c0; i.e. c0 (the centroid) is the place that minimizes the sum
   * of (l2) distances from the vectors (thus maximizing the euclidean score function).
   *
   * <p>not sure how to prove this for inner product scores, but it seems like a reasonable thing??
   * There are weird degenerate cases though like (-1, 0), (1, 0) -> (0, 0).
   *
   * @param vectorScore the function to compare vectors
   */
  public void setVectorSimilarity(VectorSimilarityFunction vectorScore) {
    this.vectorScore = vectorScore;
  }

  /**
   * Set the {@link ForkJoinPool} to run graph partitioning concurrently.
   *
   * <p>NOTE: A value of {@code null} can be used to run in the current thread, which is the
   * default.
   */
  public void setForkJoinPool(ForkJoinPool forkJoinPool) {
    this.forkJoinPool = forkJoinPool;
  }

  /**
   * Set the amount of RAM that graph partitioning is allowed to use. More RAM allows running
   * faster. If not enough RAM is provided, an exception (TODO: NotEnoughRAMException) will be
   * thrown. This is 10% of the total heap size by default.
   */
  public void setRAMBudgetMB(double ramBudgetMB) {
    this.ramBudgetMB = ramBudgetMB;
  }

  private static class PerThreadState {

    final FloatValues vectors;
    final float[] leftCentroid;
    final float[] rightCentroid;
    final float[] scratch;

    PerThreadState(FloatValues vectors) {
      try {
        this.vectors = vectors.copy();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      leftCentroid = new float[vectors.dimension()];
      rightCentroid = new float[leftCentroid.length];
      scratch = new float[leftCentroid.length];
    }
  }

  private static class DocMap extends Sorter.DocMap {

    private final int[] newToOld;
    private final int[] oldToNew;

    public DocMap(int[] newToOld) {
      this.newToOld = newToOld;
      oldToNew = new int[newToOld.length];
      for (int i = 0; i < newToOld.length; ++i) {
        oldToNew[newToOld[i]] = i;
      }
    }

    @Override
    public int size() {
      return newToOld.length;
    }

    @Override
    public int oldToNew(int docID) {
      return oldToNew[docID];
    }

    @Override
    public int newToOld(int docID) {
      return newToOld[docID];
    }
  }

  private abstract class BaseRecursiveAction extends RecursiveAction {

    protected final int depth;

    BaseRecursiveAction(int depth) {
      this.depth = depth;
    }

    protected final boolean shouldFork(int problemSize, int totalProblemSize) {
      if (forkJoinPool == null) {
        return false;
      }
      if (getSurplusQueuedTaskCount() > 3) {
        // Fork tasks if this worker doesn't have more queued work than other workers
        // See javadocs of #getSurplusQueuedTaskCount for more details
        return false;
      }
      if (problemSize == totalProblemSize) {
        // Sometimes fork regardless of the problem size to make sure that unit tests also exercise
        // forking
        return true;
      }
      return problemSize > FORK_THRESHOLD;
    }
  }

  private class ReorderTask extends BaseRecursiveAction {

    // the ids assigned to this task, a sub-range of all the ids
    private final IntsRef ids;
    // the biases for the ids - a number < 0 when the doc goes left and > 0 for right
    private final float[] biases;
    private final CloseableThreadLocal<PerThreadState> threadLocal;

    ReorderTask(
        IntsRef ids, float[] biases, CloseableThreadLocal<PerThreadState> threadLocal, int depth) {
      super(depth);
      this.ids = ids;
      this.biases = biases;
      this.threadLocal = threadLocal;
    }

    @Override
    protected void compute() {
      if (depth > 0) {
        Arrays.sort(ids.ints, ids.offset, ids.offset + ids.length);
      } else {
        assert sorted(ids);
      }

      int halfLength = ids.length >>> 1;
      if (halfLength < minPartitionSize) {
        return;
      }

      // split the ids in half
      IntsRef left = new IntsRef(ids.ints, ids.offset, halfLength);
      IntsRef right = new IntsRef(ids.ints, ids.offset + halfLength, ids.length - halfLength);

      PerThreadState state = threadLocal.get();
      FloatValues vectors = state.vectors;
      float[] leftCentroid = state.leftCentroid;
      float[] rightCentroid = state.rightCentroid;
      float[] scratch = state.scratch;

      try {
        computeCentroid(left, vectors, leftCentroid);
        computeCentroid(right, vectors, rightCentroid);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      for (int iter = 0; iter < maxIters; ++iter) {
        boolean moved;
        try {
          moved =
              shuffle(
                  vectors, ids, right.offset, leftCentroid, rightCentroid, scratch, biases, iter);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        if (moved == false) {
          break;
        }
        try {
          // TODO: should we use incremental updates?
          computeCentroid(left, vectors, leftCentroid);
          computeCentroid(right, vectors, rightCentroid);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }

      // It is fine for all tasks to share the same docs / biases array since they all work on
      // different slices of the array at a given point in time.
      ReorderTask leftTask = new ReorderTask(left, biases, threadLocal, depth + 1);
      ReorderTask rightTask = new ReorderTask(right, biases, threadLocal, depth + 1);

      if (shouldFork(ids.length, ids.ints.length)) {
        invokeAll(leftTask, rightTask);
      } else {
        leftTask.compute();
        rightTask.compute();
      }
    }

    void computeCentroid(IntsRef ids, FloatValues vectors, float[] centroid) throws IOException {
      Arrays.fill(centroid, 0);
      for (int i = ids.offset; i < ids.offset + ids.length; i++) {
        VectorUtil.add(centroid, vectors.get(ids.ints[i]));
      }
      vectorScalarMul(1 / (float) ids.length, centroid);
    }

    /** Shuffle IDs across both partitions so that each partition is closer to its centroid. */
    private boolean shuffle(
        FloatValues vectors,
        IntsRef ids,
        int midPoint,
        float[] leftCentroid,
        float[] rightCentroid,
        float[] scratch,
        float[] biases,
        int iter)
        throws IOException {

      // Computing biases is typically a bottleneck, because each iteration needs to iterate over
      // all postings to recompute biases, and the total number of postings is usually one order of
      // magnitude or more larger than the number of docs. So we try to parallelize it.
      new ComputeBiasTask(
              ids.ints,
              biases,
              ids.offset,
              ids.offset + ids.length,
              leftCentroid,
              rightCentroid,
              threadLocal,
              depth)
          .compute();

      float scale = VectorUtil.dotProduct(leftCentroid, leftCentroid) + VectorUtil.dotProduct(rightCentroid, rightCentroid);
      float maxLeftBias = Float.NEGATIVE_INFINITY;
      for (int i = ids.offset; i < midPoint; ++i) {
        maxLeftBias = Math.max(maxLeftBias, biases[i]);
      }
      float minRightBias = Float.POSITIVE_INFINITY;
      for (int i = midPoint, end = ids.offset + ids.length; i < end; ++i) {
        minRightBias = Math.min(minRightBias, biases[i]);
      }
      float gain = maxLeftBias - minRightBias;
      // This uses the simulated annealing proposed by Mackenzie et al in "Tradeoff Options for
      // Bipartite Graph Partitioning" by comparing the gain of swapping the doc from the left side
      // that is most attracted to the right and the doc from the right side that is most attracted
      // to the left against `iter` rather than zero. We use the lengths of the centroids to determine
      // an appropriate scale, so that roughly speaking we stop iterating when the gain becomes less than
      // 0.2% of the size of the vectors.
      //System.out.printf("at depth=%d, midPoint=%d, after %d iters, gain=%f\n", depth, midPoint, iter, gain);
      if (500 * gain <= scale) {
        return false;
      }

      class Selector extends IntroSelector {
        int count = 0;
        int pivotDoc;
        float pivotBias;

        @Override
        public void setPivot(int i) {
          pivotDoc = ids.ints[i];
          pivotBias = biases[i];
        }

        @Override
        public int comparePivot(int j) {
          int cmp = Float.compare(pivotBias, biases[j]);
          if (cmp == 0) {
            // Tie break on the ID to preserve ID ordering as much as possible
            cmp = pivotDoc - ids.ints[j];
          }
          return cmp;
        }

        @Override
        public void swap(int i, int j) {
          float tmpBias = biases[i];
          biases[i] = biases[j];
          biases[j] = tmpBias;

          if (i < midPoint == j < midPoint) {
            int tmpDoc = ids.ints[i];
            ids.ints[i] = ids.ints[j];
            ids.ints[j] = tmpDoc;
          } else {
            // If we're swapping across the left and right sides, we need to keep centroids
            // up-to-date.
            count ++;
            int from = Math.min(i, j);
            int to = Math.max(i, j);
            try {
              swapIdsAndCentroids(
                  ids.ints, from, to, midPoint, vectors, leftCentroid, rightCentroid, scratch);
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          }
        }
      }

      Selector selector = new Selector();
      selector.select(ids.offset, ids.offset + ids.length, midPoint);
      // System.out.printf("swapped %d / %d\n", selector.count, ids.length);
      // assert centroidValid(leftCentroid, vectors, new IntsRef(ids.ints, ids.offset, midPoint -
      // ids.offset));
      // assert centroidValid(rightCentroid, vectors, new IntsRef(ids.ints, midPoint, ids.length -
      // midPoint));
      return true;
    }

    /*
    private boolean centroidValid(float[] centroid, FloatValues vectors, IntsRef ids) {
      // recompute centroid to check the incremental calculation
      float[] check = new float[centroid.length];
      computeCentroid(ids, vectors, check);
      for (int i = 0; i < check.length; ++i) {
        if (Math.abs(check[i] - centroid[i]) > 1e-6) {
          return false;
        }
      }
      return true;
    }
     */

    private static void swapIdsAndCentroids(
        int[] ids,
        int from,
        int to,
        int midPoint,
        FloatValues vectors,
        float[] leftCentroid,
        float[] rightCentroid,
        float[] scratch)
        throws IOException {
      assert from < to;

      int fromId = ids[from];
      int toId = ids[to];

      // Now update the centroids, this makes things much faster than invalidating it and having to
      // recompute on the next iteration. It might not though?
      /*
      vectorSubtract(vectors.get(to), vectors.get(from), scratch);
      // we must normalize to the proper scale by accounting for the number of points contributing to each centroid
      vectorScalarMul(1 / (float) midPoint, scratch);
      VectorUtil.add(leftCentroid, scratch);
      vectorScalarMul(-midPoint / (float) (ids.length - midPoint), scratch);
      VectorUtil.add(rightCentroid, scratch);
       */

      ids[from] = toId;
      ids[to] = fromId;
    }
  }

  /**
   * Adds the second argument to the first
   *
   * @param u the destination
   * @param v the vector to add to the destination
   */
  static void vectorSubtract(float[] u, float[] v, float[] result) {
    for (int i = 0; i < u.length; i++) {
      result[i] = u[i] - v[i];
    }
  }

  static void vectorScalarMul(float x, float[] v) {
    for (int i = 0; i < v.length; i++) {
      v[i] *= x;
    }
  }

  private class ComputeBiasTask extends BaseRecursiveAction {

    private final int[] ids;
    private final float[] biases;
    private final int start;
    private final int end;
    private final float[] leftCentroid;
    private final float[] rightCentroid;
    private final CloseableThreadLocal<PerThreadState> threadLocal;

    ComputeBiasTask(
        int[] ids,
        float[] biases,
        int start,
        int end,
        float[] leftCentroid,
        float[] rightCentroid,
        CloseableThreadLocal<PerThreadState> threadLocal,
        int depth) {
      super(depth);
      this.ids = ids;
      this.biases = biases;
      this.start = start;
      this.end = end;
      this.leftCentroid = leftCentroid;
      this.rightCentroid = rightCentroid;
      this.threadLocal = threadLocal;
    }

    @Override
    protected void compute() {
      final int problemSize = end - start;
      if (problemSize > 1 && shouldFork(problemSize, ids.length)) {
        final int mid = (start + end) >>> 1;
        invokeAll(
            new ComputeBiasTask(
                ids, biases, start, mid, leftCentroid, rightCentroid, threadLocal, depth),
            new ComputeBiasTask(
                ids, biases, mid, end, leftCentroid, rightCentroid, threadLocal, depth));
      } else {
        FloatValues vectors = threadLocal.get().vectors;
        try {
          for (int i = start; i < end; ++i) {
            biases[i] = computeBias(vectors.get(ids[i]), leftCentroid, rightCentroid);
          }
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    }

    /**
     * Compute a float that is negative when a vector is attracted to the left and positive
     * otherwise.
     */
    private float computeBias(float[] vector, float[] leftCentroid, float[] rightCentroid) {
      return switch (vectorScore) {
        case EUCLIDEAN -> VectorUtil.squareDistance(vector, leftCentroid)
            - VectorUtil.squareDistance(vector, rightCentroid);
        // TODO: is this actually OK for COSINE and MAXIMUM_INNER_PRODUCT? I think it is?
        // perhaps instead we ought to measure <vector, (right - left)> .. oh wait that's the same thing!
        case MAXIMUM_INNER_PRODUCT, COSINE, DOT_PRODUCT -> VectorUtil.dotProduct(vector, rightCentroid)
            - VectorUtil.dotProduct(vector, leftCentroid);
        default -> throw new IllegalStateException("unsupported vector score: " + vectorScore);
      };
    }
  }

  public Sorter.DocMap computeDocMapFloat(IndexInput input, int dimension, int size) {
    return computeDocMap(new IndexFloatValues(input, dimension, size));
  }

  public Sorter.DocMap computeDocMapFloat(OffHeapFloatVectorValues values) {
    return computeDocMap(new VectorValuesFloatValues(values));
  }

  public Sorter.DocMap computeDocMap(List<?> vectors) {
    return computeDocMap(new ListFloatValues(vectors));
  }

  /** Expert: Compute the {@link DocMap} that holds the new ID numbering. */
  private Sorter.DocMap computeDocMap(FloatValues vectors) {
    if (docRAMRequirements(vectors.size()) >= ramBudgetMB * 1024 * 1024) {
      // TODO: NotEnoughRAMException
      throw new RuntimeException(
          "At least "
              + Math.ceil(docRAMRequirements(vectors.size()) / 1024. / 1024.)
              + "MB of RAM are required to hold metadata about documents in RAM, but current RAM budget is "
              + ramBudgetMB
              + "MB");
    }
    return new DocMap(computePermutation(vectors));
  }

  /**
   * Compute a permutation of the ID space that maximizes vector score between consecutive postings.
   */
  private int[] computePermutation(FloatValues vectors) {
    final int size = vectors.size();
    int[] sortedIds = new int[size];
    for (int i = 0; i < size; ++i) {
      sortedIds[i] = i;
    }
    try (CloseableThreadLocal<PerThreadState> threadLocal =
        new CloseableThreadLocal<>() {
          @Override
          protected PerThreadState initialValue() {
            return new PerThreadState(vectors);
          }
        }) {
      IntsRef ids = new IntsRef(sortedIds, 0, sortedIds.length);
      ReorderTask task = new ReorderTask(ids, new float[size], threadLocal, 0);
      if (forkJoinPool != null) {
        forkJoinPool.execute(task);
        task.join();
      } else {
        task.compute();
      }
    }
    return sortedIds;
  }

  interface FloatValues {
    // NOTE: must only use one at a time
    float[] get(int i) throws IOException;

    int size();

    int dimension();

    FloatValues copy() throws IOException;
  }

  static class ListFloatValues implements FloatValues {
    private final List<?> values;

    ListFloatValues(List<?> values) {
      this.values = values;
    }

    @Override
    public float[] get(int i) {
      return (float[]) values.get(i);
    }

    @Override
    public int size() {
      return values.size();
    }

    @Override
    public int dimension() {
      return get(0).length;
    }

    @Override
    public FloatValues copy() {
      // this is thread-safe
      return this;
    }
  }

  static class IndexFloatValues implements FloatValues {
    private final IndexInput input;
    private final int dimension;
    private final int size;
    private final float[] scratch;

    IndexFloatValues(IndexInput input, int dimension, int size) {
      this.input = input;
      this.dimension = dimension;
      this.size = size;
      scratch = new float[dimension];
    }

    @Override
    public float[] get(int i) throws IOException {
      input.seek((long) i * dimension * Float.BYTES);
      input.readFloats(scratch, 0, dimension);
      return scratch;
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public int dimension() {
      return dimension;
    }

    @Override
    public FloatValues copy() {
      return new IndexFloatValues(input.clone(), dimension, size);
    }
  }

  static class VectorValuesFloatValues implements FloatValues {
    private final RandomAccessVectorValues.Floats input;
    private final float[] scratch;

    VectorValuesFloatValues(RandomAccessVectorValues.Floats input) {
      this.input = input;
      scratch = new float[input.dimension()];
    }

    @Override
    public float[] get(int i) throws IOException {
      return input.vectorValue(i);
    }

    @Override
    public int size() {
      return input.size();
    }

    @Override
    public int dimension() {
      return input.dimension();
    }

    @Override
    public FloatValues copy() throws IOException {
      return new VectorValuesFloatValues(input.copy());
    }
  }

  /** Returns true if, and only if, the given {@link IntsRef} is sorted. */
  private static boolean sorted(IntsRef intsRef) {
    for (int i = 1; i < intsRef.length; ++i) {
      if (intsRef.ints[intsRef.offset + i - 1] > intsRef.ints[intsRef.offset + i]) {
        return false;
      }
    }
    return true;
  }

  private static long docRAMRequirements(int maxDoc) {
    // We need one int per vector for the doc map, plus one float to store the bias associated with
    // this vector.
    return 2L * Integer.BYTES * maxDoc;
  }

  /**
   * @param args  two args:
   *             a path containing an index to reorder.
   *              the name of the field the contents of which to use for reordering
   */
  public static void main(String... args) throws IOException {
    if (args.length < 2 || args.length > 6) {
      usage();
    }
    BpVectorReorderer reorderer = new BpVectorReorderer();
    String directory = args[0];
    String field = args[1];
    int threadCount = Runtime.getRuntime().availableProcessors();
    try {
      for (int i = 2; i < args.length; i++) {
        switch (args[i]) {
          case "--max-iters" -> reorderer.setMaxIters(Integer.parseInt(args[++i]));
          case "--min-partition-size" -> reorderer.setMinPartitionSize(Integer.parseInt(args[++i]));
          case "--thread-count" -> threadCount = Integer.parseInt(args[++i]);
          default -> throw new IllegalArgumentException("unknown argument: " + args[i]);
        }
      }
    } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
      usage();
    }
    if (threadCount != 1) {
      ForkJoinPool pool = new ForkJoinPool(threadCount, p -> new ForkJoinWorkerThread(p) {}, null, false);
      reorderer.setForkJoinPool(pool);
    }
    // We need to read prior graph, determine its maxconn in order to make sure our data structures
    // are big enough to handle the HNSW since we will not be creating a new one.
    int maxConn = 0;
    VectorSimilarityFunction similarity = null;
    try (IndexReader reader = DirectoryReader.open(FSDirectory.open(Path.of(directory)))) {
      for (LeafReaderContext ctx: reader.leaves()) {
        FieldInfo finfo = ctx.reader().getFieldInfos().fieldInfo(field);
        if (finfo == null) {
          throw new IllegalStateException("field not found: " + field + " in leaf " + ctx.ord);
        }
        if (finfo.hasVectorValues() == false) {
          throw new IllegalStateException("field not a vector field: " + field + " in leaf " + ctx.ord);
        }
        if (finfo.getVectorEncoding() != VectorEncoding.FLOAT32) {
          throw new IllegalStateException("vector field not encoded as float32: " + field + " in leaf " + ctx.ord);
        }
        if (similarity == null) {
          similarity = finfo.getVectorSimilarityFunction();
        } else if (similarity != finfo.getVectorSimilarityFunction()) {
          throw new IllegalStateException("vector field " + field + " was indexed with inconsistent similarity functions");
        }
        HnswGraph hnsw = ((CodecReader) ctx.reader()).getVectorReader().getGraph(field);
        if (hnsw == null) {
          throw new IllegalStateException("No HNSW graph for vector field: " + field + " in leaf " + ctx.ord);
        }
        maxConn = Math.max(hnsw.maxConn(), maxConn);
      }
      final int maxConnFinal = maxConn;
      reorderer.setVectorSimilarity(similarity);
      IndexWriterConfig iwc = new IndexWriterConfig();
      iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
      iwc.setCodec(new Lucene912Codec() {
          @Override
          public Lucene99HnswVectorsFormat getKnnVectorsFormatForField(String field) {
            // We have no idea what beam width was used to create the graph but it doesn't
            // matter because we will not be searching/reindexing these graphs, merely
            // renumbering them, so use DEFAULT_BEAM_WIDTH to satisfy the codec/format setup.
            return new Lucene99HnswVectorsFormat(maxConnFinal, DEFAULT_BEAM_WIDTH, 1, null);
          }
        });
      try (IndexWriter writer = new IndexWriter(FSDirectory.open(Path.of(directory)), iwc)) {
        for (LeafReaderContext ctx: reader.leaves()) {
          OffHeapFloatVectorValues values = (OffHeapFloatVectorValues) ctx.reader().getFloatVectorValues(field);
          Sorter.DocMap valueMap = reorderer.computeDocMapFloat(values);
          Sorter.DocMap docMap = valueMapToDocMap(valueMap, values, ctx.reader().maxDoc());
          writer.addIndexes(SortingCodecReader.wrap((CodecReader) ctx.reader(), docMap, null));
        }
      }
    }
  }

  private static void usage() {
    throw new IllegalArgumentException("""
              usage: reorder <directory> <field>
                [--max-iters N]
                [--min-partition-size P]
                [--thread-count T]""");
  }


  private static Sorter.DocMap valueMapToDocMap(Sorter.DocMap valueMap, OffHeapFloatVectorValues values, int maxDoc) throws IOException {
    if (maxDoc == values.size()) {
      return valueMap;
    }
    // valueMap maps old/new ords
    // values maps old docs/old ords
    // we want old docs/new docs map
    // sort docs with no value at the end
    int[] newToOld = new int[maxDoc];
    int[] oldToNew = new int[newToOld.length];
    int docid = 0;
    int ord = 0;
    int nextNullDoc = values.size();
    for (int nextDoc = values.nextDoc(); nextDoc != NO_MORE_DOCS; nextDoc = values.nextDoc()) {
      while (docid < nextDoc) {
        oldToNew[docid] = nextNullDoc;
        newToOld[nextNullDoc] = docid;
        ++docid;
        ++nextNullDoc;
      }
      // check me
      assert docid == nextDoc;
      int newOrd = valueMap.oldToNew(ord);
      oldToNew[docid] = newOrd;
      newToOld[newOrd] = docid;
      ++ord;
      ++docid;
    }
    while (docid < maxDoc) {
      oldToNew[docid] = nextNullDoc;
      newToOld[nextNullDoc] = docid;
      ++docid;
      ++nextNullDoc;
    }

    return new Sorter.DocMap() {

      @Override
      public int size() {
        return newToOld.length;
      }

      @Override
      public int oldToNew(int docID) {
        return oldToNew[docID];
      }

      @Override
      public int newToOld(int docID) {
        return newToOld[docID];
      }
    };
  }
}
