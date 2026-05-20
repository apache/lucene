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
package org.apache.lucene.misc.index;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.RecursiveAction;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.SortingCodecReader;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.CloseableThreadLocal;
import org.apache.lucene.util.IntroSelector;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.VectorUtil;

/**
 * Implementation of "recursive graph bisection", also called "bipartite graph partitioning" and
 * often abbreviated BP, an approach to doc ID assignment that aims at reducing the sum of the log
 * gap between consecutive neighbor node ids. See {@link BPIndexReorderer}.
 */
public class BpVectorReorderer extends AbstractBPReorderer {

  /*
   * Note on using centroids (mean of vectors in a partition) to maximize scores of pairs of vectors within each partition:
   * The function used to compare the vectors must have higher values for vectors
   * that are more similar to each other, and must preserve inequalities over sums of vectors.
   *
   * <p>This property enables us to use the centroid of a collection of vectors to represent the
   * collection. For Euclidean and inner product score functions, the centroid is the point that
   * minimizes the sum of distances from all the points (thus maximizing the score).
   *
   * <p>sum((c0 - v)^2) = n * c0^2 - 2 * c0 * sum(v) + sum(v^2) taking derivative w.r.t. c0 and
   * setting to 0 we get sum(v) = n * c0; i.e. c0 (the centroid) is the place that minimizes the sum
   * of (l2) distances from the vectors (thus maximizing the euclidean score function).
   *
   * <p>to maximize dot-product over unit vectors, note that: sum(dot(c0, v)) = dot(c0, sum(v))
   * which is maximized, again, when c0 = sum(v) / n.  For max inner product score, vectors may not
   * be unit vectors. In this case there is no maximum, but since all colinear vectors of whatever
   * scale will generate the same partition for these angular scores, we are free to choose any
   * scale and ignore the normalization factor.
   */

  /** Minimum problem size that will result in tasks being split. */
  private static final int FORK_THRESHOLD = 8192;

  /**
   * Limits how many incremental updates we do before initiating a full recalculation. Some wasted
   * work is done when this is exceeded, but more is saved when it is not. Setting this to zero
   * prevents any incremental updates from being done, instead the centroids are fully recalculated
   * for each iteration. We're not able to make it very big since too much numerical error
   * accumulates, which seems to be around 50, thus resulting in suboptimal reordering. It's not
   * clear how helpful this is though; measurements vary, so it is currently disabled (= 0).
   */
  private static final int MAX_CENTROID_UPDATES = 0;

  private final String partitionField;

  /** Constructor. */
  public BpVectorReorderer(String partitionField) {
    setMinPartitionSize(DEFAULT_MIN_PARTITION_SIZE);
    setMaxIters(DEFAULT_MAX_ITERS);
    // 10% of the available heap size by default
    setRAMBudgetMB(Runtime.getRuntime().totalMemory() / 1024d / 1024d / 10d);
    this.partitionField = partitionField;
  }

  private static class PerThreadState {

    final FloatVectorValues vectors;
    final float[] leftCentroid;
    final float[] rightCentroid;
    final float[] scratch;

    PerThreadState(FloatVectorValues vectors) {
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

    protected final TaskExecutor executor;
    protected final int depth;

    BaseRecursiveAction(TaskExecutor executor, int depth) {
      this.executor = executor;
      this.depth = depth;
    }

    protected final boolean shouldFork(int problemSize, int totalProblemSize) {
      if (executor == null) {
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

    private final VectorSimilarityFunction vectorScore;
    // the ids assigned to this task, a sub-range of all the ids
    private final IntsRef ids;
    // the biases for the ids - a number < 0 when the doc goes left and > 0 for right
    private final float[] biases;
    private final CloseableThreadLocal<PerThreadState> threadLocal;

    ReorderTask(
        IntsRef ids,
        float[] biases,
        CloseableThreadLocal<PerThreadState> threadLocal,
        TaskExecutor executor,
        int depth,
        VectorSimilarityFunction vectorScore) {
      super(executor, depth);
      this.ids = ids;
      this.biases = biases;
      this.threadLocal = threadLocal;
      this.vectorScore = vectorScore;
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
      FloatVectorValues vectors = state.vectors;
      float[] leftCentroid = state.leftCentroid;
      float[] rightCentroid = state.rightCentroid;
      float[] scratch = state.scratch;

      try {
        computeCentroid(left, vectors, leftCentroid, vectorScore);
        computeCentroid(right, vectors, rightCentroid, vectorScore);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      for (int iter = 0; iter < maxIters; ++iter) {
        int moved;
        try {
          moved = shuffle(vectors, ids, right.offset, leftCentroid, rightCentroid, scratch, biases);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        if (moved == 0) {
          break;
        }
        if (moved > MAX_CENTROID_UPDATES) {
          // if we swapped too many times we don't use the relative calculation because it
          // introduces too much error
          try {
            computeCentroid(left, vectors, leftCentroid, vectorScore);
            computeCentroid(right, vectors, rightCentroid, vectorScore);
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        }
      }

      // It is fine for all tasks to share the same docs / biases array since they all work on
      // different slices of the array at a given point in time.
      ReorderTask leftTask =
          new ReorderTask(left, biases, threadLocal, executor, depth + 1, vectorScore);
      ReorderTask rightTask =
          new ReorderTask(right, biases, threadLocal, executor, depth + 1, vectorScore);

      if (shouldFork(ids.length, ids.ints.length)) {
        invokeAll(leftTask, rightTask);
      } else {
        leftTask.compute();
        rightTask.compute();
      }
    }

    static void computeCentroid(
        IntsRef ids,
        FloatVectorValues vectors,
        float[] centroid,
        VectorSimilarityFunction vectorSimilarity)
        throws IOException {
      Arrays.fill(centroid, 0);
      for (int i = ids.offset; i < ids.offset + ids.length; i++) {
        VectorUtil.add(centroid, vectors.vectorValue(ids.ints[i]));
      }
      switch (vectorSimilarity) {
        case EUCLIDEAN, MAXIMUM_INNER_PRODUCT -> vectorScalarMul(1 / (float) ids.length, centroid);
        case DOT_PRODUCT, COSINE ->
            vectorScalarMul(
                1 / (float) Math.sqrt(VectorUtil.dotProduct(centroid, centroid)), centroid);
      }
    }

    /** Shuffle IDs across both partitions so that each partition is closer to its centroid. */
    private int shuffle(
        FloatVectorValues vectors,
        IntsRef ids,
        int midPoint,
        float[] leftCentroid,
        float[] rightCentroid,
        float[] scratch,
        float[] biases)
        throws IOException {

      /* Computing biases requires a distance calculation for each vector (document) which can be
       * costly, especially as the vector dimension increases, so we try to parallelize it.  We also
       * have the option of performing incremental updates based on the difference of the previous and
       * the new centroid, which can be less costly, but introduces incremental numeric error, and
       * needs tuning to be usable. It is disabled by default (see MAX_CENTROID_UPDATES).
       */
      new ComputeBiasTask(
              ids.ints,
              biases,
              ids.offset,
              ids.offset + ids.length,
              leftCentroid,
              rightCentroid,
              threadLocal,
              executor,
              depth,
              vectorScore)
          .compute();
      vectorSubtract(leftCentroid, rightCentroid, scratch);
      float scale = (float) Math.sqrt(VectorUtil.dotProduct(scratch, scratch));
      float maxLeftBias = Float.NEGATIVE_INFINITY;
      for (int i = ids.offset; i < midPoint; ++i) {
        maxLeftBias = Math.max(maxLeftBias, biases[i]);
      }
      float minRightBias = Float.POSITIVE_INFINITY;
      for (int i = midPoint, end = ids.offset + ids.length; i < end; ++i) {
        minRightBias = Math.min(minRightBias, biases[i]);
      }
      float gain = maxLeftBias - minRightBias;
      /* This compares the gain of swapping the doc from the left side that is most attracted to the
       * right and the doc from the right side that is most attracted to the left against the
       * average vector length (/500) rather than zero.  500 is an arbitrary heuristic value
       * determined empirically - basically we stop iterating once the centroids move less than
       * 1/500 the sum of their lengths.
       *
       * TODO We could try incorporating simulated annealing by including the iteration number in the
       * formula? eg { 1000 * gain <= scale * iter }.
       */

      // System.out.printf("at depth=%d, midPoint=%d, gain=%f\n", depth, midPoint, gain);
      if (500 * gain <= scale) {
        return 0;
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
            count++;
            int from = Math.min(i, j);
            int to = Math.max(i, j);
            try {
              swapIdsAndCentroids(
                  ids,
                  from,
                  to,
                  midPoint,
                  vectors,
                  leftCentroid,
                  rightCentroid,
                  scratch,
                  count,
                  vectorScore);
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          }
        }
      }

      Selector selector = new Selector();
      selector.select(ids.offset, ids.offset + ids.length, midPoint);
      // System.out.printf("swapped %d / %d\n", selector.count, ids.length);
      return selector.count;
    }

    private static boolean centroidValid(
        float[] centroid, FloatVectorValues vectors, IntsRef ids, int count) throws IOException {
      // recompute centroid to check the incremental calculation
      float[] check = new float[centroid.length];
      computeCentroid(ids, vectors, check, VectorSimilarityFunction.EUCLIDEAN);
      for (int i = 0; i < check.length; ++i) {
        float diff = Math.abs(check[i] - centroid[i]);
        if (diff > 1e-4) {
          return false;
        }
      }
      return true;
    }

    private static void swapIdsAndCentroids(
        IntsRef ids,
        int from,
        int to,
        int midPoint,
        FloatVectorValues vectors,
        float[] leftCentroid,
        float[] rightCentroid,
        float[] scratch,
        int count,
        VectorSimilarityFunction vectorScore)
        throws IOException {
      assert from < to;

      int[] idArr = ids.ints;
      int fromId = idArr[from];
      int toId = idArr[to];

      // Now update the centroids, this makes things much faster than invalidating it and having to
      // recompute on the next iteration.  Should be faster than full recalculation if the number of
      // swaps is reasonable.

      // We want the net effect to be moving "from" left->right and "to" right->left

      // (1) scratch = to - from
      if (count <= MAX_CENTROID_UPDATES && vectorScore == VectorSimilarityFunction.EUCLIDEAN) {
        int relativeMidpoint = midPoint - ids.offset;
        vectorSubtract(vectors.vectorValue(toId), vectors.vectorValue(fromId), scratch);
        // we must normalize to the proper scale by accounting for the number of points contributing
        // to each centroid
        // left += scratch / size(left)
        vectorScalarMul(1 / (float) relativeMidpoint, scratch);
        VectorUtil.add(leftCentroid, scratch);
        // right -= scratch / size(right)
        vectorScalarMul(-relativeMidpoint / (float) (ids.length - relativeMidpoint), scratch);
        VectorUtil.add(rightCentroid, scratch);
      }

      idArr[from] = toId;
      idArr[to] = fromId;

      if (count <= MAX_CENTROID_UPDATES) {
        assert centroidValid(
            leftCentroid, vectors, new IntsRef(idArr, ids.offset, midPoint - ids.offset), count);
        assert centroidValid(
            rightCentroid,
            vectors,
            new IntsRef(ids.ints, midPoint, ids.length - midPoint + ids.offset),
            count);
      }
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
    private final VectorSimilarityFunction vectorScore;

    ComputeBiasTask(
        int[] ids,
        float[] biases,
        int start,
        int end,
        float[] leftCentroid,
        float[] rightCentroid,
        CloseableThreadLocal<PerThreadState> threadLocal,
        TaskExecutor executor,
        int depth,
        VectorSimilarityFunction vectorScore) {
      super(executor, depth);
      this.ids = ids;
      this.biases = biases;
      this.start = start;
      this.end = end;
      this.leftCentroid = leftCentroid;
      this.rightCentroid = rightCentroid;
      this.threadLocal = threadLocal;
      this.vectorScore = vectorScore;
    }

    @Override
    protected void compute() {
      final int problemSize = end - start;
      if (problemSize > 1 && shouldFork(problemSize, ids.length)) {
        final int mid = (start + end) >>> 1;
        invokeAll(
            new ComputeBiasTask(
                ids,
                biases,
                start,
                mid,
                leftCentroid,
                rightCentroid,
                threadLocal,
                executor,
                depth,
                vectorScore),
            new ComputeBiasTask(
                ids,
                biases,
                mid,
                end,
                leftCentroid,
                rightCentroid,
                threadLocal,
                executor,
                depth,
                vectorScore));
      } else {
        FloatVectorValues vectors = threadLocal.get().vectors;
        try {
          for (int i = start; i < end; ++i) {
            biases[i] = computeBias(vectors.vectorValue(ids[i]), leftCentroid, rightCentroid);
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
        case EUCLIDEAN ->
            VectorUtil.squareDistance(vector, leftCentroid)
                - VectorUtil.squareDistance(vector, rightCentroid);
        case MAXIMUM_INNER_PRODUCT, COSINE, DOT_PRODUCT ->
            VectorUtil.dotProduct(vector, rightCentroid)
                - VectorUtil.dotProduct(vector, leftCentroid);
        default -> throw new IllegalStateException("unsupported vector score: " + vectorScore);
      };
    }
  }

  @Override
  public Sorter.DocMap computeDocMap(CodecReader reader, Directory tempDir, Executor executor)
      throws IOException {
    TaskExecutor taskExecutor;
    if (executor == null) {
      taskExecutor = null;
    } else {
      taskExecutor = new TaskExecutor(executor);
    }
    VectorSimilarityFunction vectorScore = checkField(reader, partitionField);
    if (vectorScore == null) {
      return null;
    }
    FloatVectorValues floats = reader.getFloatVectorValues(partitionField);
    Sorter.DocMap valueMap = computeValueMap(floats, vectorScore, taskExecutor);
    return valueMapToDocMap(valueMap, floats, reader.maxDoc());
  }

  /** Expert: Compute the {@link DocMap} that holds the new vector ordinal numbering. */
  Sorter.DocMap computeValueMap(
      FloatVectorValues vectors, VectorSimilarityFunction vectorScore, TaskExecutor executor) {
    if (docRAMRequirements(vectors.size()) >= ramBudgetMB * 1024 * 1024) {
      throw new NotEnoughRAMException(
          "At least "
              + Math.ceil(docRAMRequirements(vectors.size()) / 1024. / 1024.)
              + "MB of RAM are required to hold metadata about documents in RAM, but current RAM budget is "
              + ramBudgetMB
              + "MB");
    }
    return new DocMap(computePermutation(vectors, vectorScore, executor));
  }

  /**
   * Compute a permutation of the ID space that maximizes vector score between consecutive postings.
   */
  private int[] computePermutation(
      FloatVectorValues vectors, VectorSimilarityFunction vectorScore, TaskExecutor executor) {
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
      new ReorderTask(ids, new float[size], threadLocal, executor, 0, vectorScore).compute();
    }
    return sortedIds;
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
   * @param args two args: a path containing an index to reorder. the name of the field the contents
   *     of which to use for reordering
   */
  @SuppressWarnings("unused")
  public static void main(String... args) throws IOException {
    if (args.length < 2 || args.length > 8) {
      usage();
    }
    String directory = args[0];
    String field = args[1];
    BpVectorReorderer reorderer = new BpVectorReorderer(field);
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
    Executor executor;
    if (threadCount != 1) {
      executor = new ForkJoinPool(threadCount, p -> new ForkJoinWorkerThread(p) {}, null, false);
    } else {
      executor = null;
    }
    try (Directory dir = FSDirectory.open(Path.of(directory))) {
      reorderer.reorderIndexDirectory(dir, executor);
    }
  }

  void reorderIndexDirectory(Directory directory, Executor executor) throws IOException {
    try (IndexReader reader = DirectoryReader.open(directory)) {
      IndexWriterConfig iwc = new IndexWriterConfig();
      iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
      try (IndexWriter writer = new IndexWriter(directory, iwc)) {
        for (LeafReaderContext ctx : reader.leaves()) {
          CodecReader codecReader = (CodecReader) ctx.reader();
          writer.addIndexes(
              SortingCodecReader.wrap(
                  codecReader, computeDocMap(codecReader, null, executor), null));
        }
      }
    }
  }

  private static VectorSimilarityFunction checkField(CodecReader reader, String field)
      throws IOException {
    FieldInfo finfo = reader.getFieldInfos().fieldInfo(field);
    if (finfo == null) {
      return null;
      /*
      throw new IllegalStateException(
          "field not found: " + field + " in leaf " + reader.getContext().ord);
      */
    }
    if (finfo.hasVectorValues() == false) {
      return null;
      /*
      throw new IllegalStateException(
          "field not a vector field: " + field + " in leaf " + reader.getContext().ord);
      */
    }
    if (finfo.getVectorEncoding() != VectorEncoding.FLOAT32) {
      return null;
      /*
      throw new IllegalStateException(
          "vector field not encoded as float32: " + field + " in leaf " + reader.getContext().ord);
      */
    }
    return finfo.getVectorSimilarityFunction();
  }

  private static void usage() {
    throw new IllegalArgumentException(
        """
              usage: reorder <directory> <field>
                [--max-iters N]
                [--min-partition-size P]
                [--thread-count T]""");
  }

  private static Sorter.DocMap valueMapToDocMap(
      Sorter.DocMap valueMap, FloatVectorValues values, int maxDoc) throws IOException {
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
    DocIdSetIterator it = values.iterator();
    for (int nextDoc = it.nextDoc(); nextDoc != NO_MORE_DOCS; nextDoc = it.nextDoc()) {
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
