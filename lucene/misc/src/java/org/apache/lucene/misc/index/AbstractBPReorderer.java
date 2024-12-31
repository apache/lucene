package org.apache.lucene.misc.index;

public abstract class AbstractBPReorderer implements IndexReorderer {
  /**
   * Minimum size of partitions. The algorithm will stop recursing when reaching partitions below
   * this number of documents: 32.
   */
  public static final int DEFAULT_MIN_PARTITION_SIZE = 32;
  /**
   * Default maximum number of iterations per recursion level: 20. Higher numbers of iterations
   * typically don't help significantly.
   */
  public static final int DEFAULT_MAX_ITERS = 20;
  protected int minPartitionSize;
  protected int maxIters;
  protected double ramBudgetMB;

  /**
   * Set the minimum partition size, when the algorithm stops recursing, 32 by default.
   */
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
   * Set the amount of RAM that graph partitioning is allowed to use. More RAM allows running
   * faster. If not enough RAM is provided, a {@link NotEnoughRAMException} will be thrown. This is
   * 10% of the total heap size by default.
   */
  public void setRAMBudgetMB(double ramBudgetMB) {
    this.ramBudgetMB = ramBudgetMB;
  }

  /**
   * Exception that is thrown when not enough RAM is available.
   */
  public static class NotEnoughRAMException extends RuntimeException {
    NotEnoughRAMException(String message) {
      super(message);
    }
  }
}
