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

/** Base class for docid-reorderers implemented using binary partitioning (BP). */
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

  protected int minPartitionSize = DEFAULT_MIN_PARTITION_SIZE;
  protected int maxIters = DEFAULT_MAX_ITERS;
  protected double ramBudgetMB;

  public AbstractBPReorderer() {
    // 10% of the available heap size by default
    setRAMBudgetMB(Runtime.getRuntime().totalMemory() / 1024d / 1024d / 10d);
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
   * Set the amount of RAM that graph partitioning is allowed to use. More RAM allows running
   * faster. If not enough RAM is provided, a {@link NotEnoughRAMException} will be thrown. This is
   * 10% of the total heap size by default.
   */
  public void setRAMBudgetMB(double ramBudgetMB) {
    this.ramBudgetMB = ramBudgetMB;
  }

  /** Exception that is thrown when not enough RAM is available. */
  public static class NotEnoughRAMException extends RuntimeException {
    NotEnoughRAMException(String message) {
      super(message);
    }
  }
}
