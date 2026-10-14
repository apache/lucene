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

package org.apache.lucene.tests.util;

import java.util.Random;

/**
 * A {@link Random} subclass that counts calls to its methods and throws an exception if their
 * number exceeds the provided thresholds.
 *
 * <p>This can be used to debug too many calls to a random returned by {@link
 * LuceneTestCase#random()}.
 */
final class MaxCallCountRandom extends Random {
  private final Random delegate;
  // it doesn't have to be volatile (or thread-safe). just an approximation.
  private long useCount;
  private final long maxCalls;

  public MaxCallCountRandom(Random delegate, long maxCalls) {
    this.delegate = delegate;
    this.maxCalls = maxCalls;
  }

  private void incrementUseCount() {
    if (useCount++ > maxCalls) {
      throw new RuntimeException("Random.* use count exceeded the limit.");
    }
  }

  @Override
  protected int next(int bits) {
    throw new RuntimeException("Shouldn't be reachable.");
  }

  @Override
  public boolean nextBoolean() {
    incrementUseCount();
    return delegate.nextBoolean();
  }

  @Override
  public void nextBytes(byte[] bytes) {
    incrementUseCount();
    delegate.nextBytes(bytes);
  }

  @Override
  public double nextDouble() {
    incrementUseCount();
    return delegate.nextDouble();
  }

  @Override
  public float nextFloat() {
    incrementUseCount();
    return delegate.nextFloat();
  }

  @Override
  public double nextGaussian() {
    incrementUseCount();
    return delegate.nextGaussian();
  }

  @Override
  public int nextInt() {
    incrementUseCount();
    return delegate.nextInt();
  }

  @Override
  public int nextInt(int n) {
    incrementUseCount();
    return delegate.nextInt(n);
  }

  @Override
  public long nextLong() {
    incrementUseCount();
    return delegate.nextLong();
  }
}
