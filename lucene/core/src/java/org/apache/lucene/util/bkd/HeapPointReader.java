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
package org.apache.lucene.util.bkd;

import java.util.function.IntFunction;

/**
 * Utility class to read buffered points from in-heap arrays.
 *
 * @lucene.internal
 */
public final class HeapPointReader implements PointReader {
  private int curRead;
  private final int end;
  private final IntFunction<PointValue> points;

  HeapPointReader(IntFunction<PointValue> points, int start, int end) {
    curRead = start - 1;
    this.end = end;
    this.points = points;
  }

  @Override
  public boolean next() {
    curRead++;
    return curRead < end;
  }

  @Override
  public PointValue pointValue() {
    return points.apply(curRead);
  }

  @Override
  public void close() {}
}
