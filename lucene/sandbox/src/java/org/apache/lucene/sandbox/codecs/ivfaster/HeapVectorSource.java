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
package org.apache.lucene.sandbox.codecs.ivfaster;

/**
 * A {@link VectorSource} over rotated vectors already in heap, with their coarse codes packed once
 * up front.
 *
 * <p>What the {@code float[][]} entry points of {@link Clustering} wrap, so a test or a caller that
 * holds its corpus in arrays clusters through the same cursor seam the writer's file-backed source
 * uses.
 */
final class HeapVectorSource implements VectorSource {

  private final float[][] rotated;
  private final int count;
  private final int dim;
  private final DocPlanes planes;

  HeapVectorSource(float[][] rotated, int count, int dim, DocPlanes planes) {
    this.rotated = rotated;
    this.count = count;
    this.dim = dim;
    this.planes = planes;
  }

  @Override
  public int count() {
    return count;
  }

  @Override
  public int dim() {
    return dim;
  }

  @Override
  public Cursor cursor() {
    return new Cursor() {
      private int ord = -1;

      @Override
      public void load(int ord) {
        this.ord = ord;
      }

      @Override
      public float[] vector() {
        return rotated[ord];
      }

      @Override
      public void coarseInto(byte[] dest) {
        planes.copyInto(ord, dest);
      }
    };
  }
}
