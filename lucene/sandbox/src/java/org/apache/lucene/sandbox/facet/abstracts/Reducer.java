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
package org.apache.lucene.sandbox.facet.abstracts;

/** Reducer for numeric values. * */
public interface Reducer {

  /** Int values reducer. * */
  default int reduce(int a, int b) {
    throw new UnsupportedOperationException("Incompatible types");
  }
  ;

  /** Long values reducer. * */
  default long reduce(long a, long b) {
    throw new UnsupportedOperationException("Incompatible types");
  }
  ;

  /** Float values reducer. * */
  default float reduce(float a, float b) {
    throw new UnsupportedOperationException("Incompatible types");
  }
  ;

  /** Double values reducer. * */
  default double reduce(double a, double b) {
    throw new UnsupportedOperationException("Incompatible types");
  }
  ;

  /** Reducer that returns MAX of two values. * */
  public static final Reducer MAX =
      new Reducer() {
        @Override
        public long reduce(long a, long b) {
          return Math.max(a, b);
        }
      };

  /** Reducer that returns SUM of two values. * */
  public static final Reducer SUM =
      new Reducer() {
        @Override
        public long reduce(long a, long b) {
          return Math.addExact(a, b);
        }
      };
}
