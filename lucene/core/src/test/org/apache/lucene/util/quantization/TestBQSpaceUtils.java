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

package org.apache.lucene.util.quantization;

import org.apache.lucene.tests.util.LuceneTestCase;

public class TestBQSpaceUtils extends LuceneTestCase {

  private static float DELTA = Float.MIN_VALUE;

  public void testPadFloat() {
    assertArrayEquals(
        new float[] {1, 2, 3, 4}, BQSpaceUtils.pad(new float[] {1, 2, 3, 4}, 4), DELTA);
    assertArrayEquals(
        new float[] {1, 2, 3, 4}, BQSpaceUtils.pad(new float[] {1, 2, 3, 4}, 3), DELTA);
    assertArrayEquals(
        new float[] {1, 2, 3, 4, 0}, BQSpaceUtils.pad(new float[] {1, 2, 3, 4}, 5), DELTA);
  }

  public void testPadByte() {
    assertArrayEquals(new byte[] {1, 2, 3, 4}, BQSpaceUtils.pad(new byte[] {1, 2, 3, 4}, 4));
    assertArrayEquals(new byte[] {1, 2, 3, 4}, BQSpaceUtils.pad(new byte[] {1, 2, 3, 4}, 3));
    assertArrayEquals(new byte[] {1, 2, 3, 4, 0}, BQSpaceUtils.pad(new byte[] {1, 2, 3, 4}, 5));
  }
}
