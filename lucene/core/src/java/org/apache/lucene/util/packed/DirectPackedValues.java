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
package org.apache.lucene.util.packed;

import org.apache.lucene.store.RandomAccessInput;

/**
 * Tells where the values of a {@link DirectReader} live, so that callers can read many of them at
 * once instead of calling {@link org.apache.lucene.util.LongValues#get(long)} for each one. Values
 * are laid out as written by {@link DirectWriter}: value {@code i} starts at bit {@code i *
 * bitsPerValue()}, {@link #offset()} bytes into {@link #input()}.
 *
 * @lucene.internal
 */
public interface DirectPackedValues {

  /** Number of bits per value. */
  int bitsPerValue();

  /** The input that values are read from. */
  RandomAccessInput input();

  /** Offset, in bytes, of the first value in {@link #input()}. */
  long offset();
}
