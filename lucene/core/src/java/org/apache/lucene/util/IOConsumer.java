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
package org.apache.lucene.util;

import java.io.IOException;

/**
 * An IO operation with a single input that may throw an IOException.
 *
 * @see java.util.function.Consumer
 * @param <T> the consumer's input type.
 */
@FunctionalInterface
@SuppressWarnings("removal")
public interface IOConsumer<T> extends IOUtils.IOConsumer<T> {
  /**
   * Performs this operation on the given argument.
   *
   * @param input the input argument
   * @throws IOException if producing the result throws an {@link IOException}
   */
  @Override
  void accept(T input) throws IOException;
}
