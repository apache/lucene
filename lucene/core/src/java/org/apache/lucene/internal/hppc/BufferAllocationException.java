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

package org.apache.lucene.internal.hppc;

import java.util.IllegalFormatException;
import java.util.Locale;

/**
 * BufferAllocationException forked from HPPC.
 *
 * @lucene.internal
 */
public class BufferAllocationException extends RuntimeException {
  public BufferAllocationException(String message) {
    super(message);
  }

  public BufferAllocationException(String message, Object... args) {
    this(message, null, args);
  }

  public BufferAllocationException(String message, Throwable t, Object... args) {
    super(formatMessage(message, t, args), t);
  }

  private static String formatMessage(String message, Throwable t, Object... args) {
    try {
      return String.format(Locale.ROOT, message, args);
    } catch (IllegalFormatException e) {
      BufferAllocationException substitute =
          new BufferAllocationException(message + " [ILLEGAL FORMAT, ARGS SUPPRESSED]");
      if (t != null) {
        substitute.addSuppressed(t);
      }
      substitute.addSuppressed(e);
      throw substitute;
    }
  }
}
