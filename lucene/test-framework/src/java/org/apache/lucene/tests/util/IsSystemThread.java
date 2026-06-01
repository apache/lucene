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

import com.carrotsearch.randomizedtesting.jupiter.DetectThreadLeaks;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.lucene.util.Constants;

/**
 * This predicate should return {@code true} for threads that should be ignored in {@linkplain
 * DetectThreadLeaks thread leak detection}.
 */
public class IsSystemThread implements Predicate<Thread> {
  static final boolean isJ9;

  static {
    isJ9 = Constants.JAVA_VENDOR.startsWith("IBM");
  }

  @Override
  public boolean test(Thread t) {
    var threadName = t.getName();
    switch (threadName) {
      case "ClassCache Reaper": // LUCENE-6518
      case "junit-jupiter-timeout-watcher": // junit5/jupiter timeouts.
      case "JNA Cleaner": // JNA cleaner thread (system).
        return true;
    }

    if (isJ9
        && Stream.of(t.getStackTrace())
            .anyMatch(frame -> frame.getClassName().equals("java.util.Timer$TimerImpl"))) {
      return true;
    }

    return false;
  }
}
