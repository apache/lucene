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

import java.io.Closeable;
import org.apache.lucene.tests.store.BaseDirectoryWrapper;
import org.junit.Assert;

/**
 * Fails the test if the {@link BaseDirectoryWrapper} hasn't been closed and the test has no other
 * failures.
 */
final class AssertDirectoryClosed implements Closeable {
  static final String MSG_PREFIX = "Directory not closed: ";

  private final BaseDirectoryWrapper dir;
  private final SuiteFailureState failureMarker;

  AssertDirectoryClosed(BaseDirectoryWrapper dir, SuiteFailureState failureMarker) {
    this.dir = dir;
    this.failureMarker = failureMarker;
  }

  @Override
  public void close() {
    // We only attempt to check open/closed state if there were no other test
    // failures.
    try {
      if (failureMarker.wasSuccessful() && dir.isOpen()) {
        Assert.fail(MSG_PREFIX + dir);
      }
    } finally {
      try {
        dir.close();
      } catch (Throwable _) {
        // ignore, we can't do much about it. LUCENE-4058
      }
    }
  }
}
