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
package org.apache.lucene.sandbox.store;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.tests.store.BaseDirectoryTestCase;

/**
 * Runs the generic directory suite against these reads: every file is served through io_uring here,
 * rather than only the vector files, so that the randomized reads, slices, clones and end-of-file
 * behaviour actually exercise this input rather than the delegate's.
 */
public class TestIoUringUncachedDirectoryCompliance extends BaseDirectoryTestCase {

  @Override
  protected Directory getDirectory(Path path) throws IOException {
    assumeTrue(
        "io_uring not available", Boolean.getBoolean("tests.iouring.run") && IoUring.isAvailable());
    return new IoUringUncachedDirectory(
        FSDirectory.open(path), 64, true, 8, random().nextInt(101), true);
  }
}
