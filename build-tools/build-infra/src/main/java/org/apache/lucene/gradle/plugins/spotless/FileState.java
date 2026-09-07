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
package org.apache.lucene.gradle.plugins.spotless;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * The "state" of a source file. We'll just follow rsync and assume that a file with the same size
 * and the same last-update timestamp hasn't changed.
 */
record FileState(long size, long lastUpdate) {
  public static FileState of(Path path) throws IOException {
    return new FileState(Files.size(path), Files.getLastModifiedTime(path).toMillis());
  }
}
