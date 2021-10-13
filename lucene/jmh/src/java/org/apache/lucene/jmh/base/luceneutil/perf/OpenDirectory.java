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
package org.apache.lucene.jmh.base.luceneutil.perf;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;

/** The type Open directory. */
public abstract class OpenDirectory {

  /** Instantiates a new Open directory. */
  public OpenDirectory() {}

  /**
   * Open directory.
   *
   * @param path the path
   * @return the directory
   * @throws IOException the io exception
   */
  public abstract Directory open(Path path) throws IOException;

  /**
   * Get open directory.
   *
   * @param dirImpl the dir
   * @return the open directory
   */
  public static OpenDirectory get(String dirImpl) {
    if (dirImpl.equals("MMapDirectory")) {
      return new OpenDirectory() {
        @Override
        public Directory open(Path path) throws IOException {
          return new MMapDirectory(path);
        }
      };
    } else if (dirImpl.equals("NIOFSDirectory")) {
      return new OpenDirectory() {
        @Override
        public Directory open(Path path) throws IOException {
          return new NIOFSDirectory(path);
        }
      };
    } else if (dirImpl.equals("RAMDirectory")) {
      throw new UnsupportedOperationException("RAMDirectory not supported anymore!");
    } else {
      throw new IllegalArgumentException("unknown directory impl \"" + dirImpl + "\"");
    }
  }
}
