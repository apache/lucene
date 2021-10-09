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

// TODO
//  - be able to quickly run a/b tests again
//  - absorb nrt, pklokup, search, indexing into one tool?
//  - switch to named cmd line args
//  - get pk lookup working w/ remote tasks

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

/** The type Perf utils. */
public class PerfUtils {

  /** Instantiates a new Perf utils. */
  public PerfUtils() {}

  /**
   * Find commit point index commit.
   *
   * @param commit the commit
   * @param dir the dir
   * @return the index commit
   * @throws IOException the io exception
   */
  public static IndexCommit findCommitPoint(String commit, Directory dir) throws IOException {
    List<IndexCommit> commits = DirectoryReader.listCommits(dir);
    Collections.reverse(commits);

    for (final IndexCommit ic : commits) {
      Map<String, String> map = ic.getUserData();
      String ud = null;
      if (map != null) {
        ud = map.get("userData");
        System.out.println("found commit=" + ud);
        if (ud != null && ud.equals(commit)) {
          return ic;
        }
      }
    }
    throw new RuntimeException("could not find commit '" + commit + "'");
  }

  /**
   * Used memory long.
   *
   * @param runtime the runtime
   * @return the long
   */
  public static long usedMemory(Runtime runtime) {
    return runtime.totalMemory() - runtime.freeMemory();
  }

  /**
   * Clear dir.
   *
   * @param directory the directory
   * @throws IOException the io exception
   */
  public static void clearDir(Directory directory) throws IOException {
    for (String file : directory.listAll()) {
      directory.deleteFile(file);
    }
  }

  /**
   * Copy dir.
   *
   * @param from the from
   * @param to the to
   * @throws IOException the io exception
   */
  public static void copyDir(Directory from, Directory to) throws IOException {
    for (String file : from.listAll()) {
      to.copyFrom(from, file, file, IOContext.DEFAULT);
    }
  }
}
