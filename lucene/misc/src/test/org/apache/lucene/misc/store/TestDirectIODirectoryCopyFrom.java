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
package org.apache.lucene.misc.store;

import java.util.HashSet;
import java.util.OptionalLong;
import java.util.Set;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MergeInfo;
import org.apache.lucene.tests.util.LuceneTestCase;

/** Tests {@link DirectIODirectory#copyFrom} without requiring actual O_DIRECT support. */
public class TestDirectIODirectoryCopyFrom extends LuceneTestCase {

  public void testCopyFromConsultsUseDirectIO() throws Exception {
    Set<String> consulted = new HashSet<>();
    try (Directory srcDir = FSDirectory.open(createTempDir("src"));
        DirectIODirectory dir =
            new DirectIODirectory(FSDirectory.open(createTempDir("dest"))) {
              @Override
              protected boolean useDirectIO(
                  String name, IOContext context, OptionalLong fileLength) {
                consulted.add(name);
                return false;
              }
            }) {
      try (IndexOutput out = srcDir.createOutput("src", IOContext.DEFAULT)) {
        out.writeBytes(new byte[8], 8);
      }
      dir.copyFrom(srcDir, "src", "dest", IOContext.merge(new MergeInfo(10, 8, false, 2)));
      assertTrue(
          "copyFrom must route through createOutput/useDirectIO", consulted.contains("dest"));
    }
  }
}
