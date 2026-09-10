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

package org.apache.lucene.index;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.mockfile.ExtrasFS;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestTrackingTmpOutputDirectoryWrapper extends LuceneTestCase {

  public void testCopyFromTracksLogicalNameInTemporaryFiles() throws IOException {
    try (Directory source = newDirectory();
        Directory backing = newDirectory()) {
      try (IndexOutput out = source.createOutput("src", IOContext.DEFAULT)) {
        out.writeBytes(new byte[] {1, 2, 3}, 3);
      }
      TrackingTmpOutputDirectoryWrapper wrapper = new TrackingTmpOutputDirectoryWrapper(backing);
      wrapper.copyFrom(source, "src", "dest", IOContext.DEFAULT);

      assertTrue(
          "copyFrom must register dest in getTemporaryFiles() via createOutput()",
          wrapper.getTemporaryFiles().containsKey("dest"));
      String tmpName = wrapper.getTemporaryFiles().get("dest");
      assertNotEquals("temp file name must differ from logical name", "dest", tmpName);
      try (IndexInput in = wrapper.openInput("dest", IOContext.DEFAULT)) {
        assertEquals(3L, in.length());
      }
    }
  }

  public void testCopyFromCleansUpOnFailure() throws IOException {
    try (Directory source = newDirectory();
        Directory backing = newDirectory()) {
      try (IndexOutput out = source.createOutput("src", IOContext.DEFAULT)) {
        out.writeBytes(new byte[] {1, 2, 3}, 3);
      }
      // Source whose reads always throw, so copyBytes fails after createOutput succeeds.
      Directory failingSource =
          new FilterDirectory(source) {
            @Override
            public IndexInput openInput(String name, IOContext context) throws IOException {
              return new FilterIndexInput("failing:" + name, super.openInput(name, context)) {
                @Override
                public void readBytes(byte[] b, int offset, int len) throws IOException {
                  throw new IOException("simulated read failure");
                }
              };
            }
          };

      TrackingTmpOutputDirectoryWrapper wrapper = new TrackingTmpOutputDirectoryWrapper(backing);
      expectThrows(
          IOException.class,
          () -> wrapper.copyFrom(failingSource, "src", "dest", IOContext.DEFAULT));

      assertFalse(
          "dest must not remain in getTemporaryFiles() after failed copyFrom",
          wrapper.getTemporaryFiles().containsKey("dest"));
      assertEquals(
          "temp file must be deleted from backing dir after failed copyFrom",
          List.of(),
          Arrays.stream(backing.listAll()).filter(f -> ExtrasFS.isExtra(f) == false).toList());
    }
  }
}
