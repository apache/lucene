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
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;

final class TrackingTmpOutputDirectoryWrapper extends FilterDirectory {
  private final Map<String, String> fileNames = new HashMap<>();

  TrackingTmpOutputDirectoryWrapper(Directory in) {
    super(in);
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    IndexOutput output = super.createTempOutput(name, "", context);
    fileNames.put(name, output.getName());
    return output;
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    // keep the original file name if no match, it might be a temp file already
    String tmpName = fileNames.getOrDefault(name, name);
    return super.openInput(tmpName, context);
  }

  @Override
  public void copyFrom(Directory from, String src, String dest, IOContext context)
      throws IOException {
    // the inherited failure cleanup would delete dest, but createOutput() redirects dest to a
    // temp file; on failure remove the mapping and delete the temp file instead
    try (IndexInput is = from.openInput(src, IOContext.READONCE);
        IndexOutput os = createOutput(dest, context)) {
      os.copyBytes(is, is.length());
    } catch (Throwable t) {
      String tmpName = fileNames.remove(dest);
      if (tmpName != null) {
        IOUtils.deleteFilesSuppressingExceptions(t, in, tmpName);
      }
      throw t;
    }
  }

  public Map<String, String> getTemporaryFiles() {
    return fileNames;
  }
}
