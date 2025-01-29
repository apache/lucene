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
package org.apache.lucene.sandbox.vectorsearch;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/** Methods to deal with a CuVS composite file inside a segment */
/*package-private*/ class CuVSSegmentFile implements AutoCloseable {
  private final ZipOutputStream zos;

  private Set<String> filesAdded = new HashSet<String>();

  public CuVSSegmentFile(OutputStream out) {
    zos = new ZipOutputStream(out);
    zos.setLevel(Deflater.NO_COMPRESSION);
  }

  protected Logger log = Logger.getLogger(getClass().getName());

  public void addFile(String name, byte[] bytes) throws IOException {
    /*log.info(
    "Writing the file: "
        + name
        + ", size="
        + bytes.length);*/
    ZipEntry indexFileZipEntry = new ZipEntry(name);
    zos.putNextEntry(indexFileZipEntry);
    zos.write(bytes, 0, bytes.length);
    zos.closeEntry();
    filesAdded.add(name);
  }

  public Set<String> getFilesAdded() {
    return Collections.unmodifiableSet(filesAdded);
  }

  @Override
  public void close() throws IOException {
    zos.close();
  }
}
