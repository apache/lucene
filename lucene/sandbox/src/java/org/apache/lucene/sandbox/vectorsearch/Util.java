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

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.commons.lang3.SerializationUtils;

public class Util {

  public static ByteArrayOutputStream getZipEntryBAOS(
      String fileName, SegmentInputStream segInputStream) throws IOException {
    segInputStream.reset();
    ZipInputStream zipInputStream = new ZipInputStream(segInputStream);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    boolean fileFound = false;
    ZipEntry zipEntry;
    while (zipInputStream.available() == 1
        && ((zipEntry = zipInputStream.getNextEntry()) != null)) {
      if (zipEntry.getName().equals(fileName)) {
        fileFound = true;
        byte[] buffer = new byte[1024];
        int length;
        while ((length = zipInputStream.read(buffer)) != -1) {
          baos.write(buffer, 0, length);
        }
      }
    }
    if (!fileFound) throw new FileNotFoundException();
    return baos;
  }

  // private static final Logger log = Logger.getLogger(Util.class.getName());

  public static ArrayList<float[]> getMergedVectors(
      List<SegmentInputStream> segInputStreams, String fieldName, String mergedSegmentName)
      throws IOException {
    ZipEntry zs;
    ArrayList<float[]> mergedVectors = new ArrayList<float[]>();
    // log.info("Getting mergedVectors...");
    for (SegmentInputStream segInputStream : segInputStreams) {
      segInputStream.reset();
      ZipInputStream zipStream = new ZipInputStream(segInputStream);
      while ((zs = zipStream.getNextEntry()) != null) {
        // log.info("Getting mergedVectors... " + zs.getName());
        byte[] buffer = new byte[1024];
        int length;
        if (zs.getName().endsWith(".vec")) {
          String field = zs.getName().split("\\.")[0].split("/")[1];
          if (fieldName.equals(field)) {
            ByteArrayOutputStream baosM = new ByteArrayOutputStream();
            while ((length = zipStream.read(buffer)) != -1) {
              baosM.write(buffer, 0, length);
            }
            List<float[]> m = SerializationUtils.deserialize(baosM.toByteArray());
            mergedVectors.addAll(m);
          }
        }
      }
    }
    return mergedVectors;
  }
}
