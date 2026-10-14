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
package org.apache.lucene.gradle.plugins.licenses;

import groovy.json.JsonOutput;
import groovy.json.JsonSlurper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Information about a single dependency (jar file).
 *
 * @param id full module coordinates
 * @param module just the module name
 * @param fileName file name (resolved, with version)
 * @param checksum the checksum
 */
record JarInfo(String id, String module, String fileName, String checksum)
    implements Comparable<JarInfo> {
  @Override
  public int compareTo(JarInfo o) {
    return this.fileName().compareTo(o.fileName());
  }

  static void serialize(Path path, List<JarInfo> jarInfos) {
    try {
      Files.writeString(path, JsonOutput.prettyPrint(JsonOutput.toJson(jarInfos)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static List<JarInfo> deserialize(Path input) {
    try {
      @SuppressWarnings("unchecked")
      var asList = (List<Map<String, String>>) new JsonSlurper().parse(input);
      return asList.stream()
          .map(
              asMap ->
                  new JarInfo(
                      asMap.get("id"),
                      asMap.get("module"),
                      asMap.get("fileName"),
                      asMap.get("checksum")))
          .toList();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
