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

package org.apache.lucene.gradle.validation;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public final class JarInfo {

  private final String name;
  private final String jarName;
  private final String module;
  private final String checksum;

  // We keep track of the files referenced by this dependency (sha, license, notice, etc.)
  // so that we can determine unused dangling files later on.
  private final transient List<File> referencedFiles;

  public JarInfo(String name, String jarName, String module, String checksum) {
    this.name = name;
    this.jarName = jarName;
    this.module = module;
    this.checksum = checksum;
    this.referencedFiles = new ArrayList<>();
  }

  public void addReferencedFile(File file) {
    referencedFiles.add(file);
  }

  public String getName() {
    return name;
  }

  public String getJarName() {
    return jarName;
  }

  public String getModule() {
    return module;
  }

  public String getChecksum() {
    return checksum;
  }

  public List<File> getReferencedFiles() {
    return referencedFiles;
  }
}
