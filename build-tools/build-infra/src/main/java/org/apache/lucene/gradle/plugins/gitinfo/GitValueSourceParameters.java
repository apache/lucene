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
package org.apache.lucene.gradle.plugins.gitinfo;

import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.FileSystemLocation;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.ValueSourceParameters;
import org.gradle.api.services.BuildServiceParameters;
import org.gradle.process.ExecSpec;

public abstract class GitValueSourceParameters
    implements BuildServiceParameters, ValueSourceParameters {
  public abstract DirectoryProperty getRootProjectDir();

  public abstract Property<String> getGitExec();

  public abstract Property<FileSystemLocation> getDotDir();

  void configure(ExecSpec spec) {
    String workDir = getRootProjectDir().getAsFile().get().getAbsolutePath();
    spec.setWorkingDir(workDir);
    spec.environment("GIT_DIR", getDotDir().get().getAsFile().getAbsolutePath());
  }
}
