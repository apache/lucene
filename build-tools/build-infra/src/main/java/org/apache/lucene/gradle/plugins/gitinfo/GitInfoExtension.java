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

import org.gradle.api.file.FileSystemLocation;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.MapProperty;
import org.gradle.api.provider.Property;

public abstract class GitInfoExtension {
  public static final String NAME = "gitinfo";

  /**
   * @return Return a map of key-value pairs extracted from the current git status.
   */
  public abstract MapProperty<String, String> getGitInfo();

  /**
   * @return Return the location of {@code .git} directory (or file, if worktrees are used).
   */
  public abstract Property<FileSystemLocation> getDotGitDir();

  /**
   * @return Return a set of all versioned and non-versioned files (that are not ignored by {@code
   *     .gitignore}).
   */
  public abstract ListProperty<String> getAllNonIgnoredProjectFiles();
}
