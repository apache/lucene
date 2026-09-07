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

import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.internal.file.FileOperations;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;

/**
 * A helper task class that can collect and aggregate jar info files produced by {@link
 * ComputeJarInfos}.
 */
public abstract class CollectJarInfos extends DefaultTask {
  @InputFiles
  public abstract ConfigurableFileCollection getJarInfos();

  @Inject
  protected abstract FileOperations getFileOperations();

  @Internal
  protected List<JarInfo> getUniqueJarInfos() {
    return getJarInfos().getFiles().stream()
        .flatMap(it -> JarInfo.deserialize(it.toPath()).stream())
        .collect(Collectors.toSet())
        .stream()
        .sorted()
        .toList();
  }
}
