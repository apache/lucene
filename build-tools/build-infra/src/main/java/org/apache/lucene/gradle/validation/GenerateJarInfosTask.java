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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.codec.digest.DigestUtils;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.artifacts.ArtifactCollection;
import org.gradle.api.artifacts.component.ComponentArtifactIdentifier;
import org.gradle.api.artifacts.result.ResolvedArtifactResult;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;
import org.gradle.internal.component.external.model.ModuleComponentArtifactIdentifier;

/**
 * Collects dependency JAR information for a project and saves it in project.jarInfos and in an
 * output file. Each dependency has a map of attributes which make it easier to process it later on
 * (name, hash, module name)
 */
public abstract class GenerateJarInfosTask extends DefaultTask {

  /**
   * We have to use ArtifactCollection instead of ResolvedArtifactResult here as we're running into
   * an issue in Gradle: https://github.com/gradle/gradle/issues/27582
   */
  @Internal
  abstract ListProperty<ArtifactCollection> getRuntimeArtifacts();

  @InputFiles
  abstract ConfigurableFileCollection getDependencies();

  @OutputFile
  abstract RegularFileProperty getOutputFile();

  @TaskAction
  void generateJarInfos() throws IOException {
    var digestUtils = new DigestUtils(DigestUtils.getSha1Digest());
    Set<JarInfo> jarInfos = new LinkedHashSet<>();
    ArrayDeque<ResolvedArtifactResult> queue = new ArrayDeque<>();

    getRuntimeArtifacts()
        .get()
        .forEach(
            r -> {
              queue.addAll(r.getArtifacts());
            });

    Set<ComponentArtifactIdentifier> seenIds = new HashSet<>();
    Set<File> visited = new HashSet<>();
    while (queue.size() > 0) {
      ResolvedArtifactResult result = queue.removeFirst();
      ComponentArtifactIdentifier id = result.getId();
      seenIds.add(id);
      if (id instanceof ModuleComponentArtifactIdentifier
          && !((ModuleComponentArtifactIdentifier) id)
              .getComponentIdentifier()
              .getModule()
              .startsWith("org.apache.lucene")) {
        ModuleComponentArtifactIdentifier id2 = (ModuleComponentArtifactIdentifier) id;
        File file = result.getFile();
        if (visited.add(file)) {
          jarInfos.add(
              new JarInfo(
                  id2.getComponentIdentifier().getModule(),
                  file.toPath().getFileName().toString(),
                  id.getComponentIdentifier().getDisplayName(),
                  digestUtils.digestAsHex(file).trim()));
        }
      }
    }

    List<JarInfo> sorted =
        jarInfos.stream()
            .sorted(Comparator.comparing(JarInfo::getName))
            .collect(Collectors.toList());
    writeToFile(sorted);
    configureExtension(sorted);
  }

  @SuppressWarnings("all")
  private void configureExtension(List<JarInfo> sorted) {
    // TODO: remove ones we ported depentend tasks over to rely on report file.
    ((List<JarInfo>) getProject().getExtensions().getByName("jarInfos")).addAll(sorted);
  }

  private void writeToFile(Collection<JarInfo> jarInfos) throws IOException {
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    try (FileWriter writer = new FileWriter(getOutputFile().getAsFile().get())) {
      gson.toJson(jarInfos, writer);
    } catch (IOException e) {
      new GradleException("Unable to write to file", e);
    }
  }
}
