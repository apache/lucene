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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.codec.digest.DigestUtils;
import org.gradle.api.DefaultTask;
import org.gradle.api.artifacts.ArtifactCollection;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.component.ProjectComponentIdentifier;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;
import org.gradle.internal.component.external.model.ModuleComponentArtifactIdentifier;

public abstract class ComputeJarInfos extends DefaultTask {
  /**
   * We have to use {@link ArtifactCollection} instead of {@link
   * org.gradle.api.artifacts.result.ResolvedArtifactResult} here as we're running into an issue in
   * Gradle: <a href="https://github.com/gradle/gradle/issues/27582">#27582</a>.
   *
   * @see #include
   */
  @Internal
  public abstract ListProperty<ArtifactCollection> getInspectedConfigurations();

  @OutputFile
  public abstract RegularFileProperty getOutputFile();

  public ComputeJarInfos() {
    // register input configurations as inputs.
    getInputs()
        .files(
            getInspectedConfigurations()
                .map(values -> values.stream().map(ArtifactCollection::getArtifactFiles).toList()));
  }

  public void include(Provider<Configuration> confProv) {
    Provider<ArtifactCollection> prov =
        confProv.map(
            it ->
                it.getIncoming()
                    .artifactView(
                        view -> {
                          view.lenient(false);
                          view.componentFilter(
                              identifier -> !(identifier instanceof ProjectComponentIdentifier));
                        })
                    .getArtifacts());
    getInspectedConfigurations().add(prov);
  }

  @TaskAction
  void collectInfos() {
    var digestUtils = new DigestUtils(DigestUtils.getSha1Digest());
    Function<File, String> checksum =
        file -> {
          try {
            return digestUtils.digestAsHex(file).trim();
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        };

    var jarInfos =
        getInspectedConfigurations().get().stream()
            .flatMap(
                col -> {
                  return col.getArtifacts().stream()
                      .filter(v -> v.getId() instanceof ModuleComponentArtifactIdentifier)
                      .map(
                          artifact -> {
                            var id = (ModuleComponentArtifactIdentifier) artifact.getId();
                            return new JarInfo(
                                id.getComponentIdentifier().getDisplayName(),
                                id.getComponentIdentifier().getModuleIdentifier().getName(),
                                artifact.getFile().getName(),
                                checksum.apply(artifact.getFile()));
                          });
                })
            .collect(Collectors.toCollection(TreeSet::new));

    JarInfo.serialize(getOutputFile().get().getAsFile().toPath(), new ArrayList<>(jarInfos));
  }
}
