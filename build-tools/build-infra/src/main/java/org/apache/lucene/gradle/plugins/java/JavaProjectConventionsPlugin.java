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
package org.apache.lucene.gradle.plugins.java;

import java.util.stream.Stream;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.apache.lucene.gradle.plugins.spotless.GoogleJavaFormatPlugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaLibraryPlugin;

/** Applies java conventions to all projects that seem to be java projects. */
public class JavaProjectConventionsPlugin extends LuceneGradlePlugin {
  @Override
  public void apply(Project project) {
    applicableToRootProjectOnly(project);

    project.allprojects(
        p -> {
          if (Stream.of("src/java", "src/test", "src/tools")
              .anyMatch(srcDir -> p.file(srcDir).exists())) {
            applyJavaPlugins(p);
          }
        });
  }

  private void applyJavaPlugins(Project project) {
    var plugins = project.getPlugins();
    plugins.apply(JavaLibraryPlugin.class);

    plugins.apply(AlternativeJdkSupportPlugin.class);
    plugins.apply(JavaFolderLayoutPlugin.class);
    plugins.apply(JavacConfigurationPlugin.class);
    plugins.apply(JarManifestConfigurationPlugin.class);
    plugins.apply(TestsAndRandomizationPlugin.class);
    plugins.apply(TestsBeastingPlugin.class);
    plugins.apply(TestsSummaryPlugin.class);
    plugins.apply(ApplyForbiddenApisPlugin.class);
    plugins.apply(EcjLintPlugin.class);
    plugins.apply(GoogleJavaFormatPlugin.class);
    plugins.apply(CodeProfilingPlugin.class);
    plugins.apply(FailOnNoMatchingFilteredTestsPlugin.class);
    plugins.apply(CodeCoveragePlugin.class);
  }
}
