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
package org.apache.lucene.gradle.buildinfra;

import java.nio.file.Path;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.lucene.gradle.Checksum;
import org.apache.lucene.gradle.ErrorReportingTestListener;
import org.apache.lucene.gradle.datasets.ExtractReuters;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.tasks.testing.TestDescriptor;
import org.gradle.api.tasks.testing.logging.TestLogging;

public class BuildInfraPlugin implements Plugin<Project> {
  @Override
  public void apply(Project project) {
    project.getExtensions().create(BuildInfraExtension.NAME, BuildInfraExtension.class);
  }

  public static class BuildInfraExtension {
    public static final String NAME = "buildinfra";

    public ErrorReportingTestListener newErrorReportingTestListener(
        TestLogging testLogging, Path spillDir, Path outputsDir, boolean verboseMode) {
      return new ErrorReportingTestListener(testLogging, spillDir, outputsDir, verboseMode);
    }

    public DigestUtils sha1Digest() {
      return new DigestUtils(DigestUtils.getSha1Digest());
    }

    public void extractReuters(String reutersDir, String outputDir) throws Exception {
      ExtractReuters.main(new String[] {reutersDir, outputDir});
    }

    public String getOutputLogName(TestDescriptor suite) {
      return ErrorReportingTestListener.getOutputLogName(suite);
    }

    public Class<?> checksumClass() {
      return Checksum.class;
    }
  }
}
