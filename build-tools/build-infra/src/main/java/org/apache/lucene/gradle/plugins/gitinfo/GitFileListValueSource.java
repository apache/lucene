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

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import javax.inject.Inject;
import org.gradle.api.GradleException;
import org.gradle.api.provider.ValueSource;
import org.gradle.process.ExecOperations;
import org.slf4j.LoggerFactory;

/** Returns a list of versioned and non-versioned (but not ignored) git files. */
public abstract class GitFileListValueSource
    implements ValueSource<List<String>, GitValueSourceParameters> {

  @Inject
  public abstract ExecOperations getExecOps();

  @Override
  public List<String> obtain() {
    try (var baos = new ByteArrayOutputStream();
        var out = new BufferedOutputStream(baos)) {
      var result =
          getExecOps()
              .exec(
                  spec -> {
                    spec.setStandardOutput(out);
                    spec.setErrorOutput(out);
                    spec.setIgnoreExitValue(true);

                    getParameters().configure(spec);

                    spec.setExecutable(getParameters().getGitExec().get());
                    spec.args(
                        "ls-files",
                        // show cached
                        "-c",
                        // show others
                        "-o",
                        // apply .gitignore exclusions
                        "--exclude-standard",
                        // don't quote paths, 0-terminate strings.
                        "-z");
                  });
      out.flush();

      // Assume the output is UTF-8, even if it has 0 eols.
      String gitOutput = baos.toString(StandardCharsets.UTF_8);

      if (result.getExitValue() != 0) {
        // Something went wrong. Assume this isn't a git checkout and
        // return an empty list of files.
        LoggerFactory.getLogger(getClass())
            .warn(
                "Failed executing git (exit status: {}). An empty list of versioned files will be used (run 'git init'?).",
                result.getExitValue());

        return List.of();
      }

      return List.of(gitOutput.split("\u0000"));
    } catch (IOException e) {
      throw new GradleException("Errors calling git to fetch local repository status.", e);
    }
  }
}
