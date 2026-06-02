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
package org.apache.lucene.gradle.plugins.misc;

import java.io.File;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Locale;
import org.apache.lucene.gradle.SuppressForbidden;
import org.gradle.api.GradleException;
import org.gradle.api.logging.LogLevel;
import org.gradle.api.logging.Logger;
import org.gradle.api.tasks.Exec;
import org.gradle.api.tasks.TaskAction;
import org.gradle.process.ExecResult;

/**
 * An {@link org.gradle.api.tasks.Exec} task that does not emit any output unless the executed
 * command fails.
 */
public abstract class QuietExec extends Exec {
  public QuietExec() {
    super.setIgnoreExitValue(true);
  }

  @TaskAction
  @Override
  protected void exec() {
    try {
      // TODO: maybe add an output stream that spills to disk only when necessary here?
      File outputFile = File.createTempFile("exec-output-", ".txt", getTemporaryDir());
      try (OutputStream os =
          new FilterOutputStream(Files.newOutputStream(outputFile.toPath())) {
            @Override
            public void close() {
              // no-op. we close this stream manually.
            }
          }) {
        super.setStandardInput(InputStream.nullInputStream());
        super.setStandardOutput(os);
        super.setErrorOutput(os);

        super.exec();
        os.flush();

        var result = super.getExecutionResult().get();
        processResult(result, outputFile, getExecutable(), getLogger());
      }
    } catch (IOException e) {
      throw new GradleException("QuietExec failed: " + e.getMessage(), e);
    }
  }

  public static void processResult(
      ExecResult result, File outputFile, String executable, Logger logger) throws IOException {
    boolean failed = result.getExitValue() != 0;
    boolean shouldLogOutput = failed || (logger.isInfoEnabled());
    if (shouldLogOutput) {
      String output = getOutputAsString(outputFile);
      logger.log(failed ? LogLevel.ERROR : LogLevel.INFO, output);
    }

    if (failed) {
      throw new GradleException(
          String.format(
              Locale.ROOT,
              "The executed process '%s' returned an unexpected status code: %d, full process output logged above.",
              executable,
              result.getExitValue()));
    }
  }

  @SuppressForbidden(reason = "Intentional default-codepage conversion.")
  private static String getOutputAsString(File outputFile) throws IOException {
    return new String(Files.readAllBytes(outputFile.toPath()));
  }

  @Override
  public Exec setStandardInput(InputStream inputStream) {
    throw propertyUnsupported("standardInput");
  }

  @Override
  public Exec setStandardOutput(OutputStream outputStream) {
    throw propertyUnsupported("standardOutput");
  }

  @Override
  public Exec setIgnoreExitValue(boolean ignoreExitValue) {
    throw propertyUnsupported("ignoreExitValue");
  }

  @Override
  public Exec setErrorOutput(OutputStream outputStream) {
    throw propertyUnsupported("errorOutput");
  }

  private GradleException propertyUnsupported(String propName) {
    throw new GradleException("Quiet exec does not support setting this property: " + propName);
  }
}
