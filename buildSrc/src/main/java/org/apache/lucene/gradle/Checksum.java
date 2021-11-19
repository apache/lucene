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

/*
 * A task that generates SHA512 checksums. Cloned from:
 * https://github.com/gradle/gradle-checksum/blob/03351de/src/main/java/org/gradle/crypto/checksum/Checksum.java
 * with custom fixes to make the checksum palatable to shasum, see pending PR:
 * https://github.com/gradle/gradle-checksum/pull/4
 *
 * Original license ASL:
 * https://github.com/gradle/gradle-checksum/blob/03351de/LICENSE
 */

package org.apache.lucene.gradle;

import org.apache.commons.codec.digest.DigestUtils;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.FileType;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.TaskAction;
import org.gradle.work.Incremental;
import org.gradle.work.InputChanges;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Locale;

public class Checksum extends DefaultTask {
  private FileCollection files;
  private File outputDir;
  private Algorithm algorithm;

  public enum Algorithm {
    MD5(new DigestUtils(DigestUtils.getMd5Digest())),
    SHA256(new DigestUtils(DigestUtils.getSha256Digest())),
    SHA384(new DigestUtils(DigestUtils.getSha384Digest())),
    SHA512(new DigestUtils(DigestUtils.getSha512Digest()));

    private final DigestUtils hashFunction;

    Algorithm(DigestUtils hashFunction) {
      this.hashFunction = hashFunction;
    }

    public String getExtension() {
      return name().toLowerCase(Locale.ROOT);
    }
  }

  public Checksum() {
    outputDir = new File(getProject().getBuildDir(), "checksums");
    algorithm = Algorithm.SHA256;
  }

  @InputFiles
  @Incremental
  public FileCollection getFiles() {
    return files;
  }

  public void setFiles(FileCollection files) {
    this.files = files;
  }

  @Input
  public Algorithm getAlgorithm() {
    return algorithm;
  }

  public void setAlgorithm(Algorithm algorithm) {
    this.algorithm = algorithm;
  }

  @OutputDirectory
  public File getOutputDir() {
    return outputDir;
  }

  public void setOutputDir(File outputDir) {
    if (outputDir.exists() && !outputDir.isDirectory()) {
      throw new IllegalArgumentException("Output directory must be a directory.");
    }
    this.outputDir = outputDir;
  }

  @TaskAction
  public void generateChecksumFiles(InputChanges inputChanges) throws IOException {
    if (!getOutputDir().exists()) {
      if (!getOutputDir().mkdirs()) {
        throw new IOException("Could not create directory:" + getOutputDir());
      }
    }

    if (!inputChanges.isIncremental()) {
      getProject().delete(allPossibleChecksumFiles());
    }

    inputChanges
        .getFileChanges(getFiles())
        .forEach(
            fileChange -> {
              if (fileChange.getFileType() == FileType.DIRECTORY) {
                getProject()
                    .getLogger()
                    .warn("Checksums can't be applied to directories: " + fileChange.getFile());
                return;
              }

              File input = fileChange.getFile();
              switch (fileChange.getChangeType()) {
                case REMOVED:
                  if (input.isFile()) {
                    getProject().delete(outputFileFor(input));
                  }
                  break;

                case ADDED:
                case MODIFIED:
                  input = fileChange.getFile();
                  if (input.isFile()) {
                    File checksumFile = outputFileFor(input);

                    try {
                      String checksum = algorithm.hashFunction.digestAsHex(input).trim();

                      /*
                       * https://man7.org/linux/man-pages/man1/sha1sum.1.html
                       *
                       * The default mode is to print a line with checksum, a space, a character
                       * indicating input mode ('*' for binary, ' ' for text or where
                       * binary is insignificant), and name for each FILE.
                       */
                      boolean binaryMode = true;

                      Files.writeString(
                          checksumFile.toPath(),
                          String.format(
                              Locale.ROOT,
                              "%s %s%s",
                              checksum,
                              binaryMode ? "*" : " ",
                              input.getName()),
                          StandardCharsets.UTF_8);
                    } catch (IOException e) {
                      throw new GradleException("Trouble creating checksum: " + e.getMessage(), e);
                    }
                  }
                  break;
                default:
                  throw new RuntimeException();
              }
            });
  }

  private File outputFileFor(File inputFile) {
    return new File(getOutputDir(), inputFile.getName() + "." + algorithm.getExtension());
  }

  private FileCollection allPossibleChecksumFiles() {
    FileCollection possibleFiles = null;
    for (Algorithm algo : Algorithm.values()) {
      if (possibleFiles == null) {
        possibleFiles = filesFor(algo);
      } else {
        possibleFiles = possibleFiles.plus(filesFor(algo));
      }
    }
    return possibleFiles;
  }

  private FileCollection filesFor(final Algorithm algo) {
    return getProject()
        .fileTree(getOutputDir(), files -> files.include("**/*." + algo.toString().toLowerCase()));
  }
}
