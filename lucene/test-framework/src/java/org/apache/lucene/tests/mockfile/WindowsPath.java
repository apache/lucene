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
package org.apache.lucene.tests.mockfile;

import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;

class WindowsPath extends FilterPath {

  static HashSet<Character> RESERVED_CHARACTERS =
      new HashSet<>(Arrays.asList('<', '>', ':', '\"', '\\', '|', '?', '*'));

  static HashSet<String> RESERVED_NAMES =
      new HashSet<>(
          Arrays.asList(
              "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7",
              "COM8", "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8",
              "LPT9"));

  WindowsPath(Path path, FilterFileSystem fileSystem) {
    super(path, fileSystem);
  }

  @Override
  public Path resolve(Path other) {
    checkInvalidPath(other);
    return wrap(delegate.resolve(toDelegate(other)));
  }

  private void checkInvalidPath(Path other) {
    String fileName = other.getFileName().toString();

    if (RESERVED_NAMES.contains(fileName)) {
      throw new InvalidPathException(
          fileName, "File name: " + fileName + " is one of the reserved file names");
    }

    for (int i = 0; i < fileName.length(); i++) {
      if (RESERVED_CHARACTERS.contains(fileName.charAt(i))) {
        throw new InvalidPathException(
            fileName,
            "File name: " + fileName + " contains a reserved character: " + fileName.charAt(i));
      }
    }
  }
}
