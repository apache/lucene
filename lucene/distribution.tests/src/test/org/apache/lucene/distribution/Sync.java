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
package org.apache.lucene.distribution;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

final class Sync {
  private static class Entry {
    String name;
    Path path;

    public Entry(Path path) {
      this.path = path;
      this.name = path.getFileName().toString();
    }
  }

  public void sync(Path source, Path target) throws IOException {
    List<Entry> sourceEntries = files(source);
    List<Entry> targetEntries = files(target);

    for (Entry src : sourceEntries) {
      Path dst = target.resolve(src.name);
      if (Files.isDirectory(src.path)) {
        Files.createDirectories(dst);
        sync(src.path, dst);
      } else {
        if (!Files.exists(dst)
            || Files.size(dst) != Files.size(src.path)
            || Files.getLastModifiedTime(dst).compareTo(Files.getLastModifiedTime(src.path)) != 0) {
          Files.copy(
              src.path,
              dst,
              StandardCopyOption.COPY_ATTRIBUTES,
              StandardCopyOption.REPLACE_EXISTING);
        }
      }
    }

    Set<String> atSource = sourceEntries.stream().map(e -> e.name).collect(Collectors.toSet());
    targetEntries.stream().filter(v -> !atSource.contains(v.name)).forEach(e -> remove(e.path));
  }

  private List<Entry> files(Path source) throws IOException {
    ArrayList<Entry> entries = new ArrayList<>();
    try (DirectoryStream<Path> ds = Files.newDirectoryStream(source)) {
      ds.forEach(p -> entries.add(new Entry(p)));
    }
    return entries;
  }

  private static void remove(Path p) {
    try {
      Files.walkFileTree(
          p,
          new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                throws IOException {
              Files.delete(dir);
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                throws IOException {
              Files.delete(file);
              return FileVisitResult.CONTINUE;
            }
          });
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
