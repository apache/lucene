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

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * A simpler version of <a
 * href="https://github.com/gradle/gradle/blob/master/platforms/core-runtime/wrapper-shared/src/main/java/org/gradle/wrapper/">Gradle's
 * real wrapper</a>. This one doesn't use {@code org.gradle.cli} or other Gradle-internal class,
 * skipping property-merging/CLI-flag handling (no {@code -g}/{@code -q} support here).
 */
public class GradleWrapper {

  public static void main(String[] args) throws Exception {
    Path propertiesFile = findWrapperProperties(Path.of("").toAbsolutePath());
    Properties props = new Properties();
    try (InputStream in = Files.newInputStream(propertiesFile)) {
      props.load(in);
    }

    Path gradleHome = ensureDistribution(props, resolveGradleUserHome());
    System.exit(launchGradle(gradleHome, args));
  }

  private static Path findWrapperProperties(Path start) {
    for (Path dir = start; dir != null; dir = dir.getParent()) {
      Path candidate =
          dir.resolve("gradle").resolve("wrapper").resolve("gradle-wrapper.properties");
      if (Files.isRegularFile(candidate)) {
        return candidate;
      }
    }
    throw new IllegalStateException(
        "Could not locate gradle/wrapper/gradle-wrapper.properties above " + start);
  }

  private static Path resolveGradleUserHome() {
    String sysProp = System.getProperty("gradle.user.home");
    if (sysProp != null && !sysProp.isEmpty()) {
      return Path.of(sysProp);
    }
    String env = System.getenv("GRADLE_USER_HOME");
    if (env != null && !env.isEmpty()) {
      return Path.of(env);
    }
    return Path.of(System.getProperty("user.home"), ".gradle");
  }

  private static Path ensureDistribution(Properties props, Path gradleUserHome) throws Exception {
    String urlStr = require(props, "distributionUrl");
    URI uri = URI.create(urlStr);
    String zipName = Path.of(uri.getPath()).getFileName().toString();
    String baseName =
        zipName.endsWith(".zip") ? zipName.substring(0, zipName.length() - 4) : zipName;

    // Doesn't need to match Gradle's own MD5-based cache hash scheme (PathAssembler) -- this is
    // an independent cache, keyed only well enough to dedupe by exact distributionUrl.
    String hash = sha256Hex(urlStr).substring(0, 16);
    Path installRoot =
        gradleUserHome.resolve("wrapper").resolve("dists").resolve(baseName).resolve(hash);
    Path marker = installRoot.resolve(".installed");
    if (Files.exists(marker)) {
      return findExtractedHome(installRoot);
    }

    Files.createDirectories(installRoot);
    Path zipFile = Files.createTempFile(installRoot, "download-", ".zip");
    try {
      download(uri, zipFile, parseIntOr(props.getProperty("networkTimeout"), 10_000));

      String expectedSha256 = props.getProperty("distributionSha256Sum");
      if (expectedSha256 != null) {
        String actual = sha256HexOfFile(zipFile);
        if (!actual.equalsIgnoreCase(expectedSha256)) {
          throw new IOException(
              "Checksum mismatch for "
                  + zipName
                  + ": expected "
                  + expectedSha256
                  + ", got "
                  + actual);
        }
      }

      extractZip(zipFile, installRoot);
      Files.writeString(marker, "ok");
    } finally {
      Files.deleteIfExists(zipFile);
    }
    return findExtractedHome(installRoot);
  }

  private static Path findExtractedHome(Path installRoot) throws IOException {
    try (var stream = Files.list(installRoot)) {
      return stream
          .filter(Files::isDirectory)
          .findFirst()
          .orElseThrow(
              () -> new IOException("No extracted distribution found under " + installRoot));
    }
  }

  private static void download(URI uri, Path dest, int timeoutMs)
      throws IOException, InterruptedException {
    HttpClient client =
        HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NORMAL)
            .connectTimeout(Duration.ofMillis(timeoutMs))
            .build();
    HttpRequest request = HttpRequest.newBuilder(uri).GET().build();
    HttpResponse<Path> response = client.send(request, HttpResponse.BodyHandlers.ofFile(dest));
    if (response.statusCode() != 200) {
      throw new IOException("Download failed: HTTP " + response.statusCode() + " for " + uri);
    }
  }

  private static void extractZip(Path zipFile, Path destDir) throws IOException {
    try (ZipInputStream zis = new ZipInputStream(Files.newInputStream(zipFile))) {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        Path target = destDir.resolve(entry.getName()).normalize();
        if (!target.startsWith(destDir)) {
          throw new IOException("Zip entry outside target directory: " + entry.getName());
        }
        if (entry.isDirectory()) {
          Files.createDirectories(target);
        } else {
          Files.createDirectories(target.getParent());
          Files.copy(zis, target, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        }
      }
    }
  }

  private static int launchGradle(Path gradleHome, String[] args) throws Exception {
    Path launcherJar = findLauncherJar(gradleHome);
    URL[] classpath = {launcherJar.toUri().toURL()};
    try (URLClassLoader loader =
        new URLClassLoader(classpath, ClassLoader.getSystemClassLoader())) {
      Thread.currentThread().setContextClassLoader(loader);
      Class<?> gradleMain = Class.forName("org.gradle.launcher.GradleMain", true, loader);
      Method main = gradleMain.getMethod("main", String[].class);
      try {
        main.invoke(null, (Object) args);
        // GradleMain calls System.exit() itself on failure; a normal return means success.
        return 0;
      } catch (InvocationTargetException e) {
        Throwable cause = e.getCause() != null ? e.getCause() : e;
        cause.printStackTrace();
        return 1;
      }
    }
  }

  private static Path findLauncherJar(Path gradleHome) throws IOException {
    Path lib = gradleHome.resolve("lib");
    try (var stream = Files.list(lib)) {
      return stream
          .filter(p -> p.getFileName().toString().matches("gradle-launcher-.*\\.jar"))
          .findFirst()
          .orElseThrow(() -> new IOException("Could not find gradle-launcher-*.jar under " + lib));
    }
  }

  private static String require(Properties props, String key) {
    String value = props.getProperty(key);
    if (value == null) {
      throw new IllegalStateException("Missing required property: " + key);
    }
    return value;
  }

  private static int parseIntOr(String value, int fallback) {
    if (value == null) {
      return fallback;
    }
    try {
      return Integer.parseInt(value.trim());
    } catch (NumberFormatException e) {
      return fallback;
    }
  }

  private static String sha256Hex(String s) throws Exception {
    return toHex(MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8)));
  }

  private static String sha256HexOfFile(Path path) throws Exception {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    try (InputStream in = Files.newInputStream(path)) {
      byte[] buf = new byte[8192];
      int n;
      while ((n = in.read(buf)) != -1) {
        digest.update(buf, 0, n);
      }
    }
    return toHex(digest.digest());
  }

  private static String toHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }
}
