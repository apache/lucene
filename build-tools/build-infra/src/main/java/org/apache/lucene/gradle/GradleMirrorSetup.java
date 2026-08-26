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
package org.apache.lucene.gradle;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLConnection;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Standalone class run by {@code gradlew}/{@code gradlew.bat} (before the gradle wrapper) when the
 * {@value #LUCENE_GRADLE_DISTRIBUTION_URL_ENV} and/or {@value #LUCENE_GRADLE_WRAPPER_URL_ENV}
 * environment variables are set. This should be used in environments that cannot reach GitHub
 * and/or services.gradle.org.
 *
 * <p>This class downloads (each step only if the corresponding variable is set):
 *
 * <ul>
 *   <li>{@code gradle-wrapper.jar} from {@value #LUCENE_GRADLE_WRAPPER_URL_ENV} into {@code
 *       gradle/wrapper/} (otherwise GradleWrapperDownloader would fetch it from GitHub),
 *   <li>the gradle distribution from {@value #LUCENE_GRADLE_DISTRIBUTION_URL_ENV}, installing it in
 *       the gradle user home exactly where the gradle wrapper would install the "official"
 *       distribution so that the wrapper considers it already installed.
 * </ul>
 *
 * <p>Both URLs may contain a {@code ${gradleVersion}} placeholder, replaced with the version parsed
 * from {@code distributionUrl} in {@code gradle-wrapper.properties}. For example:
 *
 * <pre>
 * LUCENE_GRADLE_DISTRIBUTION_URL=file:///tmp/local-gradle-mirror/${gradleVersion}/gradle-${gradleVersion}-bin.zip
 * LUCENE_GRADLE_WRAPPER_URL=file:///tmp/local-gradle-mirror/${gradleVersion}/gradle-wrapper.jar
 * </pre>
 *
 * <p>By default, checksums of the distribution and/or the wrapper must match those that are present
 * in {@code gradle-wrapper.properties}. Setting {@value #LUCENE_GRADLE_VERIFY_CHECKSUMS_ENV} to
 * {@code false} disables all checksum verification: an existing {@code gradle-wrapper.jar} is used
 * as-is and downloads are installed unverified.
 *
 * <p>This class must not have any dependencies outside of standard java libraries as it's run
 * directly from source.
 *
 * <p>The code in this class is based on automated analysis of gradle wrapper sources from gradle
 * v9.7.1.
 */
public class GradleMirrorSetup {
  public static final String LUCENE_GRADLE_DISTRIBUTION_URL_ENV = "LUCENE_GRADLE_DISTRIBUTION_URL";
  public static final String LUCENE_GRADLE_WRAPPER_URL_ENV = "LUCENE_GRADLE_WRAPPER_URL";
  public static final String LUCENE_GRADLE_VERIFY_CHECKSUMS_ENV = "LUCENE_GRADLE_VERIFY_CHECKSUMS";

  /** Inline this annotation to keep this class self-contained. */
  @Retention(RetentionPolicy.CLASS)
  @Target({ElementType.CONSTRUCTOR, ElementType.FIELD, ElementType.METHOD, ElementType.TYPE})
  private @interface SuppressForbidden {
    String reason();
  }

  /**
   * Checksum verification is on unless {@value #LUCENE_GRADLE_VERIFY_CHECKSUMS_ENV} is set to
   * "false".
   */
  private static final boolean VERIFY_CHECKSUMS =
      !"false".equalsIgnoreCase(System.getenv(LUCENE_GRADLE_VERIFY_CHECKSUMS_ENV));

  /** Used when {@code networkTimeout} is not set in {@code gradle-wrapper.properties}. */
  private static final int DEFAULT_NETWORK_TIMEOUT_MILLIS = (int) TimeUnit.SECONDS.toMillis(10);

  private static final Pattern DISTRIBUTION_NAME =
      Pattern.compile("gradle-(?<version>.+?)-(bin|all)\\.zip");
  private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{(?<varname>[^}]*)\\}");
  private static final boolean IS_WINDOWS =
      System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("windows");

  private final Path projectDir;
  private final Path gradleUserHome;

  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("Usage: java GradleMirrorSetup.java <project dir> [gradle arguments]");
      System.exit(2);
    }

    try {
      checkVersion();
      new GradleMirrorSetup(Paths.get(args[0]), args).run();
    } catch (Exception e) {
      System.err.println("ERROR: " + e.getMessage());
      System.exit(3);
    }
  }

  public static void checkVersion() {
    int major = Runtime.version().feature();
    if (major < 17) {
      throw new IllegalStateException("java version must be 17 or later, your version: " + major);
    }
  }

  GradleMirrorSetup(Path projectDir, String[] gradleArgs) {
    this.projectDir = projectDir.toAbsolutePath().normalize();

    // Gradle user home: -g/--gradle-user-home, GRADLE_USER_HOME or ~/.gradle.
    String userHome = null;
    for (int i = 1; i < gradleArgs.length; i++) {
      String arg = gradleArgs[i];
      if ((arg.equals("-g") || arg.equals("--gradle-user-home")) && i + 1 < gradleArgs.length) {
        userHome = gradleArgs[++i];
      } else if (arg.startsWith("--gradle-user-home=")) {
        userHome = arg.substring("--gradle-user-home=".length());
      }
    }
    if (userHome == null) {
      userHome = System.getenv("GRADLE_USER_HOME");
    }
    this.gradleUserHome =
        userHome != null
            ? Paths.get(userHome)
            : Paths.get(System.getProperty("user.home"), ".gradle");
  }

  void run() throws Exception {
    Path wrapperDir = projectDir.resolve("gradle").resolve("wrapper");
    Path propertiesFile = wrapperDir.resolve("gradle-wrapper.properties");
    if (!Files.exists(propertiesFile)) {
      throw new IOException("Wrapper property file not found: " + propertiesFile);
    }
    Properties props = new Properties();
    try (Reader reader = Files.newBufferedReader(propertiesFile, StandardCharsets.UTF_8)) {
      props.load(reader);
    }
    String distributionUrl = props.getProperty("distributionUrl");
    if (distributionUrl == null) {
      throw new IOException("No 'distributionUrl' in " + propertiesFile);
    }
    Matcher m = DISTRIBUTION_NAME.matcher(distributionUrl);
    if (!m.find()) {
      throw new IOException(
          "Could not parse the gradle version from distributionUrl in "
              + propertiesFile
              + ": "
              + distributionUrl);
    }
    String gradleVersion = m.group("version");
    int timeout =
        Integer.parseInt(
            props
                .getProperty("networkTimeout", String.valueOf(DEFAULT_NETWORK_TIMEOUT_MILLIS))
                .trim());

    String wrapperUrl = expand(System.getenv(LUCENE_GRADLE_WRAPPER_URL_ENV), gradleVersion);
    String mirrorDistributionUrl =
        expand(System.getenv(LUCENE_GRADLE_DISTRIBUTION_URL_ENV), gradleVersion);

    if (!VERIFY_CHECKSUMS) {
      log(
          "NOTE: checksum verification disabled ("
              + LUCENE_GRADLE_VERIFY_CHECKSUMS_ENV
              + "=false).");
    }

    if (wrapperUrl != null) {
      setupWrapperJar(wrapperDir, wrapperUrl, timeout);
    }
    if (mirrorDistributionUrl != null) {
      setupDistribution(props, distributionUrl, mirrorDistributionUrl, gradleVersion, timeout);
    }
  }

  /** Replaces {@code ${gradleVersion}}; null/blank input yields null. */
  private static String expand(String template, String gradleVersion) throws IOException {
    if (template == null || template.trim().isEmpty()) {
      return null;
    }
    Matcher m = PLACEHOLDER.matcher(template.trim());
    StringBuilder sb = new StringBuilder();
    while (m.find()) {
      if (!m.group("varname").equals("gradleVersion")) {
        throw new IOException(
            "Unknown placeholder "
                + m.group()
                + " (only ${gradleVersion} is supported) in: "
                + template);
      }
      m.appendReplacement(sb, Matcher.quoteReplacement(gradleVersion));
    }
    return m.appendTail(sb).toString();
  }

  /**
   * Makes sure gradle/wrapper/gradle-wrapper.jar is present and matches gradle-wrapper.jar.sha256,
   * downloading it from the mirror if needed. With checksum verification disabled, any existing jar
   * is used as-is and downloads are not verified.
   */
  private void setupWrapperJar(Path wrapperDir, String wrapperUrl, int timeout) throws Exception {
    Path jar = wrapperDir.resolve("gradle-wrapper.jar");
    Path checksumFile = wrapperDir.resolve("gradle-wrapper.jar.sha256");
    String expectedChecksum = null;
    if (VERIFY_CHECKSUMS) {
      for (String line : Files.readAllLines(checksumFile, StandardCharsets.UTF_8)) {
        // sha256sum format: "<checksum> *gradle-wrapper.jar" ('*' marks binary mode).
        String[] parts = line.trim().split("\\s+");
        if (parts.length == 2 && parts[1].replaceFirst("^\\*", "").equals("gradle-wrapper.jar")) {
          expectedChecksum = parts[0];
        }
      }
      if (expectedChecksum == null) {
        throw new IOException("No checksum for gradle-wrapper.jar in " + checksumFile);
      }
    }

    if (Files.exists(jar)
        && (!VERIFY_CHECKSUMS || sha256(jar).equalsIgnoreCase(expectedChecksum))) {
      return;
    }

    log("Downloading gradle-wrapper.jar from " + wrapperUrl);
    Path temp = Files.createTempFile(wrapperDir, ".gradle-wrapper", ".tmp");
    try {
      download(new URI(wrapperUrl), temp, timeout);
      if (VERIFY_CHECKSUMS) {
        String actualChecksum = sha256(temp);
        if (!actualChecksum.equalsIgnoreCase(expectedChecksum)) {
          throw new IOException(
              "The gradle-wrapper.jar downloaded from "
                  + wrapperUrl
                  + " does not match "
                  + checksumFile
                  + " (expected: "
                  + expectedChecksum
                  + ", actual: "
                  + actualChecksum
                  + ").");
        }
      }
      Files.move(temp, jar, StandardCopyOption.REPLACE_EXISTING);
    } finally {
      Files.deleteIfExists(temp);
    }
  }

  /**
   * Downloads the distribution from the mirror and installs it where the gradle wrapper would
   * install the official one: {@code <base>/<path>/<name>/<md5 of the official url as base36>/}.
   */
  private void setupDistribution(
      Properties props, String distributionUrl, String mirrorUrl, String gradleVersion, int timeout)
      throws Exception {
    String expectedSha256 = props.getProperty("distributionSha256Sum");
    if (expectedSha256 == null || expectedSha256.trim().isEmpty()) {
      if (VERIFY_CHECKSUMS) {
        throw new IOException(
            "No 'distributionSha256Sum' in gradle-wrapper.properties; refusing to install a gradle"
                + " distribution without checksum verification.");
      }
      expectedSha256 = null;
    } else {
      expectedSha256 = expectedSha256.trim();
    }

    URI official = new URI(distributionUrl);
    String zipName = official.getPath().replaceAll(".*/", "");
    String distName = zipName.replaceAll("\\.[^.]*$", "");
    String hash =
        new BigInteger(
                1,
                MessageDigest.getInstance("MD5")
                    .digest(withoutUserInfo(official).getBytes(StandardCharsets.UTF_8)))
            .toString(36);
    Path distDir =
        baseDir(props.getProperty("distributionBase", "GRADLE_USER_HOME"))
            .resolve(props.getProperty("distributionPath", "wrapper/dists"))
            .resolve(distName)
            .resolve(hash);
    Path zip =
        baseDir(props.getProperty("zipStoreBase", "GRADLE_USER_HOME"))
            .resolve(props.getProperty("zipStorePath", "wrapper/dists"))
            .resolve(distName)
            .resolve(hash)
            .resolve(zipName);
    Path marker = zip.resolveSibling(zipName + ".ok");
    Path lockFile = zip.resolveSibling(zipName + ".lck");
    Files.createDirectories(zip.getParent());

    // The same lock file the gradle wrapper uses, in case several builds start concurrently.
    long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(2);
    while (true) {
      try (FileChannel channel =
          FileChannel.open(lockFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
        FileLock lock = channel.tryLock();
        if (lock != null) {
          try {
            if (Files.exists(marker) && findLauncherJar(distDir) != null) {
              return;
            }

            log("Downloading gradle distribution from " + mirrorUrl);
            download(new URI(mirrorUrl), zip, timeout);
            if (VERIFY_CHECKSUMS) {
              String actual = sha256(zip);
              if (!actual.equalsIgnoreCase(expectedSha256)) {
                Files.delete(zip);
                throw new IOException(
                    "The distribution downloaded from "
                        + mirrorUrl
                        + " does not match distributionSha256Sum in gradle-wrapper.properties"
                        + " (expected: "
                        + expectedSha256
                        + ", actual: "
                        + actual
                        + ").");
              }
            }

            // Remove leftovers of previous installations, then unpack.
            if (Files.isDirectory(distDir)) {
              try (Stream<Path> s = Files.list(distDir)) {
                for (Path p : s.filter(Files::isDirectory).collect(Collectors.toList())) {
                  deleteRecursively(p);
                }
              }
            }
            unzip(zip, distDir);
            Path launcherJar = findLauncherJar(distDir);
            if (launcherJar == null) {
              throw new IOException(
                  "The archive downloaded from " + mirrorUrl + " is not a gradle distribution?");
            }
            Path gradleCommand = launcherJar.getParent().resolveSibling("bin").resolve("gradle");
            if (!IS_WINDOWS && Files.exists(gradleCommand)) {
              try {
                Files.setPosixFilePermissions(
                    gradleCommand, PosixFilePermissions.fromString("rwxr-xr-x"));
              } catch (IOException | UnsupportedOperationException e) {
                log("Could not set executable permissions for: " + gradleCommand + " (" + e + ")");
              }
            }
            if (!Files.exists(marker)) {
              Files.createFile(marker);
            }
            Files.delete(zip);
            log("Gradle " + gradleVersion + " installed in " + distDir);
            return;
          } finally {
            lock.release();
          }
        }
      }
      if (System.nanoTime() > deadline) {
        throw new IOException("Timed out waiting for the lock on " + lockFile);
      }
      sleep(500);
    }
  }

  private Path baseDir(String base) throws IOException {
    switch (base) {
      case "GRADLE_USER_HOME":
        return gradleUserHome;
      case "PROJECT":
        return projectDir;
      default:
        throw new IOException("Unknown base directory in gradle-wrapper.properties: " + base);
    }
  }

  /**
   * The gradle wrapper hashes the distribution URL with any user info stripped so we recreate such
   * an url here.
   */
  private static String withoutUserInfo(URI uri) throws URISyntaxException {
    return new URI(
            uri.getScheme(),
            null,
            uri.getHost(),
            uri.getPort(),
            uri.getPath(),
            uri.getQuery(),
            uri.getFragment())
        .toASCIIString();
  }

  /** Downloads to a temporary ".part" file first; retries a few times on failures. */
  private void download(URI uri, Path target, int timeout) throws Exception {
    Path part = target.resolveSibling(target.getFileName() + ".part");
    for (int attempt = 1; ; attempt++) {
      try {
        // Proxies are configured using the standard HTTP(S) proxy system properties.
        URLConnection conn = uri.toURL().openConnection();
        conn.setConnectTimeout(timeout);
        conn.setReadTimeout(timeout);
        if (conn instanceof HttpURLConnection
            && ((HttpURLConnection) conn).getResponseCode() != HttpURLConnection.HTTP_OK) {
          throw new IOException(
              "Server returned HTTP " + ((HttpURLConnection) conn).getResponseCode());
        }
        try (InputStream in = conn.getInputStream();
            OutputStream out = Files.newOutputStream(part)) {
          in.transferTo(out);
        }
        Files.move(part, target, StandardCopyOption.REPLACE_EXISTING);
        return;
      } catch (IOException e) {
        Files.deleteIfExists(part);
        if (attempt >= 3 || !uri.getScheme().startsWith("http")) {
          throw new IOException("Could not download " + uri + ": " + e.getMessage(), e);
        }
        log("Could not download " + uri + " (" + e.getMessage() + "), will retry in 10 seconds.");
        sleep(TimeUnit.SECONDS.toMillis(10));
      }
    }
  }

  private static String sha256(Path path) throws Exception {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    try (InputStream in = Files.newInputStream(path)) {
      byte[] buffer = new byte[64 * 1024];
      for (int n; (n = in.read(buffer)) != -1; ) {
        digest.update(buffer, 0, n);
      }
    }
    return String.format(Locale.ROOT, "%064x", new BigInteger(1, digest.digest()));
  }

  private static void unzip(Path zip, Path dest) throws IOException {
    try (FileSystem zipFs = FileSystems.newFileSystem(zip, (ClassLoader) null)) {
      for (Path root : zipFs.getRootDirectories()) {
        try (Stream<Path> entries = Files.walk(root)) {
          for (Path entry : entries.collect(Collectors.toList())) {
            String name = root.relativize(entry).toString();
            // this corresponds to checks from PathTraversalChecker
            if (name.startsWith("/")
                || name.startsWith("\\")
                || (IS_WINDOWS && name.contains(":"))
                || Stream.of(name.replace('\\', '/').split("/")).anyMatch(".."::equals)) {
              throw new IOException("Unsafe zip entry: " + name);
            }
            Path target = dest.resolve(name);
            if (Files.isDirectory(entry)) {
              Files.createDirectories(target);
            } else {
              Files.copy(entry, target, StandardCopyOption.REPLACE_EXISTING);
            }
          }
        }
      }
    }
  }

  private static void deleteRecursively(Path dir) throws IOException {
    try (Stream<Path> s = Files.walk(dir)) {
      for (Path p : s.sorted(Comparator.reverseOrder()).collect(Collectors.toList())) {
        Files.delete(p);
      }
    }
  }

  /**
   * Returns lib/gradle-launcher-*.jar if distDir contains exactly one directory with exactly one
   * such jar (this is how the gradle wrapper validates an installation), null otherwise.
   */
  private static Path findLauncherJar(Path distDir) throws IOException {
    if (!Files.isDirectory(distDir)) {
      return null;
    }
    List<Path> dirs;
    try (Stream<Path> s = Files.list(distDir)) {
      dirs = s.filter(Files::isDirectory).collect(Collectors.toList());
    }
    if (dirs.size() != 1 || !Files.isDirectory(dirs.get(0).resolve("lib"))) {
      return null;
    }
    try (Stream<Path> s = Files.list(dirs.get(0).resolve("lib"))) {
      List<Path> jars =
          s.filter(p -> p.getFileName().toString().matches("gradle-launcher-.*\\.jar"))
              .collect(Collectors.toList());
      return jars.size() == 1 ? jars.get(0) : null;
    }
  }

  private static void log(String message) {
    System.err.println(message);
  }

  @SuppressForbidden(reason = "Valid use of thread.sleep.")
  private static void sleep(long millis) throws InterruptedException {
    Thread.sleep(millis);
  }
}
