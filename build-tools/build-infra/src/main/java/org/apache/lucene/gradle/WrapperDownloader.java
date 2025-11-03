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

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Standalone class used to download the {@code gradle-wrapper.jar}.
 *
 * <p>Ensure this class has no dependencies outside of standard java libraries as it's used direct
 */
public class WrapperDownloader {
  public static void main(String[] args) {
    if (args.length != 1) {
      System.err.println("Usage: java WrapperDownloader.java <destination>");
      System.exit(2);
    }

    try {
      checkVersion();
      new WrapperDownloader().run(Paths.get(args[0]));
    } catch (Exception e) {
      System.err.println("ERROR: " + e.getMessage());
      System.exit(3);
    }
  }

  public static void checkVersion() {
    int major = Runtime.version().feature();
    if (major != 25) {
      throw new IllegalStateException("java version must be 25, your version: " + major);
    }
  }

  public void run(Path destination) throws IOException, NoSuchAlgorithmException {
    var expectedFileName = destination.getFileName().toString();
    Path checksumPath = destination.resolveSibling(expectedFileName + ".sha256");
    if (!Files.exists(checksumPath)) {
      throw new IOException("Checksum file not found: " + checksumPath);
    }

    String expectedChecksum;
    try (var lines = Files.lines(checksumPath, StandardCharsets.UTF_8)) {
      expectedChecksum =
          lines
              .map(
                  line -> {
                    // "The default mode is to print a line with: checksum, a space,
                    // a character indicating input mode  ('*' for binary, ' ' for text
                    // or where binary is insignificant), and name for each FILE."
                    var spaceIndex = line.indexOf(" ");
                    if (spaceIndex != -1 && spaceIndex + 2 < line.length()) {
                      var mode = line.charAt(spaceIndex + 1);
                      String fileName = line.substring(spaceIndex + 2);
                      if (mode == '*' && fileName.equals(expectedFileName)) {
                        return line.substring(0, spaceIndex);
                      }
                    }

                    Logger.getLogger(WrapperDownloader.class.getName())
                        .warning(
                            "Something is wrong with the checksum file. Regenerate with "
                                + "'sha256sum -b gradle-wrapper.jar > gradle-wrapper.jar.sha256'");
                    return null;
                  })
              .filter(Objects::nonNull)
              .findFirst()
              .orElse(null);

      if (expectedChecksum == null) {
        throw new IOException(
            "The checksum file did not contain the expected checksum for '"
                + expectedFileName
                + "'?");
      }
    }

    Path versionPath =
        destination.resolveSibling(destination.getFileName().toString() + ".version");
    if (!Files.exists(versionPath)) {
      throw new IOException("Wrapper version file not found: " + versionPath);
    }
    String wrapperVersion = Files.readString(versionPath, StandardCharsets.UTF_8).trim();

    MessageDigest digest = MessageDigest.getInstance("SHA-256");

    if (Files.exists(destination)) {
      if (checksum(digest, destination).equalsIgnoreCase(expectedChecksum)) {
        // File exists, checksum matches, good to go!
        return;
      } else {
        System.err.println("Checksum mismatch, will attempt to re-download gradle-wrapper.jar");
        System.out.println(destination);
        Files.delete(destination);
      }
    }

    URL url =
        URI.create(
                "https://raw.githubusercontent.com/gradle/gradle/v"
                    + wrapperVersion
                    + "/gradle/wrapper/gradle-wrapper.jar")
            .toURL();
    System.err.println("Downloading gradle-wrapper.jar from " + url);

    // Zero-copy save the jar to a temp file
    Path temp = Files.createTempFile(destination.getParent(), ".gradle-wrapper", ".tmp");
    try {
      int retries = 3;
      int retryDelay = 30;
      HttpURLConnection connection;
      while (true) {
        connection = (HttpURLConnection) url.openConnection();
        try {
          connection.connect();
        } catch (IOException e) {
          if (retries-- > 0) {
            // Retry after a short delay
            System.err.println(
                "Error connecting to server: " + e + ", will retry in " + retryDelay + " seconds.");
            sleep(TimeUnit.SECONDS.toMillis(retryDelay));
            continue;
          }
        }

        switch (connection.getResponseCode()) {
          case /* TOO_MANY_REQUESTS */ 429:
          // it may not be possible to recover from this using a short delay
          // but try anyway.
          case HttpURLConnection.HTTP_INTERNAL_ERROR:
          case HttpURLConnection.HTTP_UNAVAILABLE:
          case HttpURLConnection.HTTP_BAD_GATEWAY:
            if (retries-- > 0) {
              // Retry after a short delay.
              System.err.println(
                  "Server returned HTTP "
                      + connection.getResponseCode()
                      + ", will retry in "
                      + retryDelay
                      + " seconds.");
              sleep(TimeUnit.SECONDS.toMillis(retryDelay));
              continue;
            }
        }

        // fall through, let getInputStream() throw IOException if there's a failure.
        break;
      }

      try (InputStream is = connection.getInputStream();
          OutputStream out = Files.newOutputStream(temp)) {
        is.transferTo(out);
      }

      String checksum = checksum(digest, temp);
      if (!checksum.equalsIgnoreCase(expectedChecksum)) {
        throw new IOException(
            String.format(
                Locale.ROOT,
                "Checksum mismatch on downloaded gradle-wrapper.jar (was: %s, expected: %s).",
                checksum,
                expectedChecksum));
      }

      Files.move(temp, destination, REPLACE_EXISTING);
      temp = null;
    } catch (IOException | InterruptedException e) {
      throw new IOException(
          "Could not download gradle-wrapper.jar ("
              + e.getClass().getSimpleName()
              + ": "
              + e.getMessage()
              + ").");
    } finally {
      if (temp != null) {
        Files.deleteIfExists(temp);
      }
    }
  }

  private static void sleep(long millis) throws InterruptedException {
    Thread.sleep(millis);
  }

  private String checksum(MessageDigest messageDigest, Path path) throws IOException {
    try {
      char[] hex = "0123456789abcdef".toCharArray();
      byte[] digest = messageDigest.digest(Files.readAllBytes(path));
      StringBuilder sb = new StringBuilder();
      for (byte b : digest) {
        sb.append(hex[(b >> 4) & 0xf]).append(hex[b & 0xf]);
      }
      return sb.toString();
    } catch (IOException e) {
      throw new IOException(
          "Could not compute digest of file: " + path + " (" + e.getMessage() + ")");
    }
  }
}
