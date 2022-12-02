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
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.EnumSet;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.APPEND;

/**
 * Standalone class that can be used to download a gradle-wrapper.jar
 * <p>
 * Has no dependencies outside of standard java libraries
 */
public class WrapperDownloader {
  public static void main(String[] args) {
    if (args.length != 1) {
      System.err.println("Usage: java WrapperDownloader.java <destination>");
      System.exit(1);
    }

    try {
      new WrapperDownloader().run(Paths.get(args[0]));
    } catch (Exception e) {
      System.err.println("ERROR: " + e.getMessage());
      System.exit(3);
    }
  }

  public static void checkVersion() {
    int major = Runtime.getRuntime().version().feature();
    if (major < 11 || major > 19) {
      throw new IllegalStateException("java version must be between 11 and 19, your version: " + major);
    }
  }

  public void run(Path destination) throws IOException, NoSuchAlgorithmException {
    Path checksumPath = destination.resolveSibling(destination.getFileName().toString() + ".sha256");
    if (!Files.exists(checksumPath)) {
      throw new IOException("Checksum file not found: " + checksumPath);
    }
    String expectedChecksum = Files.readString(checksumPath, StandardCharsets.UTF_8).trim();

    Path versionPath = destination.resolveSibling(destination.getFileName().toString() + ".version");
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

    URL url = new URL("https://raw.githubusercontent.com/gradle/gradle/v" + wrapperVersion + "/gradle/wrapper/gradle-wrapper.jar");
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
            System.err.println("Error connecting to server: " + e + ", will retry in " + retryDelay + " seconds.");
            Thread.sleep(TimeUnit.SECONDS.toMillis(retryDelay));
            continue;
          }
        }

        switch (connection.getResponseCode()) {
          case HttpURLConnection.HTTP_INTERNAL_ERROR:
          case HttpURLConnection.HTTP_UNAVAILABLE:
          case HttpURLConnection.HTTP_BAD_GATEWAY:
            if (retries-- > 0) {
              // Retry after a short delay.
              System.err.println("Server returned HTTP " + connection.getResponseCode() + ", will retry in " + retryDelay + " seconds.");
              Thread.sleep(TimeUnit.SECONDS.toMillis(retryDelay));
              continue;
            }
        }

        // fall through, let getInputStream() throw IOException if there's a failure.
        break;
      }

      try (InputStream is = connection.getInputStream();
           OutputStream out = Files.newOutputStream(temp)){
        is.transferTo(out);
      }

      String checksum = checksum(digest, temp);
      if (!checksum.equalsIgnoreCase(expectedChecksum)) {
        throw new IOException(String.format(Locale.ROOT,
                "Checksum mismatch on downloaded gradle-wrapper.jar (was: %s, expected: %s).",
                checksum,
                expectedChecksum));
      }

      Files.move(temp, destination, REPLACE_EXISTING);
      temp = null;
    } catch (IOException | InterruptedException e) {
      throw new IOException("Could not download gradle-wrapper.jar (" +
          e.getClass().getSimpleName() + ": " + e.getMessage() + ").");
    } finally {
      if (temp != null) {
        Files.deleteIfExists(temp);
      }
    }
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
      throw new IOException("Could not compute digest of file: " + path + " (" + e.getMessage() + ")");
    }
  }
}
