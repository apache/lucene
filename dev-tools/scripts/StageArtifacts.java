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

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.Console;
import java.io.IOException;
import java.io.StringReader;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.PasswordAuthentication;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Sonatype nexus artifact staging/deployment script. This could be made
 * nicer, but this keeps it to JDK classes only.
 *
 * <p>The implementation is based on the REST API documentation of
 * <a href="https://oss.sonatype.org/nexus-staging-plugin/default/docs/index.html">nexus-staging-plugin</a>
 * and on anecdotal evidence and reverse-engineered information from around
 * the web... Weird that such a crucial piece of infrastructure has such obscure
 * documentation.
 */
public class StageArtifacts {
  private static final String DEFAULT_NEXUS_URI = "https://repository.apache.org";

  private static class Params {
    URI nexusUri = URI.create(DEFAULT_NEXUS_URI);
    String userName;
    char[] userPass;
    Path mavenDir;
    String description;

    private static char[] envVar(String envVar) {
      var value = System.getenv(envVar);
      return value == null ? null : value.toCharArray();
    }

    static void requiresArgument(String[] args, int at) {
      if (at + 1 >= args.length) {
        throw new RuntimeException("Option '" + args[at]
            + "' requires an argument, pass --help for help.");
      }
    }

    static Params parse(String[] args) {
      try {
        var params = new Params();
        for (int i = 0; i < args.length; i++) {
          switch (args[i]) {
            case "-n":
            case "--nexus":
              requiresArgument(args, i);
              params.nexusUri = URI.create(args[++i]);
              break;
            case "-u":
            case "--user":
              requiresArgument(args, i);
              params.userName = args[++i];
              break;
            case "-p":
            case "--password":
              requiresArgument(args, i);
              params.userPass = args[++i].toCharArray();
              break;
            case "--description":
              requiresArgument(args, i);
              params.description = args[++i];
              break;

            case "-h":
            case "--help":
              System.out.println("java " + StageArtifacts.class.getName() + " [options] path-to-maven-artifacts");
              System.out.println("  -u, --user  User name for authentication.");
              System.out.println("              better: ASF_USERNAME env. var.");
              System.out.println("  -p, --password  Password for authentication.");
              System.out.println("              better: ASF_PASSWORD env. var.");
              System.out.println("  -n, --nexus URL to Apache Nexus (optional).");
              System.out.println("  --description  Staging repo description (optional).");
              System.out.println("");
              System.out.println("  path        Path to maven artifact directory.");
              System.out.println("");
              System.out.println(" Password can be omitted for console prompt-input.");
              System.exit(0);

            default:
              if (params.mavenDir != null) {
                throw new RuntimeException("Exactly one maven artifact directory should be provided.");
              }
              params.mavenDir = Paths.get(args[i]);
              break;
          }
        }

        if (params.userName == null) {
          var v = envVar("ASF_USERNAME");
          if (v != null) {
            params.userName = new String(v);
          }
        }
        Objects.requireNonNull(params.userName, "User name is required for authentication.");

        if (params.userPass == null) {
          params.userPass = envVar("ASF_PASSWORD");
          if (params.userPass == null) {
            Console console = System.console();
            if (console != null) {
              System.out.println("Enter password for " + params.userName + ":");
              params.userPass = console.readPassword();
            } else {
              throw new RuntimeException("No console, can't prompt for password.");
            }
          }
        }
        Objects.requireNonNull(params.userPass, "User password is required for authentication.");

        if (params.mavenDir == null || !Files.isDirectory(params.mavenDir)) {
          throw new RuntimeException("Maven artifact directory is required and must exist.");
        }
        return params;
      } catch (IndexOutOfBoundsException e) {
        throw new RuntimeException("Required argument missing (pass --help for help)?");
      }
    }
  }

  private static class NexusApi {
    private final HttpClient client;
    private final URI nexusUri;

    public NexusApi(Params params) {
      Authenticator authenticator = new Authenticator() {
        @Override
        protected PasswordAuthentication getPasswordAuthentication() {
          return new PasswordAuthentication(params.userName, params.userPass);
        }
      };

      this.client = HttpClient.newBuilder()
          .authenticator(authenticator)
          .build();

      this.nexusUri = params.nexusUri;
    }

    public String requestProfileId(PomInfo pomInfo) throws IOException {
      String result = sendGet("/service/local/staging/profile_evaluate", Map.of(
          "g", pomInfo.groupId,
          "a", pomInfo.artifactId,
          "v", pomInfo.version,
          "t", "maven2"
      ));

      return XmlElement.parse(result)
          .onlychild("stagingProfiles")
          .onlychild("data")
          .onlychild("stagingProfile")
          .onlychild("id")
          .text();
    }

    public String createStagingRepository(String profileId, String description) throws IOException {
      String result = sendPost("/service/local/staging/profiles/" + URLEncoder.encode(profileId, StandardCharsets.UTF_8) + "/start",
          "application/xml",
          HttpURLConnection.HTTP_CREATED,
          ("<promoteRequest>\n" +
              "  <data>\n" +
              "    <description><![CDATA[" + description + "]]></description>\n" +
              "  </data>\n" +
              "</promoteRequest>").getBytes(StandardCharsets.UTF_8));

      return XmlElement.parse(result)
          .onlychild("promoteResponse")
          .onlychild("data")
          .onlychild("stagedRepositoryId")
          .text();
    }

    public void uploadArtifact(String stagingRepoId, Path path, String relativePath) throws IOException {
      sendPost("/service/local/staging/deployByRepositoryId/"
              + URLEncoder.encode(stagingRepoId, StandardCharsets.UTF_8)
              + "/"
              + relativePath,
          "application/octet-stream",
          HttpURLConnection.HTTP_CREATED,
          Files.readAllBytes(path));
    }

    public void closeStagingRepository(String profileId, String stagingRepoId) throws IOException {
      sendPost("/service/local/staging/profiles/" + URLEncoder.encode(profileId, StandardCharsets.UTF_8) + "/finish",
          "application/xml",
          HttpURLConnection.HTTP_CREATED,
          ("<promoteRequest>\n" +
              "  <data>\n" +
              "    <stagedRepositoryId><![CDATA[" + stagingRepoId + "]]></stagedRepositoryId>\n" +
              "  </data>\n" +
              "</promoteRequest>").getBytes(StandardCharsets.UTF_8));
    }

    private String sendPost(String serviceEndpoint, String contentType, int expectedStatus, byte[] bytes) throws IOException {
      URI target = nexusUri.resolve(serviceEndpoint);

      try {
        HttpResponse.BodyHandler<String> bodyHandler = HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8);
        HttpRequest req = HttpRequest.newBuilder()
            .POST(HttpRequest.BodyPublishers.ofByteArray(bytes))
            .header("Content-Type", contentType)
            .uri(target)
            // we could use json if XML is too difficult to work with.
            // .header("Accept", "application/json")
            .build();
        HttpResponse<String> response = client.send(req, bodyHandler);
        if (response.statusCode() != expectedStatus) {
          throw new IOException("Unexpected HTTP error returned: " + response.statusCode() + ", response body: "
              + response.body());
        }
        return response.body();
      } catch (InterruptedException e) {
        throw new IOException("HTTP timeout", e);
      }
    }

    private String sendGet(String serviceEndpoint, Map<String, String> getArgs) throws IOException {
      // JDK: jeez... why make a http client and not provide uri-manipulation utilities?
      URI target;
      try {
        target = nexusUri.resolve(serviceEndpoint);
        target = new URI(
            target.getScheme(), target.getUserInfo(), target.getHost(), target.getPort(),
            target.getPath(),
            getArgs.entrySet().stream()
                .map(e -> entityEncode(e.getKey()) + "=" + entityEncode(e.getValue()))
                .collect(Collectors.joining("&")),
            null);

        HttpResponse.BodyHandler<String> bodyHandler = HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8);
        HttpRequest req = HttpRequest.newBuilder()
            .GET()
            .uri(target)
            // we could use json if XML is too difficult to work with.
            // .header("Accept", "application/json")
            .build();
        HttpResponse<String> response = client.send(req, bodyHandler);
        if (response.statusCode() != HttpURLConnection.HTTP_OK) {
          throw new IOException("Unexpected HTTP error returned: " + response.statusCode());
        }
        return response.body();
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        throw new IOException("HTTP timeout", e);
      }
    }

    private String entityEncode(String value) {
      return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
  }

  private static class XmlElement {
    private final Node element;

    static XmlElement parse(String xml) throws IOException {
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      try (var is = new StringReader(xml)) {
        Document parse = dbf.newDocumentBuilder().parse(new InputSource(is));
        return new XmlElement(parse);
      } catch (ParserConfigurationException | SAXException e) {
        throw new RuntimeException(e);
      }
    }

    public XmlElement(Node child) {
      this.element = child;
    }

    public XmlElement onlychild(String tagName) throws IOException {
      ArrayList<XmlElement> children = new ArrayList<>();
      NodeList childNodes = element.getChildNodes();
      for (int i = 0, max = childNodes.getLength(); i < max; i++) {
        var child = childNodes.item(i);
        if (child.getNodeType() == Node.ELEMENT_NODE &&
            Objects.equals(child.getNodeName(), tagName)
        ) {
          children.add(new XmlElement(child));
        }
      }
      if (children.isEmpty()) {
        throw new IOException("No child node found for: " + tagName);
      }
      return children.get(0);
    }

    public String text() {
      return element.getTextContent();
    }
  }

  private static class PomInfo {
    String groupId;
    String artifactId;
    String version;

    public static PomInfo extractPomInfo(Path path) throws IOException {
      PomInfo pomInfo = new PomInfo();
      XmlElement project =
          XmlElement.parse(Files.readString(path, StandardCharsets.UTF_8))
              .onlychild("project");
      pomInfo.groupId = project.onlychild("groupId").text();
      pomInfo.artifactId = project.onlychild("artifactId").text();
      pomInfo.version = project.onlychild("version").text();
      return pomInfo;
    }
  }

  public static void main(String[] args) throws Exception {
    try {
      var params = Params.parse(args);
      var nexus = new NexusApi(params);

      // Collect all files to be uploaded.
      List<Path> artifacts;
      try (Stream<Path> pathStream = Files.walk(params.mavenDir)) {
        artifacts = pathStream
            .filter(Files::isRegularFile)
            // Ignore locally generated maven metadata files.
            .filter(path -> !path.getFileName().toString().startsWith("maven-metadata."))
            .sorted(Comparator.comparing(Path::toString))
            .collect(Collectors.toList());
      }

      // Figure out nexus profile ID based on POMs. It is assumed that all artifacts
      // fall under the same profile.
      PomInfo pomInfo = PomInfo.extractPomInfo(
          artifacts.stream()
              .filter(path -> path.getFileName().toString().endsWith(".pom"))
              .findFirst()
              .orElseThrow());

      // Sanity check for directory structure - all files should have the groupId folder prefix.
      {
        String expectedPrefix = pomInfo.groupId.replace('.', '/');
        for (Path path : artifacts) {
          Path relative = params.mavenDir.relativize(path);
          List<String> urlSegments = new ArrayList<>();
          for (Path segment : relative) {
            urlSegments.add(URLEncoder.encode(segment.toString(), StandardCharsets.UTF_8));
          }
          String relativeUrl = String.join("/", urlSegments);

          if (!relativeUrl.startsWith(expectedPrefix)) {
            throw new RuntimeException("Maven folder structure does not match the expected groupId, "
              + "expected prefix: " + expectedPrefix + ", artifact: " + relativeUrl);
          }
        }
      }

      System.out.println("Requesting profile ID for artifact: "
          + pomInfo.groupId + ":" + pomInfo.artifactId + ":" + pomInfo.version);
      String profileId = nexus.requestProfileId(pomInfo);
      System.out.println("  => Profile ID: " + profileId);

      System.out.println("Creating staging repository.");
      String description = Objects.requireNonNullElse(params.description,
          "Staging repository: " + pomInfo.groupId + "."
              + pomInfo.artifactId + ":" + pomInfo.version);
      String stagingRepoId = nexus.createStagingRepository(profileId, description);
      System.out.println("  => Staging repository ID: " + stagingRepoId);

      System.out.printf("Uploading %s artifact(s).%n", artifacts.size());
      for (Path path : artifacts) {
        Path relative = params.mavenDir.relativize(path);
        List<String> urlSegments = new ArrayList<>();
        for (Path segment : relative) {
          urlSegments.add(URLEncoder.encode(segment.toString(), StandardCharsets.UTF_8));
        }
        String relativeUrl = String.join("/", urlSegments);
        System.out.println("  => " + relative + ": " + relativeUrl);
        nexus.uploadArtifact(stagingRepoId, path, relativeUrl);
      }

      System.out.println("Closing the staging repository.");
      nexus.closeStagingRepository(profileId, stagingRepoId);
      System.out.println("  => Staging repository is available at: ");
      System.out.println("     https://repository.apache.org/content/repositories/" + stagingRepoId);

      System.out.println();
      System.out.println("You must review and release the staging repository manually from Nexus GUI!");
    } catch (Exception e) {
      System.err.println("Something went wrong: " + e.getMessage());
      System.exit(1);
    }
  }
}
