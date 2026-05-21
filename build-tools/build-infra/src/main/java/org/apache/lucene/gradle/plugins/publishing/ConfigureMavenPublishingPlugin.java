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
package org.apache.lucene.gradle.plugins.publishing;

import java.util.List;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.component.AdhocComponentWithVariants;
import org.gradle.api.component.ConfigurationVariantDetails;
import org.gradle.api.plugins.BasePluginExtension;
import org.gradle.api.provider.Provider;
import org.gradle.api.publish.PublishingExtension;
import org.gradle.api.publish.maven.MavenPom;
import org.gradle.api.publish.maven.MavenPublication;
import org.gradle.api.publish.maven.plugins.MavenPublishPlugin;
import org.gradle.api.publish.tasks.GenerateModuleMetadata;
import org.gradle.api.tasks.Delete;
import org.gradle.api.tasks.bundling.Jar;
import org.gradle.plugins.signing.SigningExtension;
import org.gradle.plugins.signing.SigningPlugin;

/**
 * This plugin configures aspects related to all project publications, including:
 *
 * <ul>
 *   <li>configuring maven artifacts
 *   <li>setting up target maven repositories for publications to end up on
 *   <li>configuring binary and source release artifacts
 *   <li>other concerns related to publishing
 * </ul>
 */
public class ConfigureMavenPublishingPlugin extends LuceneGradlePlugin {
  public static final String OPT_SIGN = "sign";
  public static final String MAVEN_ARTIFACTS_CONFIGURATION = "mavenArtifacts";

  @Override
  public void apply(Project project) {
    applicableToRootProjectOnly(project);

    getBuildOptions(project)
        .addBooleanOption(OPT_SIGN, "Sign assembled distribution artifacts.", false);

    configureGpgSigning(project);
    getLuceneBuildGlobals(project).getPublishedProjects().forEach(this::configureMavenPublications);

    configurePublicationsToLocalMavenRepository(project);
    configurePublicationsToLocalBuildDirectory(project);
    configurePublicationsToApacheNexus(project, true);
    configurePublicationsToApacheNexus(project, false);
  }

  /** Configure artifact push to apache nexus (releases or snapshots repository). */
  private void configurePublicationsToApacheNexus(
      Project rootProject, boolean useReleaseRepository) {
    var apacheNexusRepositoryUrl =
        useReleaseRepository
            ? "https://repository.apache.org/service/local/staging/deploy/maven2"
            : "https://repository.apache.org/content/repositories/snapshots";

    var type = useReleaseRepository ? "Releases" : "Snapshots";

    var publicationTask =
        useReleaseRepository
            ? "publishSignedJarsPublicationToApacheReleasesRepository"
            : "publishJarsPublicationToApacheSnapshotsRepository";

    var tasks = rootProject.getTasks();
    tasks.register(
        "mavenToApache" + type,
        task -> {
          task.setGroup("Distribution");
          task.setDescription(
              "Publish Lucene Maven artifacts to Apache "
                  + type
                  + " repository: "
                  + apacheNexusRepositoryUrl);

          for (var p : getLuceneBuildGlobals(rootProject).getPublishedProjects()) {
            task.dependsOn(p.getTasks().matching(t -> t.getName().equals(publicationTask)));
          }
        });

    // These access credentials must be passed by the release manager
    // (either on command-line, via the environment or via ~/.gradle.properties).
    var providers = rootProject.getProviders();
    Provider<String> asfNexusUsername =
        providers
            .gradleProperty("asfNexusUsername")
            .orElse(providers.systemProperty("asfNexusUsername"))
            .orElse(providers.environmentVariable("ASF_NEXUS_USERNAME"));

    Provider<String> asfNexusPassword =
        providers
            .gradleProperty("asfNexusPassword")
            .orElse(providers.systemProperty("asfNexusPassword"))
            .orElse(providers.environmentVariable("ASF_NEXUS_PASSWORD"));

    var buildGlobals = getLuceneBuildGlobals(rootProject);
    var checkRepositoryPushPreconditions =
        tasks.register(
            "check" + type + "RepositoryPushPreconditions",
            task -> {
              task.doFirst(
                  _ -> {
                    // Make sure we're pushing a release version to release repo and a snapshot
                    // to a snapshot repo.
                    if (buildGlobals.snapshotBuild == useReleaseRepository) {
                      throw new GradleException(
                          type
                              + " repository will not accept this version: "
                              + rootProject.getVersion());
                    }

                    // Make sure access credentials have been passed.
                    if (!asfNexusUsername.isPresent() || !asfNexusPassword.isPresent()) {
                      throw new GradleException(
                          "asfNexusUsername or asfNexusPassword is empty: these are required to publish to "
                              + " ASF Nexus.");
                    }
                  });
            });

    buildGlobals
        .getPublishedProjects()
        .forEach(
            project -> {
              // Add the publishing repository
              var publishingExtension =
                  project.getExtensions().getByType(PublishingExtension.class);
              publishingExtension
                  .getRepositories()
                  .maven(
                      repo -> {
                        repo.setName("Apache" + type);
                        repo.setUrl(apacheNexusRepositoryUrl);
                        repo.credentials(
                            creds -> {
                              creds.setUsername(asfNexusUsername.getOrNull());
                              creds.setPassword(asfNexusPassword.getOrNull());
                            });
                      });

              // Make sure any actual publication task is preceded by precondition checks.
              project
                  .getTasks()
                  .matching(
                      task -> task.getName().matches("publish.+ToApache" + type + "Repository"))
                  .configureEach(
                      t -> {
                        t.dependsOn(checkRepositoryPushPreconditions);
                      });
            });
  }

  /**
   * Configure maven publishing task that copies artifacts into the root project's {@code
   * build/maven-artifacts}. This is used in smoke-testing script runs.
   */
  private void configurePublicationsToLocalBuildDirectory(Project project) {
    var mavenRepositoryDir = project.getLayout().getBuildDirectory().dir("maven-artifacts");
    var publishedProjects = getLuceneBuildGlobals(project).getPublishedProjects();

    var tasks = project.getTasks();
    var mavenToBuildTask =
        tasks.register(
            "mavenToBuild",
            task -> {
              task.getOutputs().dir(mavenRepositoryDir);

              // In signed mode, collect signed artifacts. Otherwise, collect
              // unsigned JARs (and their checksums).
              boolean withSignedArtifacts =
                  getBuildOptions(project).getOption(OPT_SIGN).asBooleanProvider().get();
              var mavenConventionTask =
                  withSignedArtifacts
                      ? "publishSignedJarsPublicationToBuildRepository"
                      : "publishJarsPublicationToBuildRepository";

              task.dependsOn(
                  publishedProjects.stream()
                      .map(p -> p.getTasks().matching(t -> t.getName().equals(mavenConventionTask)))
                      .toList());
            });

    var cleanBuildTask =
        tasks.register(
            "cleanMavenBuildRepository",
            Delete.class,
            task -> {
              task.delete(mavenRepositoryDir);
            });

    for (var p : publishedProjects) {
      // Clean the build repository prior to publishing anything. Ensures we don't
      // have multiple artifacts there.
      p.getTasks()
          .matching(t -> t.getName().matches("publish.+ToBuildRepository"))
          .configureEach(
              t -> {
                t.dependsOn(cleanBuildTask);
              });

      var publishingExtension = p.getExtensions().getByType(PublishingExtension.class);
      publishingExtension
          .getRepositories()
          .maven(
              repo -> {
                repo.setName("Build");
                repo.setUrl(mavenRepositoryDir);
              });
    }

    project.getConfigurations().create(MAVEN_ARTIFACTS_CONFIGURATION);

    project
        .getArtifacts()
        .add(
            MAVEN_ARTIFACTS_CONFIGURATION,
            mavenRepositoryDir,
            configurablePublishArtifact -> {
              configurablePublishArtifact.builtBy(mavenToBuildTask);
            });
  }

  /**
   * Configure maven publishing task that copies artifacts into {@code ~/.m2} (this is to allow
   * local testing and review, mostly).
   */
  private void configurePublicationsToLocalMavenRepository(Project project) {
    project
        .getTasks()
        .register(
            "mavenToLocal",
            task -> {
              task.setGroup("Distribution");
              task.setDescription("Publish Lucene Maven artifacts to ~/.m2 repository.");

              task.dependsOn(
                  getLuceneBuildGlobals(project).getPublishedProjects().stream()
                      .map(p -> p.getTasks().named("publishJarsPublicationToMavenLocal"))
                      .toList());
            });
  }

  private void configureGpgSigning(Project project) {
    Provider<Boolean> useGpgOption =
        getBuildOptions(project)
            .addBooleanOption("useGpg", "Use GPG for signing artifacts.", false);

    if (useGpgOption.get()) {
      // Do this check before 'useGpgCmd()' (and once), otherwise gradle will fail
      // with a confusing error about 'signatory.keyId'
      //
      // 'signatory.keyId' is an implementation detail of the SigningPlugin that it populates
      // from 'signing.gnupg.keyName' when useGpgCmd()
      // is used -- but does not explain in the error produced if 'signing.gnupg.keyName' is not
      // set.
      var propName = "signing.gnupg.keyName";
      if (!project.hasProperty(propName)) {
        throw new GradleException(
            "'"
                + propName
                + "' project property must be set when using external GPG via 'useGpg', please see help/publishing.txt");
      }

      for (var p : project.getAllprojects()) {
        p.getPlugins()
            .withType(SigningPlugin.class)
            .configureEach(
                _ -> {
                  p.getExtensions().configure(SigningExtension.class, SigningExtension::useGpgCmd);
                });
      }
    }
  }

  private void configureMavenPublications(Project project) {
    project.getPlugins().apply(MavenPublishPlugin.class);
    project.getPlugins().apply(SigningPlugin.class);

    var signingExtension = project.getExtensions().getByType(SigningExtension.class);
    var publishingExtension = project.getExtensions().getByType(PublishingExtension.class);
    var publications = publishingExtension.getPublications();

    // We have two types of publications: jars and signed jars.
    var jarsPublication = publications.create("jars", MavenPublication.class);
    var signedJarsPublication = publications.create("signedJars", MavenPublication.class);

    // signedJars publication is always signed.
    signingExtension.sign(signedJarsPublication);

    // Each publication consists of the java components, source and javadoc artifacts.
    // Add tasks to assemble source and javadoc JARs.
    var sourcesJarTask =
        project
            .getTasks()
            .register(
                "sourcesJar",
                Jar.class,
                sourcesJar -> {
                  sourcesJar.dependsOn("classes");
                  sourcesJar.getArchiveClassifier().set("sources");
                  sourcesJar.from(
                      project
                          .getExtensions()
                          .getByType(org.gradle.api.plugins.JavaPluginExtension.class)
                          .getSourceSets()
                          .getByName("main")
                          .getAllJava());
                });

    var javadocJarTask =
        project
            .getTasks()
            .register(
                "javadocJar",
                Jar.class,
                javadocJar -> {
                  javadocJar.dependsOn("javadoc");
                  javadocJar.getArchiveClassifier().set("javadoc");
                  javadocJar.from(
                      project.getTasks().named("javadoc").get().getOutputs().getFiles());
                });

    for (MavenPublication pub : List.of(jarsPublication, signedJarsPublication)) {
      pub.setGroupId(project.getGroup().toString());
      pub.setArtifactId(
          project.getExtensions().getByType(BasePluginExtension.class).getArchivesName().get());

      pub.from(project.getComponents().getByName("java"));

      pub.artifact(sourcesJarTask);
      pub.artifact(javadocJarTask);

      configurePom(project, pub);
    }

    // Hack: prevent any test fixture JARs from being published.
    project
        .getConfigurations()
        .matching(
            cfg ->
                cfg.getName().equals("testFixturesApiElements")
                    || cfg.getName().equals("testFixturesRuntimeElements"))
        .configureEach(
            cfg -> {
              project.getComponents().stream()
                  .filter(c -> "java".equals(c.getName()))
                  .filter(c -> c instanceof AdhocComponentWithVariants)
                  .map(c -> (AdhocComponentWithVariants) c)
                  .forEach(
                      comp ->
                          comp.withVariantsFromConfiguration(
                              cfg, ConfigurationVariantDetails::skip));
            });

    // Hack: do not generate or publish Gradle module metadata (*.module)
    project
        .getTasks()
        .withType(GenerateModuleMetadata.class)
        .configureEach(t -> t.setEnabled(false));
  }

  private void configurePom(Project project, MavenPublication pub) {
    MavenPom pom = pub.getPom();

    Provider<String> prov =
        project.getProviders().provider(() -> "Apache Lucene (module: " + project.getName() + ")");

    pom.getName().set(prov);
    pom.getDescription().set(prov);
    pom.getUrl().set("https://lucene.apache.org/");

    pom.licenses(
        licenses ->
            licenses.license(
                lic -> {
                  lic.getName().set("Apache 2");
                  lic.getUrl().set("https://www.apache.org/licenses/LICENSE-2.0.txt");
                }));

    pom.getInceptionYear().set("2000");

    pom.issueManagement(
        im -> {
          im.getSystem().set("github");
          im.getUrl().set("https://github.com/apache/lucene/issues");
        });

    pom.ciManagement(
        ci -> {
          ci.getSystem().set("Jenkins");
          ci.getUrl().set("https://builds.apache.org/job/Lucene/");
        });

    pom.mailingLists(
        mls -> {
          mls.mailingList(
              ml -> {
                ml.getName().set("Java User List");
                ml.getSubscribe().set("java-user-subscribe@lucene.apache.org");
                ml.getUnsubscribe().set("java-user-unsubscribe@lucene.apache.org");
                ml.getArchive().set("https://mail-archives.apache.org/mod_mbox/lucene-java-user/");
              });
          mls.mailingList(
              ml -> {
                ml.getName().set("Java Developer List");
                ml.getSubscribe().set("dev-subscribe@lucene.apache.org");
                ml.getUnsubscribe().set("dev-unsubscribe@lucene.apache.org");
                ml.getArchive().set("https://mail-archives.apache.org/mod_mbox/lucene-dev/");
              });
          mls.mailingList(
              ml -> {
                ml.getName().set("Java Commits List");
                ml.getSubscribe().set("commits-subscribe@lucene.apache.org");
                ml.getUnsubscribe().set("commits-unsubscribe@lucene.apache.org");
                ml.getArchive()
                    .set("https://mail-archives.apache.org/mod_mbox/lucene-java-commits/");
              });
        });

    pom.scm(
        scm -> {
          scm.getConnection().set("scm:git:https://gitbox.apache.org/repos/asf/lucene.git");
          scm.getDeveloperConnection()
              .set("scm:git:https://gitbox.apache.org/repos/asf/lucene.git");
          scm.getUrl().set("https://gitbox.apache.org/repos/asf?p=lucene.git");
        });
  }
}
