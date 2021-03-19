<!--
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 -->

# Apache Lucene

![Lucene Logo](https://lucene.apache.org/theme/images/lucene/lucene_logo_green_300.png?v=0e493d7a)

Apache Lucene is a high-performance, full featured text search engine library
written in Java.

[![Build Status](https://ci-builds.apache.org/job/Lucene/job/Lucene-Artifacts-main/badge/icon?subject=Lucene)](https://ci-builds.apache.org/job/Lucene/job/Lucene-Artifacts-main/)

## Online Documentation

This README file only contains basic setup instructions.  For more
comprehensive documentation, visit:

- Lucene: <http://lucene.apache.org/core/documentation.html>

## Building with Gradle

### Basic steps:
  
  0. Install OpenJDK 11 (or greater)
  1. Download Lucene from Apache and unpack it
  2. Connect to the top-level of your installation (parent of the lucene top-level directory)
  3. Run gradle

### Step 0) Set up your development environment (OpenJDK 11 or greater)

We'll assume that you know how to get and set up the JDK - if you
don't, then we suggest starting at https://www.oracle.com/java/ and learning
more about Java, before returning to this README. Lucene runs with
Java 11 and later.

Lucene uses [Gradle](https://gradle.org/) for build control.

NOTE: Lucene changed from Ant to Gradle as of release 9.0. Prior releases
still use Ant.

### Step 1) Checkout/Download Lucene source code

We'll assume you already did this, or you wouldn't be reading this
file. However, you might have received this file by some alternate
route, or you might have an incomplete copy of the Lucene, so: you 
can directly checkout the source code from GitHub:

  https://github.com/apache/lucene
  
Or Lucene source archives at particlar releases are available as part of Lucene downloads:

  https://lucene.apache.org/core/downloads.html

Download either a zip or a tarred/gzipped version of the archive, and
uncompress it into a directory of your choice.

### Step 2) Change directory (cd) into the top-level directory of the source tree

The parent directory for Lucene contains the base configuration file for the build.
By default, you do not need to change any of the settings in this file, but you do
need to run Gradle from this location so it knows where to find the necessary
configurations.

### Step 3) Run Gradle

Assuming you can exectue "./gradlew help" should show you the main tasks that
can be executed to show help sub-topics.

If you want to build Lucene, type:

```
./gradlew assemble
```

NOTE: DO NOT use `gradle` command that is already installed on your machine (unless you know what you'll do).
The "gradle wrapper" (gradlew) does the job - downloads the correct version of it, setups necessary configurations.

The first time you run Gradle, it will create a file "gradle.properties" that
contains machine-specific settings. Normally you can use this file as-is, but it
can be modified if necessary.

`./gradlew check` will assemble Lucene and run all validation
  tasks unit tests.

`./gradlew help` will print a list of help commands for high-level tasks. One
  of these is `helpAnt` that shows the gradle tasks corresponding to ant
  targets you may be familiar with.

If you want to build the documentation, type:

```
./gradlew documentation
```

### Gradle build and IDE support

- *IntelliJ* - IntelliJ idea can import the project out of the box. 
               Code formatting conventions should be manually adjusted. 
- *Eclipse*  - Basic support ([help/IDEs.txt](https://github.com/apache/lucene/blob/main/help/IDEs.txt#L7)).
- *Netbeans* - Not tested.

## Contributing

Please review the [Contributing to Lucene
Guide](https://cwiki.apache.org/confluence/display/lucene/HowToContribute) for information on
contributing.

## Discussion and Support

- [Users Mailing List](https://lucene.apache.org/core/discussion.html#java-user-list-java-userluceneapacheorg)
- [Developers Mailing List](https://lucene.apache.org/core/discussion.html#developer-lists)
- [Issue Tracker](https://issues.apache.org/jira/browse/LUCENE)
- IRC: `#lucene` and `#lucene-dev` on freenode.net
