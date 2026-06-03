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

Apache Lucene is a high-performance, full-featured text search engine library
written in Java.

[![Build Status](https://ci-builds.apache.org/job/Lucene/job/Lucene-Artifacts-main/badge/icon?subject=Lucene)](https://ci-builds.apache.org/job/Lucene/job/Lucene-Artifacts-main/)
[![Revved up by Develocity](https://img.shields.io/badge/Revved%20up%20by-Develocity-06A0CE?logo=Gradle&labelColor=02303A)](https://develocity.apache.org/scans?search.buildToolType=gradle&search.rootProjectNames=lucene-root)

## Online Documentation

This README file only contains basic setup instructions. For more
comprehensive documentation, visit:

- Latest Releases: <https://lucene.apache.org/core/documentation.html>
- Nightly: <https://ci-builds.apache.org/job/Lucene/job/Lucene-Artifacts-main/javadoc/>
- New contributors should start by reading [Contributing Guide](./CONTRIBUTING.md)
- Build System Documentation: [help/](./help/)
- Migration Guide: [lucene/MIGRATE.md](./lucene/MIGRATE.md)

## Building

### Basic steps

1. Install JDK 25 using your package manager or download manually from
[OpenJDK](https://jdk.java.net/),
[Adoptium](https://adoptium.net/temurin/releases),
[Azul](https://www.azul.com/downloads/),
[Oracle](https://www.oracle.com/java/technologies/downloads/) or any other JDK provider.
2. Clone Lucene's git repository (or download the source distribution).
3. Run gradle launcher script (`gradlew`).

We'll assume that you know how to get and set up the JDK - if you don't, then we suggest starting at <https://jdk.java.net/> and learning more about Java, before returning to this README.

## Contributing

Bug fixes, improvements and new features are always welcome!
Please review the [Contributing to Lucene
Guide](./CONTRIBUTING.md) for information on
contributing.

- Additional Developer Documentation: [dev-docs/](./dev-docs/)

## Discussion and Support

- [Users Mailing List](https://lucene.apache.org/core/discussion.html#java-user-list-java-userluceneapacheorg)
- [Developers Mailing List](https://lucene.apache.org/core/discussion.html#developer-lists)
- IRC: `#lucene` and `#lucene-dev` on freenode.net
