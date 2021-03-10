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

Apache Lucene is a high-performance, full featured text search engine library
written in Java.

[![Build Status](https://ci-builds.apache.org/job/Lucene/job/Lucene-Artifacts-master/badge/icon?subject=Lucene)](https://ci-builds.apache.org/job/Lucene/job/Lucene-Artifacts-master/)

## Online Documentation

This README file only contains basic setup instructions.  For more
comprehensive documentation, visit:

- Lucene: <http://lucene.apache.org/core/documentation.html>

## Building with Gradle

### Building Lucene

See [lucene/BUILD.md](./lucene/BUILD.md).

### Gradle build and IDE support

- *IntelliJ* - IntelliJ idea can import the project out of the box. 
               Code formatting conventions should be manually adjusted. 
- *Eclipse*  - Not tested.
- *Netbeans* - Not tested.


### Gradle build and tests

`./gradlew assemble` will build libraries and documentation.

`./gradlew check` will run all validation tasks and unit tests.

`./gradlew help` will print a list of help commands for high-level tasks. One
  of these is `helpAnt` that shows the gradle tasks corresponding to ant
  targets you may be familiar with.

## Contributing

Please review the [Contributing to Lucene
Guide](https://cwiki.apache.org/confluence/display/lucene/HowToContribute) for information on
contributing.

## Discussion and Support

- [Users Mailing List](https://lucene.apache.org/core/discussion.html#java-user-list-java-userluceneapacheorg)
- [Developers Mailing List](https://lucene.apache.org/core/discussion.html#developer-lists)
- [Issue Tracker](https://issues.apache.org/jira/browse/LUCENE)
- IRC: `#lucene` and `#lucene-dev` on freenode.net
