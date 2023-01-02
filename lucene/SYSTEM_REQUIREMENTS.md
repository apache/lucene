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

# System Requirements 

Apache Lucene runs on Java 11 or greater.

It is also recommended to always use the latest update version of your
Java VM, because bugs may affect Lucene. An overview of known JVM bugs
can be found on http://wiki.apache.org/lucene-java/JavaBugs

With all Java versions it is strongly recommended to not use experimental
`-XX` JVM options.

CPU, disk and memory requirements are based on the many choices made in 
implementing Lucene (document size, number of documents, and number of 
hits retrieved to name a few). The benchmarks page has some information 
related to performance on particular platforms. 
