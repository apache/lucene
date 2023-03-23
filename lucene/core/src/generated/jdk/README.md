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

# Generated Java API signatures files

This directory contains generated `.apijar` files. Those are special JAR files containing
class files that only have public signatures of certain packages of the Java class
library, but no bytecode at all. Those files are only used to compile the MR-JAR of Apache
Lucene while allowing to link against APIs only provided as preview APIs in future
JDK versions.

`.apijar` files are provided for developer's convenience in the Lucene source tree.
They are not part of Lucene's APIs or source code and are not part of binary releases.
See them as binary blobs with encoded information also provided through the public
[Javadocs](https://docs.oracle.com/en/java/javase/) of the corresponding Java
class library. They contain **no** program code.

This allows Lucene developers to compile the code without downloading a copy of all
supported JDK versions (Java 19, Java 20,...).

To regenerate those files call `gradlew :lucene:core:regenerate`. While doing this
you need to either have
[Gradle toolchain auto-provisioning](https://docs.gradle.org/current/userguide/toolchains.html#sec:provisioning)
enabled (this is the default for Lucene) or use environment variables like `JAVA19_HOME`
to point the Lucene build system to missing JDK versions. The regeneration task prints
a warning if a specific JDK is missing, leaving the already existing `.apijar` file
untouched.

The extraction is done with the ASM library, see `ExtractForeignAPI.java` source code.
