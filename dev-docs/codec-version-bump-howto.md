<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Lucene Codec Version Bump How-To Manual

Changing the name of the codec in Lucene is required for maintaining backward compatibility and ensuring a smooth transition for users. 
Through explicit versioning, we isolate changes to the codec, and prevent unintended interactions between different codec 
versions. Changes to the codec format version serves as a signal that a change in the format or structure of the index has occurred.

This manual provides a step-by-step guide through the process of "bumping" the version of the Lucene Codec.

### Fork and modify files
* Creating a new Codec:
  * Create a new package for your new codec of version XXX: `org.apache.lucene.codecs.luceneXXX`.
  * Copy the previous codec in, updating all references to the old version number.
  * Set the new codec, as the `defaultCodec` in [Codec.java](https://github.com/apache/lucene/blob/main/lucene/core/src/java/org/apache/lucene/codecs/Codec.java).
  * Update all references from the old codec version to the new one.
* Fork Components:
  * Fork the required codec components, ensure the new codec is using those components.
  * Fork subclasses impacted by the codec changes and update version numbers accordingly.
* ForUtil (if modified):
  * Move the unmodified `gen_ForUtil.py` to the latest `backward_codecs` package. Move the modified `gen_ForUtil.py` to the newly forked codec package.
  * Change the [generateForUtil.json](https://github.com/apache/lucene/blob/main/lucene/core/src/generated/checksums/generateForUtil.json) to use the newest `luceneXXX` package. Create a [generateForUtilXXX.json](https://github.com/apache/lucene/tree/main/lucene/backward-codecs/src/generated/checksums) for the `gen_ForUtil.py` moved to `backwards_codecs`.
  * In [forUtil.gradle](https://github.com/apache/lucene/blob/main/gradle/generation/forUtil.gradle), change the genDir to the latest 
    codec. Create a new task to `generateForUtil` for the `ForUtil` in `backwards_codecs`.
  * Generate the `ForUtil`'s by running the command : `./gradlew generateForUtil`.

### Move original files to backwards_codecs
* Deprecate previous Codec:
  * Create a new package for the previous codec of version XXX: `org.apache.lucene.backward_codecs.luceneXXX.LuceneXXXCodec`.
* Move the original copy of other files that are to be modified to the new `backwards_codecs` package:
  * Make them read-only where possible. e.g. in `LuceneXXXPostingsFormat`, remove instantiation of `LuceneXXXPostingsWriter`.
  * Where it exists, increment the `VERSION_CURRENT` variable.

### Unit tests
* The codec version changes to it's unit tests follow a similar structure to the source code.
* Move existing test files related to forked files to the `backward_codecs` package.
* Create RW forks of the required `backwards_codecs` components (otherwise we cannot test writing to `backwards_codecs`) for use in 
  backwards compatibility unit tests (the ones moved to backward_codecs). Copy previous implementations of the RW classes and update with the 
  formats used in that specific codec. Use the RW components in test cases of relevant codecs. 
* Create a new `test.org.apache.lucene.backward_codecs.luceneXXX` package and copy in unit tests for any forked codec components. Modify 
  the test code to call the newly forked components.
* Move tests for modified components to a new `org.apache.lucene.backward_codecs.luceneXXX` package and call the new components.
* For tests of unmodified components, change their references from the old codec to the new one if possible.

### module-info.java and META-INF
* New packages and classes will require being added to the relevant module-info.java and/or META-INF.
* Replace references to the old components with the forked components.

### Clean-up
* Refine javadocs and other documentation to align with the new codec version.
* Double check that we are not losing test coverage for our `backwards_codecs`.

### Previous PRs for reference:
* https://github.com/apache/lucene/pull/880 - Lucene91 -> Lucene92
* https://github.com/apache/lucene/pull/1041 - Lucene92 -> Lucene94
* https://github.com/apache/lucene/pull/12582 - Lucene95 -> Lucene99
* https://github.com/apache/lucene/pull/12741 - Lucene95PostingsFormat -> Lucene99PostingsFormat + ForUtil changes
