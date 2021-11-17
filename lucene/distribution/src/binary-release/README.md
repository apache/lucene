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

# Apache Lucene README file

## Introduction

This is a binary distribution of Lucene. Lucene is a Java full-text 
search engine. Lucene is not a complete application, but rather a code library 
and an API that can easily be used to add search capabilities to applications.

 * The Lucene web site is at: https://lucene.apache.org/
 * Please join the Lucene-User mailing list by sending a message to:
   java-user-subscribe@lucene.apache.org

## Files in this binary distribution

The following sub-folders are included in the binary distribution of Lucene:

* `bin/`:
  Convenience scripts to launch Lucene Luke and other index-maintenance tools.
* `modules/`:
  All binary Lucene Java modules (JARs).
* `modules-thirdparty/` 
  Third-party binary modules required to run Lucene Luke.
* `licenses/`
  Third-party licenses and notice files.

Please note that this package does not include all the binary dependencies
of all Lucene modules. Up-to-date dependency information for each Lucene
module is published to  Maven central (as Maven POMs).

To review the documentation, read the main documentation page, located at:
`docs/index.html`

