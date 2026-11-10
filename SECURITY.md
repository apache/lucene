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

# Security Policy

## Reporting a Vulnerability

Apache Lucene is a project of the Apache Software Foundation (ASF) and follows the standard [ASF vulnerability handling process](https://apache.org/security/#vulnerability-handling). Please report suspected vulnerabilities privately to [security@apache.org](mailto:security@apache.org) or to the project's private list at [security@lucene.apache.org](mailto:security@lucene.apache.org). Do not report vulnerabilities through public GitHub issues, pull requests, or mailing lists. The Apache Software Foundation, being a volunteer organization, does not have a bug bounty program.

Please do not send automated scanner reports or reports about publicly known CVEs in third-party dependencies. Dependency upgrades can be requested through a regular GitHub issue.

## Scope and Threat Model

Apache Lucene is a low-level search library, not a standalone server or end-user application. It is embedded into other software ("downstream applications") such as Apache Solr, Elasticsearch, OpenSearch, and many custom products. Lucene's threat model assumes that the downstream application is responsible for authentication, authorization, network transport, and sanitization of all untrusted input. Reports that assume an attacker can directly feed arbitrary data into low-level Lucene APIs are therefore generally not treated as vulnerabilities in Lucene itself.

### Index files are trusted

Lucene index files are fully trusted by the Lucene code. For performance reasons, index file contents are not defensively validated when read. Modifying, corrupting, or crafting index files can cause undefined behavior such as endless loops, `ArrayIndexOutOfBoundsException`, other runtime exceptions, excessive memory allocation, or JVM crashes. This is by design and is not considered a security issue.

Protecting index files from tampering is the responsibility of the downstream application and the operating environment. Applications that replicate or transfer index files over the network (as Solr, Elasticsearch, and OpenSearch do) must ensure the integrity and authenticity of those files themselves, for example through authenticated transport and filesystem permissions. An attacker who can write to index files already controls the data layer, which is outside Lucene's trust boundary.

### Untrusted query and analysis input

Query parsers, analyzers, and tokenizers operate on the input they are given. Certain inputs, such as pathological regular expressions, deeply nested queries, huge wildcard or fuzzy expansions, or extremely large tokens, can consume significant CPU or memory. Lucene provides mechanisms to bound this (for example, timeouts, `maxDeterminizedStates`, clause limits, and length limits), but applying such limits to untrusted user input is the responsibility of the downstream application. Resource exhaustion caused by unbounded, unfiltered input passed directly to Lucene APIs is not considered a vulnerability in Lucene.

### Other out-of-scope reports

The same principle applies to other APIs that consume caller-provided data: file paths passed to directory implementations, serialized or externally supplied data structures, and configuration values are trusted as given. Lucene also makes no security guarantees when running with untrusted code in the same JVM; it is a library executing with the full privileges of its host process.

### Reporting out-of-scope issues publicly

Issues that fall outside the scope above are still valid bug reports and hardening suggestions. They can be opened as regular public GitHub issues or pull requests, since no coordinated disclosure is needed. For example, a change that prevents an endless loop or replaces a low-level failure with a clean `CorruptIndexException` on a broken index is welcome, provided it does not negatively impact performance. Robustness improvements must not slow down the hot paths that motivate Lucene's trust in its input.

### In-scope reports

We do treat a bug as a security issue if it allows an attacker to break a guarantee that Lucene explicitly documents as safe to rely on for untrusted input, despite correct usage. Examples are memory corruption or unexpected code execution triggered through such APIs while reasonable limits recommended by the documentation are in place.

## Supported Versions

Security fixes are applied to the current main branch and the stable release branch. Users should always run the latest release of the branch they use. Older major versions do not receive security updates.
