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

# Contributing to Lucene Guide

## Working with Code

### Getting the source code

First of all, you need the Lucene source code.

Get the source code using: `git clone https://github.com/apache/lucene`

### Notes for required Java version

Be sure that you are using an appropriate version of the JDK. Please check [README](./README.md) for the required JDK version for current main branch.

### Building with Gradle

Lucene uses [Gradle](https://gradle.org/) for build control. Gradle is itself Java-based and may be incompatible with newer Java versions; you can still build and test Lucene with these Java releases, see [jvms.txt](./help/jvms.txt) for more information.

Run `./gradlew help`, this will print a list of help guides that introduce and explain
  various parts of the build system, including typical workflow tasks.

If you want to build Lucene, type:

```
./gradlew assemble
```

NOTE: DO NOT use the `gradle` command that is perhaps installed on your machine. This may result in using a different gradle version than the project requires and this is known to lead to very cryptic errors. The "gradle wrapper" (gradlew script) does everything required to build the project from scratch: it downloads the correct version of gradle, sets up sane local configurations and is tested on multiple environments.

The first time you run gradlew, it will create a file "gradle.properties" that contains machine-specific settings. Normally you can use this file as-is, but it can be modified if necessary.

If you want to just build the documentation, type:

```
./gradlew documentation
```

### Code formatting and checks

If you've modified any sources, run `./gradlew tidy` to apply code formatting conventions automatically (see [help/formatting.txt](https://github.com/apache/lucene/blob/main/help/formatting.txt)).

Please make sure that all unit tests and validations succeed before constructing your patch: `./gradlew check`. This will assemble Lucene and run all validation tasks (including tests). There are various commands to check the code; type `./gradlew helpWorkflow` to show typical workflow tasks ([help/workflow.txt](./help/workflow.txt)).

In case your contribution fixes a bug, please create a new test case that fails before your fix, to show the presence of the bug and ensure it never re-occurs. A test case showing the presence of a bug is also a good contribution by itself.

### IDE support

- *IntelliJ* - IntelliJ idea can import and build gradle-based projects out of the box.
- *Eclipse*  - Basic support ([help/IDEs.txt](https://github.com/apache/lucene/blob/main/help/IDEs.txt#L7)).
- *Netbeans* - Not tested.

## Benchmarking 

Use the tool suite at [luceneutil](https://github.com/mikemccand/luceneutil) to benchmark your code changes
if you think that your change may have measurably changed the performance of a task. Apache Lucene also contains an off the shelf benchmark [module](https://github.com/apache/lucene/tree/main/lucene/benchmark).

This is the same suite that is run in the [nightly benchmarks](https://home.apache.org/~mikemccand/lucenebench/).

The instructions for running the benchmarks can be found in the luceneutil [README](https://github.com/mikemccand/luceneutil/blob/master/README.md).

The Lucene community is also interested in other implementations of these benchmark tasks.
Feel free to share your findings (especially if your implementation performs better!) through the [Lucene mailing lists](https://lucene.apache.org/core/discussion.html) or open [PRs](https://github.com/mikemccand/luceneutil/pulls), [issues](https://github.com/mikemccand/luceneutil/issues) on the luceneutil project directly.

## Contributing your work

You have two options to contribute your work: you can either create a patch and attach it to an issue on [Jira](https://issues.apache.org/jira/browse/LUCENE), or open a pull request at https://github.com/apache/lucene - whichever works best for you.

Please be patient. Committers are busy people too. If no one responds to your patch after a few days, please make friendly reminders. Please incorporate others' suggestions into into your patch if you think they're reasonable. Finally, remember that even a patch that is not committed is useful to the community.

### Opening a pull request

Please refer to [GitHub's documentation](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests) for an explanation of how to create a pull request.

You can open a pull request with or without Jira issue. If there is an associated Jira issue, the pull request title should start with the Jira issue number that your work is related to, this way your pull request will get automatically linked from the Jira issue.

### Creating a patch

Note that you do not need to create a patch if you already opened a pull request on GitHub.

Before creating your patch, you may want to get 'main' up to date with the latest from upstream. This will help avoid the possibility of others finding merge conflicts when applying your patch. This can be done with git pull if main is the current branch.

If your changes are in your git working tree (i.e. not committed to a branch), then do this:

1. Check to see what files you have modified with: `git status`
2. Add any new files with: `git add src/.../MyNewClass.java`
3. In order to create a patch, just type `git diff HEAD > LUCENE-NNNN.patch`

If your changes are all committed to the current (feature) branch: `git diff main... > LUCENE-NNNN.patch`

If some of your changes are committed to the current (feature) branch but you have working-copy changes too: `git diff {{git merge-base main head > LUCENE-NNNN.patch}}`. This will save a diff into the LUCENE-NNNN.patch file. Read the patch file. Make sure it includes ONLY the modifications required to fix a single issue.

Patches should be attached to an issue in Jira.

To enable automatic validation of Jira patches, switch the issue's status to "Patch Available" by clicking on the "Enable Patch Review" button near the top of the page. This will enqueue an ASF Jenkins job PreCommit-LUCENE-Build to run various quality checks on the patch and post a validation report as a comment (by "Lucene/Solr QA") on the issue. Expect a delay of 12 hours or so before the patch validation job actually runs. Note that in order for a patch file to trigger validation, its name must conform to the naming rules outlined here: https://yetus.apache.org/documentation/in-progress/precommit-patchnames/

### Add a CHANGES entry

You may want to add a CHANGES entry to [CHANGES.txt](./lucene/CHANGES.txt). A CHANGES entry should start with the Jira issue key `LUCENE-XXXXX` or pull request number `GITHUB#XXX` that is followed by the description of the change and contributors' name. Please see the existing entries for reference.

## Stay involved

Contributors should join the [Lucene mailing lists](https://lucene.apache.org/core/discussion.html). In particular, the commit list (to see changes as they are made), the dev list (to join discussions of changes) and the user list (to help others).

Please keep discussions about Lucene on list so that everyone benefits. Emailing individual committers with questions about specific Lucene issues is discouraged. See http://people.apache.org/~hossman/#private_q.

## Getting your feet wet: where to begin?

New to Lucene? Want to find JIRA issues that you can work on without taking on the whole world?

The Lucene/Solr developers use the "newdev" label to mark issues that developers new to Lucene might be interested in working on. The rough criteria used to make this selection are:

- Nobody has done any work on the issue yet.
- The issue is likely not controversial.
- The issue is likely self-contained with limited scope.

To see a list of open Lucene issues with the newdev label, look at this link [http://s.apache.org/newdevlucenesolr](https://issues.apache.org/jira/browse/SOLR-15591?jql=(project%3DLUCENE)%20AND%20resolution%3DUnresolved%20AND%20labels%3Dnewdev).

## Developer tips

For more contribution guidelines and tips, see [DeveloperTips](https://cwiki.apache.org/confluence/display/LUCENE/DeveloperTips).
