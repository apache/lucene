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

# GitHub Issues How-To Manual

## Issue labels

There are a few pre-fixed label families to organize/search issues.

- `type` : issue type
  - `type:bug` is attached to bug reports
  - `type:enhancement` is attached to enhancement requests or suggestions
  - `type:test` is attached to test improvements or failure reports
  - `type:task` is attached to general tasks
  - `type:documentation` is attached to tasks relate to documentation
- `fixVersion` : the versions in which a bug or enhancement is planned to be released
  - this may be used for release planning
  - (example) `fixVersion:10.0.0`
- `affectsVersion` : the versions in which a bug was found 
  - this may be used with `type:bug` 
  - (example) `affectsVersion:9.1.0`
- `component` : Lucene components
  - (example) `components:module/core/index`, `component:module/analysis`

A `type` label is automatically attached to an issue by the issue template that the reporter selected. Other labels such as `component` may be manually added by committers.

If necessary, uncategorized labels may also be used.

- `good first issue`
- `discuss`
- `duplicate`

Committers can add/edit labels manually or programmatically using [Labels API](https://docs.github.com/en/rest/issues/labels).

## Issue templates

Each issue template (web form) is associated with one `type` label. You should add an issue template when adding a new `type` label and vice versa.

- `Bug Report` is associated with `type:bug` label
- `Enhancement Request/Suggestion` is associated with `type:enhancement` label
- `Test Improvement / Failure Report` is associated with `type:test` label
- `Task` is associated with `type:task` label
- `Documentation` is associated with `type:documentation` label

Issue templates are written in YAML format and committed in `.github/ISSUE_TEMPLATE`. See [GitHub documentation](https://docs.github.com/en/communities/using-templates-to-encourage-useful-issues-and-pull-requests/syntax-for-issue-forms) for details.