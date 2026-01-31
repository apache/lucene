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

## Milestones

We use Milestones for release planning.

A milestone represents a release. Issues and/or PRs should be associated with proper Milestones where the changes are planned to be delivered.

All issues/PRs associated with a milestone must be resolved before the release, which means unresolved issues/PRs in a milestone are blockers for the release. Release managers should consider how to address blockers. Some may be resolved by developers, and others may be postponed to future releases.

Once the release is done, the Milestone should be closed then a new Milestone for the next release should be created.

You can see the list of current active (opened) Milestones here. <https://github.com/apache/lucene/milestones>

See [GitHub documentation](https://docs.github.com/en/issues/using-labels-and-milestones-to-track-work/about-milestones) for more details.

### Relation between Milestones and CHANGES

The Milestone associated with an Issue/PR should be the same version in [CHANGES](https://github.com/apache/lucene/blob/main/lucene/CHANGES.txt). For instance, if an Issue/PR appears in the CHANGES section 10.0.0, it should be marked as Milestone 10.0.0.

## Issue labels

There are a few pre-defined label families to organize/search issues.

- `type` (color code `#ffbb00`) : issue type
    - `type:bug` is attached to bug reports
    - `type:enhancement` is attached to enhancement requests or suggestions
    - `type:test` is attached to test improvements or failure reports
    - `type:task` is attached to general tasks
    - `type:documentation` is attached to tasks relate to documentation
- `affects-version` (color code `#f19072`) : the versions in which a bug was found
    - this may be used with `type:bug`
    - (example) `affects-version:9.1.0`
- `module` (color code `#a0d8ef`) : Lucene module
    - (example) `module:core/index`, `module:analysis`
- `tool` (color code `#a0d8ef`) : tooling
    - (example) `tool:build`, `tool:release-wizard`

A `type` label is automatically attached to an issue by the issue template that the reporter selected. Other labels such as `affects-version` may be manually added by committers.

If necessary, uncategorized labels may also be used.

- `good first issue`
- `discuss`
- `duplicate`
- `website`

Committers can add/edit labels manually or programmatically using [Labels API](https://docs.github.com/en/rest/issues/labels).

## Issue templates

Each issue template (web form) is associated with one `type` label. You should add an issue template when adding a new `type` label and vice versa.

- `Bug Report` is associated with `type:bug` label
- `Enhancement Request/Suggestion` is associated with `type:enhancement` label
- `Test Improvement / Failure Report` is associated with `type:test` label
- `Task` is associated with `type:task` label
- `Documentation` is associated with `type:documentation` label

Issue templates are written in YAML format and committed in `.github/ISSUE_TEMPLATE`. See [GitHub documentation](https://docs.github.com/en/communities/using-templates-to-encourage-useful-issues-and-pull-requests/syntax-for-issue-forms) for details.

## Should I raise an Issue when I already have a working patch?

It's up to you.

From a broader viewpoint, there are no differences between issues and pull requests. You can associate both issues and PRs with Milestones/Labels and mention both issues and PRs in the CHANGES in the very same manner. Sometimes a pull request would be sufficient, and sometimes you may want to open an issue and PRs on it, depending on the context.
