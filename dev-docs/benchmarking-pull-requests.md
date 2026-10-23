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

# Benchmarking Pull Requests

The [`run-benchmark-luceneutil`](../.github/workflows/run-benchmark-luceneutil.yml) workflow runs a
[luceneutil](https://github.com/mikemccand/luceneutil) comparison of a pull request against the commit it
branched from, and posts the results on the pull request.

## Requesting a benchmark

Apply one of these labels to the pull request:

| Label | Corpus | Use for |
| --- | --- | --- |
| `benchmark:luceneutil-1m` | first 1M documents of the wikimedium corpus | judging a change |
| `benchmark:luceneutil-10k` | first 10k documents | checking that the benchmark runs; too noisy to judge a change |

Only people who can label pull requests (committers) can start a benchmark. Contributors who want one should
ask a committer. Review the change before applying the label: the pull request's code is built and run on a
GitHub-hosted runner.

A status comment is posted when the run starts and updated with the results when it finishes. A 1M run takes
about 12 minutes and a 10k run about 10, most of it building both commits and running the JVM iterations. The
label stays on the pull request; to benchmark again, for example after new commits, remove the label and apply
it again. A new request on the same pull request cancels a run that is still in progress.

## What is compared

- **Candidate**: the pull request's head commit when the label was applied. Commits pushed after that are not
  included, even if they arrive before the run starts.
- **Baseline**: the merge-base of that commit with the target branch, so only the pull request's own changes
  are measured, not commits that landed on the target branch since.

Both commits are built and benchmarked in the same job, on the same runner. luceneutil builds one index with
the baseline and runs its standard search tasks against it with both commits, alternating between them over
20 fresh JVMs each. The status comment names the exact commits that were measured.

## Reading the results

The comment lists the tasks whose throughput changed significantly (p < 0.05), regressions first, followed by
the full results table in a collapsed section. For each task it shows the mean QPS and standard deviation of
both commits, the change, and the p-value.

Keep in mind:

- GitHub-hosted runners are shared virtual machines, so results are noisier than the
  [nightly benchmarks](https://benchmarks.mikemccandless.com/), which run on dedicated hardware. On a 1M run
  the standard deviation of most tasks is around 10%, so changes of a few percent are hard to detect.
- With 41 tasks, about 2 are expected to pass p < 0.05 by chance. Runs of a change that does not touch any
  code flagged one task per 1M run, a different one each time, and four on a 10k run. A real change shows up
  again when the benchmark is re-run; to re-run it, remove the label and apply it again.
- Only search performance is measured. Both commits search the index built by the baseline, so changes to
  indexing speed or to the index format are not reflected.
- For changes that need a closer look, run luceneutil locally as described in its
  [README](https://github.com/mikemccand/luceneutil/blob/main/README.md).

## Running it manually

The workflow can also be started from the Actions tab (`workflow_dispatch`) with any two refs. Leaving both
empty compares a commit with itself (an A/A run), which is useful for checking how noisy a runner is. The
`attempts` input runs the same comparison as separate jobs on separate runners, e.g. `[1,2,3]`.

## How it works

Two workflows split the work so that pull request code never runs with write access:

| Workflow | Trigger | What it does | Runs pull request code |
| --- | --- | --- | --- |
| [`run-benchmark-luceneutil`](../.github/workflows/run-benchmark-luceneutil.yml) | `pull_request` (labeled), `workflow_dispatch` | Takes the labeled head commit from the event, looks up the merge-base, builds both commits, runs luceneutil, and uploads the log | yes |
| [`report-benchmark-luceneutil`](../.github/workflows/report-benchmark-luceneutil.yml) | `workflow_run` of the benchmark | Posts the status comment, then renders the results and updates the comment | no |

The benchmark runs in the pull request's context, so it has a read-only token and no secrets, and its caches
are scoped to the pull request. The commit to benchmark comes from the label event itself rather than being
looked up when the job runs, so the code that runs is the code that was labeled.

The report workflow runs from the default branch with permission to comment, and treats everything from the
benchmark run as untrusted: it checks that the run's head commit and branch belong to the pull request named in
the run, validates the values in the uploaded artifact, and renders the report with
[`luceneutil_report.py`](../dev-tools/scripts/luceneutil_report.py) from the default branch.

## Maintenance

- **Labels**: the `benchmark:luceneutil-1m` and `benchmark:luceneutil-10k` labels must exist in the repository.
- **Workflow coupling**: the report workflow refers to the benchmark workflow by its `name` and reads the pull
  request number and label from its `run-name`. Change them together.
- **luceneutil version**: the workflow uses a pinned luceneutil commit (`LUCENEUTIL_REF`). Update it to pick
  up new luceneutil tasks or fixes.
- **Corpus**: the corpus slice is checked against `CORPUS_SHA256`. If the corpus file or `CORPUS_LINES`
  changes, update the checksum as well.
