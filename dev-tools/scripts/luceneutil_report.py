#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Formats the results of a luceneutil baseline vs candidate comparison as Markdown.

Used by .github/workflows/run-benchmark-luceneutil.yml to write the job summary and
the pull request comment. It reads the luceneutil log and reports its final results table.
"""

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

SIGNIFICANCE = 0.05
SMOKE_TEST_SOURCES = {"wikimedium10k"}

# One row of the luceneutil results table, for example:
#   MedTerm     11029.12      (3.9%)    11709.36      (5.4%)    6.2% (  -3% -   16%) 0.040
ROW = re.compile(
  r"^\s*(?P<task>[A-Za-z0-9_]+)"
  r"\s+(?P<base_qps>[\d.]+)\s+\(\s*(?P<base_sd>[\d.]+)%\)"
  r"\s+(?P<cand_qps>[\d.]+)\s+\(\s*(?P<cand_sd>[\d.]+)%\)"
  r"\s+(?P<change>-?[\d.]+)%\s+\(\s*-?\d+%\s+-\s+-?\d+%\)"
  r"\s+(?P<p_value>[\d.]+)\s*$"
)


@dataclass(frozen=True)
class TaskResult:
  task: str
  base_qps: float
  base_sd: float
  cand_qps: float
  cand_sd: float
  change: float
  p_value: float

  @property
  def significant(self) -> bool:
    return self.p_value < SIGNIFICANCE


def parse_results(log: str) -> list[TaskResult]:
  """Returns the rows of the last results table; luceneutil prints an updated one after every iteration."""
  lines = log.splitlines()
  headers = [i for i, line in enumerate(lines) if "TaskQPS" in line and "p-value" in line]
  if not headers:
    return []
  results: list[TaskResult] = []
  for line in lines[headers[-1] + 1 :]:
    match = ROW.match(line)
    if match is None:
      break
    results.append(
      TaskResult(
        task=match["task"],
        base_qps=float(match["base_qps"]),
        base_sd=float(match["base_sd"]),
        cand_qps=float(match["cand_qps"]),
        cand_sd=float(match["cand_sd"]),
        change=float(match["change"]),
        p_value=float(match["p_value"]),
      )
    )
  return results


def format_table(results: list[TaskResult]) -> list[str]:
  lines = [
    "| Task | Baseline QPS | Candidate QPS | Change | p-value |",
    "| --- | ---: | ---: | ---: | ---: |",
  ]
  for r in results:
    lines.append(f"| {r.task} | {r.base_qps:.2f} ± {r.base_sd:.1f}% | {r.cand_qps:.2f} ± {r.cand_sd:.1f}% | {r.change:+.1f}% | {r.p_value:.3f} |")
  return lines


def render(results: list[TaskResult], args: argparse.Namespace) -> str:
  lines = [
    f"### luceneutil `{args.source}`: candidate `{args.candidate[:10]}` vs baseline `{args.baseline[:10]}`",
    f"{args.runner}, {args.cpus} CPUs, attempt {args.attempt}",
    "",
  ]
  if not results:
    lines.append("luceneutil did not produce a results table. See the workflow logs.")
    return "\n".join(lines) + "\n"

  # Regressions first, then improvements, each largest first
  significant = sorted((r for r in results if r.significant), key=lambda r: (r.change >= 0, -abs(r.change)))
  if significant:
    lines.append(f"**{len(significant)} of {len(results)} tasks changed significantly (p < {SIGNIFICANCE}):**")
    lines.append("")
    lines.extend(format_table(significant))
    lines.append("")
    lines.append(
      f"With {len(results)} tasks, about {round(len(results) * SIGNIFICANCE)} are expected to pass p < {SIGNIFICANCE} by chance,"
      " even for a change that does not affect performance. Remove and re-apply the label to check whether a result reproduces."
    )
  else:
    lines.append(f"No significant change in any of the {len(results)} tasks (p < {SIGNIFICANCE}).")

  if args.source in SMOKE_TEST_SOURCES:
    lines.append("")
    lines.append(f"> `{args.source}` is a smoke test: its results are too noisy to judge a change. Use the `benchmark:luceneutil-1m` label.")

  lines.append("")
  lines.append(f"<details><summary>All {len(results)} tasks</summary>")
  lines.append("")
  lines.extend(format_table(sorted(results, key=lambda r: r.change)))
  lines.append("")
  lines.append("QPS is the mean ± standard deviation over the JVM iterations.")
  lines.append("")
  lines.append("</details>")
  return "\n".join(lines) + "\n"


def main() -> int:
  parser = argparse.ArgumentParser(description="Format luceneutil results as Markdown")
  parser.add_argument("log", type=Path, help="luceneutil output log")
  parser.add_argument("--output", type=Path, required=True, help="Markdown file to write")
  parser.add_argument("--source", required=True, help="luceneutil source, e.g. wikimedium1m")
  parser.add_argument("--baseline", required=True, help="baseline commit")
  parser.add_argument("--candidate", required=True, help="candidate commit")
  parser.add_argument("--runner", required=True, help="runner label")
  parser.add_argument("--cpus", required=True, help="number of CPUs on the runner")
  parser.add_argument("--attempt", default="1", help="attempt id")
  args = parser.parse_args()

  log = args.log.read_text(encoding="utf-8", errors="replace") if args.log.is_file() else ""
  args.output.write_text(render(parse_results(log), args), encoding="utf-8")
  return 0


if __name__ == "__main__":
  sys.exit(main())
