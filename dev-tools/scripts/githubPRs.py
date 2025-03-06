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

"""Simple script that queries GitHub for all open PRs, then finds the ones without
issue number in title, and the ones where the linked JIRA is already closed
"""

import os
import sys

sys.path.append(os.path.dirname(__file__))
import argparse
import json
import re
from typing import TYPE_CHECKING, Any, cast

from github import Github
from jinja2 import BaseLoader, Environment
from jira import JIRA, Issue
from jira.client import ResultList

if TYPE_CHECKING:
  from github.PullRequest import PullRequest


def read_config():
  parser = argparse.ArgumentParser(description="Find open Pull Requests that need attention")
  parser.add_argument("--json", action="store_true", default=False, help="Output as json")
  parser.add_argument("--html", action="store_true", default=False, help="Output as html")
  parser.add_argument("--token", help="Github access token in case you query too often anonymously")
  newconf = parser.parse_args()
  return newconf


def out(text: str):
  global conf
  if not (conf.json or conf.html):
    print(text)


def make_html(dict: dict[Any, Any]):
  global conf
  template = Environment(loader=BaseLoader()).from_string("""
  <h1>Lucene Github PR report</h1>

  <p>Number of open Pull Requests: {{ open_count }}</p>

  <h2>PRs lacking JIRA reference in title ({{ no_jira_count }})</h2>
  <ul>
  {% for pr in no_jira %}
    <li><a href="https://github.com/apache/lucene/pull/{{ pr.number }}">#{{ pr.number }}: {{ pr.created }} {{ pr.title }}</a> ({{ pr.user }})</li>
  {%- endfor %}
  </ul>

  <h2>Open PRs with a resolved JIRA ({{ closed_jira_count }})</h2>
  <ul>
  {% for pr in closed_jira %}
    <li><a href="https://github.com/apache/lucene/pull/{{ pr.pr_number }}">#{{ pr.pr_number }}</a>: <a href="https://issues.apache.org/jira/browse/{{ pr.issue_key }}">{{ pr.status }} {{ pr.resolution_date }} {{ pr.issue_key}}: {{ pr.issue_summary }}</a> ({{ pr.assignee }})</li>
  {%- endfor %}
  </ul>
  """)
  return template.render(dict)


def main():
  global conf
  conf = read_config()
  token = conf.token if conf.token is not None else None
  if token:
    gh = Github(token)
  else:
    gh = Github()
  jira = JIRA("https://issues.apache.org/jira")  # this ctor has broken types in jira library. # pyright: ignore[reportArgumentType]
  result: dict[str, Any] = {}
  repo = gh.get_repo("apache/lucene")
  open_prs = repo.get_pulls(state="open")
  out("Lucene Github PR report")
  out("============================")
  out("Number of open Pull Requests: %s" % open_prs.totalCount)
  result["open_count"] = open_prs.totalCount

  lack_jira = list(filter(lambda x: not re.match(r".*\b(LUCENE)-\d{3,6}\b", x.title), open_prs))
  result["no_jira_count"] = len(lack_jira)
  lack_jira_list: list[dict[str, Any]] = []
  for pr in lack_jira:
    lack_jira_list.append({"title": pr.title, "number": pr.number, "user": pr.user.login, "created": pr.created_at.strftime("%Y-%m-%d")})
  result["no_jira"] = lack_jira_list
  out("\nPRs lacking JIRA reference in title")
  for pr in lack_jira_list:
    out("  #%s: %s %s (%s)" % (pr["number"], pr["created"], pr["title"], pr["user"]))

  out("\nOpen PRs with a resolved JIRA")
  has_jira = list(filter(lambda x: re.match(r".*\b(LUCENE)-\d{3,6}\b", x.title), open_prs))

  issue_ids: list[str] = []
  issue_to_pr: dict[str, PullRequest] = {}
  for pr in has_jira:
    match = re.match(r".*\b((LUCENE)-\d{3,6})\b", pr.title)
    assert match
    jira_issue_str = match.group(1)
    issue_ids.append(jira_issue_str)
    issue_to_pr[jira_issue_str] = pr

  resolved_jiras = cast(ResultList[Issue], jira.search_issues(jql_str="key in (%s) AND status in ('Closed', 'Resolved')" % ", ".join(issue_ids)))
  closed_jiras: list[dict[str, Any]] = []
  for issue in resolved_jiras:
    pr_title = issue_to_pr[issue.key].title
    pr_number = issue_to_pr[issue.key].number
    assignee = issue.fields.assignee.name if issue.fields.assignee else None
    resolution = issue.fields.resolution.name if issue.fields.resolution else None
    closed_jiras.append(
      {
        "issue_key": issue.key,
        "status": issue.fields.status.name,
        "resolution": resolution,
        "resolution_date": issue.fields.resolutiondate[:10],
        "pr_number": pr_number,
        "pr_title": pr_title,
        "issue_summary": issue.fields.summary,
        "assignee": assignee,
      }
    )

  closed_jiras.sort(key=lambda r: r["pr_number"], reverse=True)
  for issue in closed_jiras:
    out("  #%s: %s %s %s: %s (%s)" % (issue["pr_number"], issue["status"], issue["resolution_date"], issue["issue_key"], issue["issue_summary"], issue["assignee"]))
  result["closed_jira_count"] = len(resolved_jiras)
  result["closed_jira"] = closed_jiras

  if conf.json:
    print(json.dumps(result, indent=4))

  if conf.html:
    print(make_html(result))


if __name__ == "__main__":
  try:
    main()
  except KeyboardInterrupt:
    print("\nReceived Ctrl-C, exiting early")
