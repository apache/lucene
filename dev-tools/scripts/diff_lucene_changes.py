#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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

import os
import re
import subprocess
import sys
import tempfile
import urllib.request

"""
A simple tool to see diffs between main's version of CHANGES.txt entries for
a given release vs the stable branch's version.  It's best to keep these 1)
identical and 2) matching what changes were actually backported to be honest
to users and avoid future annoying conflicts on backport.
"""

# e.g. python3 -u diff_lucene_changes.py branch_9_9 main 9.9.0

#


def get_changes_url(branch_name):
  if os.path.isdir(branch_name):
    url = f"file://{branch_name}/lucene/CHANGES.txt"
  else:
    url = f"https://raw.githubusercontent.com/apache/lucene/{branch_name}/lucene/CHANGES.txt"
  print(f"NOTE: resolving {branch_name} --> {url}")
  return url


def extract_release_section(changes_txt, release_name):
  match = re.search(f"=======+ Lucene {re.escape(release_name)} =======+(.*?)=======+ Lucene .*? =======+$", changes_txt.decode("utf-8"), re.MULTILINE | re.DOTALL)
  assert match
  return match.group(1).encode("utf-8")


def main():
  if len(sys.argv) < 3 or len(sys.argv) > 5:
    print("\nUsage: python3 -u dev-tools/scripts/diff_lucene_changes.py <branch1-or-local-clone> <branch2-or-local-clone> <release-name> [diff-commandline-extras]\n")
    print('  e.g.: python3 -u dev-tools/scripts/diff_lucene_changes.py branch_9_9 /l/trunk 9.9.0 "-w"\n')
    sys.exit(1)

  branch1 = sys.argv[1]
  branch2 = sys.argv[2]
  release_name = sys.argv[3]

  if len(sys.argv) > 4:
    diff_cl_extras = [sys.argv[4]]
  else:
    diff_cl_extras = []

  branch1_changes = extract_release_section(urllib.request.urlopen(get_changes_url(branch1)).read(), release_name)
  branch2_changes = extract_release_section(urllib.request.urlopen(get_changes_url(branch2)).read(), release_name)

  with tempfile.NamedTemporaryFile() as f1, tempfile.NamedTemporaryFile() as f2:
    f1.write(branch1_changes)
    f2.write(branch2_changes)

    command = ["diff"] + diff_cl_extras + [f1.name, f2.name]

    # diff returns non-zero exit status when there are diffs, so don't pass check=True
    print(subprocess.run(command, check=False, capture_output=True).stdout.decode("utf-8"))


if __name__ == "__main__":
  main()
