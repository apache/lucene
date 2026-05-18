#!/usr/bin/env bash
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

set -euo pipefail

traperr() {
  echo "ERROR: ${BASH_SOURCE[1]} at about line ${BASH_LINENO[0]}"
}
set -o errtrace
trap traperr ERR

usage() {
  echo
  echo "Usage: ${BASH_SOURCE[0]} <major.minor.patch> <release-tag>"
  echo "  e.g. ${BASH_SOURCE[0]} 10.3.0 releases/lucene/10.3.0"
  echo
  echo "Creates a development branch (mongot_<M>_<m>_<p>) from a Lucene"
  echo "release tag, commits .evergreen.yml and scripts/release/ from HEAD,"
  echo "pushes the branch, and creates an Evergreen project."
  exit 1
}

log() {
  local status="$(tput setaf 4)==>$(tput sgr0)"
  case "$1" in
    info)
      shift;;
    ok)
      shift; status="$(tput bold)[$(tput setaf 2)+$(tput sgr0)$(tput bold)]$(tput sgr0)"
      ;;
    notok)
      shift; status="$(tput bold)[$(tput setaf 1)-$(tput sgr0)$(tput bold)]$(tput sgr0)"
      ;;
    fatal)
      shift
      echo -n "$(tput bold)[$(tput setaf 1)!!$(tput sgr0)$(tput bold)]$(tput sgr0) "
      echo "$@"
      exit 1
      ;;
    status)
      shift
      echo -n "$(tput setaf 2)==>$(tput sgr0)$(tput bold) "
      echo -n "$@"
      echo $(tput sgr0)
      return
      ;;
  esac

  echo "$status $@"
}

EVERGREEN_API="https://evergreen.mongodb.com/rest/v2"

# Set desired version and release tag from command line arguments.
if [ -z "$1" ]; then
  log notok "Missing version (major.minor.patch)!"
  usage
fi
if ! [[ "$1" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  log notok "Version must be in major.minor.patch format (e.g. 10.3.0)!"
  usage
fi
if [ -z "$2" ]; then
  log notok "Missing release tag!"
  usage
fi
version=$1
release_tag=$2

if ! git rev-parse --verify "${release_tag}^{}" >/dev/null 2>&1; then
  log fatal "Tag '${release_tag}' not found. Run 'git fetch --tags' first."
fi

# Set evergreen credentials from ~/.evergreen.yml.
log status "Checking for evergreen credentials..."
evergreen_user=$(grep "^user: " $HOME/.evergreen.yml | cut -c7-)
evergreen_api_key=$(grep "^api_key: " $HOME/.evergreen.yml | cut -c10-)
if [ -z "$evergreen_user" ] || [ -z "$evergreen_api_key" ]; then
  log error "Missing evergreen credentials. Please set up the evergreen CLI!"
  exit 1
else
  log ok "Detected evergreen user '${evergreen_user}'"
fi

project_name="lucene-mongot-${version}"
branch_name="mongot_${version//\./_}"
echo
log status "Creating evergreen project '${project_name}'..."

# Create the evergreen project if it doesn't exist.
if curl -fs -H "Api-User: $evergreen_user" -H "Api-Key: $evergreen_api_key" \
    $EVERGREEN_API/projects/$project_name >/dev/null; then
  log ok "Evergreen project already exists, skipping creation."
else
  copy_response=$(curl -fsSL -H "Api-User: $evergreen_user" -H "Api-Key: $evergreen_api_key" \
     -X POST "$EVERGREEN_API/projects/lucene-mongot/copy?new_project=$project_name")
  copy_exitcode=$?

  if [ $copy_exitcode -ne 0 ]; then
    echo $copy_response
    log fatal "Could not create evergreen project!"
  fi
fi

# Configure the project branch and display name, and enable it.
log "Configuring evergreen project..."
configure_response=$(curl -fsSL -H "Api-User: $evergreen_user" -H "Api-Key: $evergreen_api_key" \
    -H "Content-Type: application/json" -d @- \
    -X PATCH $EVERGREEN_API/projects/$project_name <<EOF
{
  "branch_name": "${branch_name}",
  "display_name": "${project_name}",
  "enabled": true,
  "pr_testing_enabled": null,
  "manual_pr_testing_enabled": null
}
EOF
)
configure_exitcode=$?

if [ $configure_exitcode -ne 0 ]; then
  echo $configure_response
  log fatal "Could not configure evergreen project!"
else
  log ok "Evergreen project enabled!"
fi

# Create the git branch if it doesn't already exist.
echo
log status "Creating git branch '${branch_name}'..."
if git show-ref --quiet "refs/heads/${branch_name}"; then
  log ok "Branch already exists, skipping create."
else
  # Build a commit that layers .evergreen.yml and scripts/release/ from the
  # current branch onto the release tag. Uses a temporary index to handle
  # subdirectories without checking out the branch.
  tmp_index=$(mktemp)

  GIT_INDEX_FILE="$tmp_index" git read-tree "${release_tag}^{tree}"

  evg_blob=$(git rev-parse "HEAD:.evergreen.yml")
  GIT_INDEX_FILE="$tmp_index" git update-index --add \
    --cacheinfo 100644,"${evg_blob}",".evergreen.yml"

  while IFS=$'\t' read -r meta path; do
    mode=$(echo "$meta" | cut -d' ' -f1)
    blob=$(echo "$meta" | cut -d' ' -f3)
    GIT_INDEX_FILE="$tmp_index" git update-index --add \
      --cacheinfo "${mode},${blob},${path}"
  done < <(git ls-tree -r HEAD -- scripts/release/)

  new_tree=$(GIT_INDEX_FILE="$tmp_index" git write-tree)

  # -S requires GPG signing to be configured in git (gpg key + user.signingkey).
  new_commit=$(git commit-tree -S "$new_tree" \
    -p "$(git rev-parse "${release_tag}^{}")" \
    -m "[mongot setup] Add .evergreen.yml and scripts/release/ for Evergreen CI")
  git branch "$branch_name" "$new_commit"
  rm -f "$tmp_index"
fi

log "Pushing '${branch_name}' to origin..."
git push -u origin "$branch_name"

echo
log ok "Success! See project waterfall at https://spruce.mongodb.com/commits/$project_name"
