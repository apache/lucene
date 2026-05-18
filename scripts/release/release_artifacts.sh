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
  echo "Usage: ${BASH_SOURCE[0]} --version <version>"
  echo
  echo "Creates an Evergreen patch build that builds Lucene modules, signs them"
  echo "with GPG (Garasign), and uploads them to the development S3 bucket."
  echo "Must be run from the release branch."
  echo
  echo "Modules are read from scripts/release/modules.conf. Edit that file to"
  echo "change which modules are published."
  echo
  echo "For CDN (signed) releases, push a git tag matching 'releases/mongot/<version>'"
  echo "instead — Evergreen will automatically build, sign, and upload."
  echo
  echo "Options:"
  echo "  --version   Maven version string in N.N.N-N format (e.g. 10.3.2-1)"
  echo
  echo "Examples:"
  echo "  ${BASH_SOURCE[0]} --version 10.3.2-1"
  echo "  ${BASH_SOURCE[0]} --version 9.11.1-2"
  echo
  echo "Requires the 'evergreen' CLI to be installed and configured."
  exit 1
}

log() {
  local status
  case "$1" in
    info)    shift; status="$(tput setaf 4)==>$(tput sgr0)" ;;
    ok)      shift; status="$(tput bold)[$(tput setaf 2)+$(tput sgr0)$(tput bold)]$(tput sgr0)" ;;
    error)   shift; status="$(tput bold)[$(tput setaf 1)-$(tput sgr0)$(tput bold)]$(tput sgr0)" ;;
    status)
      shift
      echo -n "$(tput setaf 2)==>$(tput sgr0)$(tput bold) "
      echo -n "$@"
      echo "$(tput sgr0)"
      return
      ;;
  esac
  echo "$status $@"
}

# --- Parse arguments ---

version=""

while [ $# -gt 0 ]; do
  case "$1" in
    --version)  version="$2";      shift 2 ;;
    --help|-h)  usage ;;
    *)
      log error "Unknown option: $1"
      usage
      ;;
  esac
done

# --- Validate arguments ---

if [ -z "$version" ]; then
  log error "Missing --version"
  usage
fi

if ! [[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+-[0-9]+$ ]]; then
  log error "Version must be in N.N.N-N format, e.g. 10.3.2-1 (got: ${version})"
  usage
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULES_CONF="${SCRIPT_DIR}/modules.conf"

if [ ! -f "$MODULES_CONF" ]; then
  log error "${MODULES_CONF} not found."
  exit 1
fi

modules_csv=$(grep -v '^\s*#' "$MODULES_CONF" | grep -v '^\s*$' | tr '\n' ',' | sed 's/,$//')
IFS=',' read -ra modules <<< "$modules_csv"

if [ ${#modules[@]} -eq 0 ]; then
  log error "No modules listed in ${MODULES_CONF}."
  exit 1
fi

if ! command -v evergreen &>/dev/null; then
  log error "'evergreen' CLI not found. Install it from https://evergreen.mongodb.com/settings"
  exit 1
fi

branch="$(git branch --show-current)"

# Derive the Evergreen project from the branch name. Release branches must
# target the per-version project (e.g. lucene-mongot-9.11.1) created by
# setup_branch.sh — otherwise the diff between the release branch and main
# is too large for Evergreen to accept.
if [[ "$branch" == "main" ]]; then
  project="lucene-mongot"
elif [[ "$branch" =~ ^mongot_([0-9]+_[0-9]+_[0-9]+)$ ]]; then
  lucene_version="${BASH_REMATCH[1]//_/.}"
  project="lucene-mongot-${lucene_version}"
else
  log error "Expected to be on 'main' or a mongot release branch (mongot_M_m_p), got: ${branch}"
  log error "Create a release branch with: scripts/release/setup_branch.sh <version> <release-tag>"
  exit 1
fi

# --- Summary ---

echo
log status "Release configuration:"
log info "  Version:  ${version}"
log info "  Branch:   ${branch}"
log info "  Modules:  ${modules_csv}"
log info "  Project:  ${project}"
echo

# --- Create Evergreen patch from the current branch ---

log status "Creating Evergreen patch build..."

# --uncommitted sends local working-tree changes so that edits to
# modules.conf or other release scripts are picked up without committing.
evergreen patch \
  --project "$project" \
  --uncommitted \
  --yes \
  --finalize \
  --browse \
  --variants ubuntu2204-large \
  --tasks tests_and_cleanup \
  --tasks publish-dev \
  --param "release_version=${version}" \
  --param "release_modules=${modules_csv}" \
  --description "${branch} - Uploading lucene-mongot artifacts to S3"

log ok "Evergreen patch created."
echo
log status "Next steps:"
log info "  The Evergreen build will publish ${#modules[@]} module(s) to the dev S3 bucket."
log info "  The artifact version will be ${version}-<build_id>."
log info "  Find the full version under the 'Files' tab of the Evergreen task."
echo
log info "  Modules:"
for module in "${modules[@]}"; do
  log info "    ${module}"
done
