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

# Build and stage specific Lucene modules in Maven repository layout.
#
# Gradle's maven-publish plugin produces the standard Maven directory
# structure (groupId/artifactId/version/) with JARs, POMs, and checksums.
# The staging directory is then uploaded to S3 by the Evergreen task.
#
# Expected environment variables (set by Evergreen via env):
#   JAVA_HOME           — path to the JDK
#   RELEASE_VERSION     — Maven version string  (e.g. 9.11.1-1)
#   RELEASE_MODULES     — comma-separated list   (e.g. lucene-core,lucene-backward-codecs)
#   MAVEN_STAGING_DIR   — output directory for staged Maven artifacts
set -euo pipefail

export PATH="$JAVA_HOME/bin:$PATH"

if [ -z "${RELEASE_MODULES:-}" ]; then
  echo "ERROR: RELEASE_MODULES is not set. Pass it via Evergreen parameter 'release_modules'."
  exit 1
fi

if [ -z "${RELEASE_VERSION:-}" ]; then
  echo "ERROR: RELEASE_VERSION is not set. Pass it via Evergreen parameter 'release_version'."
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export MAVEN_STAGING_DIR="${MAVEN_STAGING_DIR:?MAVEN_STAGING_DIR is required}"
mkdir -p "$MAVEN_STAGING_DIR"

IFS=',' read -ra modules <<< "$RELEASE_MODULES"
gradle_tasks=()
for module in "${modules[@]}"; do
  if [[ "$module" != lucene-* ]]; then
    echo "ERROR: module '$module' must start with 'lucene-' (check modules.conf)"
    exit 1
  fi
  short="${module#lucene-}"
  if [[ "$short" == analysis-* ]]; then
    gradle_path=":lucene:${short/analysis-/analysis:}"
  else
    gradle_path=":lucene:${short}"
  fi
  gradle_tasks+=("${gradle_path}:publishJarsPublicationToStagingRepository")
done

echo "Publishing modules to staging directory: $MAVEN_STAGING_DIR"
echo "Tasks: ${gradle_tasks[*]}"

./gradlew "${gradle_tasks[@]}" \
  --init-script "$SCRIPT_DIR/s3-staging.init.gradle" \
  -Pversion.release="$RELEASE_VERSION"

find "$MAVEN_STAGING_DIR" -name 'maven-metadata.xml*' -delete

echo ""
echo "Staged artifacts:"
find "$MAVEN_STAGING_DIR" -type f | sort
