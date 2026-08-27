#!/usr/bin/env bash

set -euo pipefail

echo "📊 Fetching PR data..."
PR_DATA=$(gh pr view "$PR_NUMBER" --json labels,milestone,title)
PR_LABELS=$(echo "$PR_DATA" | jq -r '.labels[].name | select(test("^backport/"; "i"))')
PR_MILESTONE=$(echo "$PR_DATA" | jq -r '.milestone.title // ""')
PR_TITLE=$(echo "$PR_DATA" | jq -r '.title // "Unknown title"')
PR_MERGE_COMMIT_SHA="${PR_MERGE_COMMIT_SHA:-unknown}"
DRY_RUN="${BACKPORT_DRY_RUN:-true}"

normalize_version() {
  local raw="$1"
  local trimmed
  local normalized=""
  trimmed=$(echo "$raw" | sed -E 's/^[[:space:]]+|[[:space:]]+$//g')
  if [[ $trimmed =~ ([0-9]+(\.[0-9]+)*) ]]; then
    normalized="${BASH_REMATCH[1]}"
  fi
  echo "$normalized"
}

is_valid_sha() {
  local sha="$1"
  [[ "$sha" =~ ^[0-9a-f]{7,40}$ ]]
}

create_comment() {
  if [ "$DRY_RUN" = "true" ]; then
    echo "[dry-run] Would comment on PR #$PR_NUMBER:"
    echo "$1"
    return 0
  fi
  gh pr comment "$PR_NUMBER" --body "$1"
}

add_label() {
  if [ "$DRY_RUN" = "true" ]; then
    echo "[dry-run] Would add label '$1' on PR #$PR_NUMBER"
    return 0
  fi
  gh pr edit "$PR_NUMBER" --add-label "$1" 2>/dev/null || true
}

publish_outputs() {
  local has_targets="$1"
  local targets_json="$2"

  if [ -n "${GITHUB_OUTPUT:-}" ]; then
    {
      echo "dry_run=$DRY_RUN"
      echo "has_targets=$has_targets"
      echo "targets<<EOF"
      echo "$targets_json"
      echo "EOF"
    } >> "$GITHUB_OUTPUT"
  fi
}

if [ -z "$PR_LABELS" ] && [ -z "$PR_MILESTONE" ]; then
  echo "ℹ️ No backport labels or milestone found. Nothing to do."
  publish_outputs "false" "[]"
  exit 0
fi

if [ "$DRY_RUN" != "true" ] && ! is_valid_sha "$PR_MERGE_COMMIT_SHA"; then
  create_comment "❌ **Automatic backports skipped**

Invalid merge commit SHA was provided by workflow context: \`$PR_MERGE_COMMIT_SHA\`.

Backports are skipped for safety."
  add_label "backport-failed"
  publish_outputs "false" "[]"
  exit 0
fi

echo "🏷️ Ensuring backport labels exist..."
if [ "$DRY_RUN" = "true" ]; then
  echo "[dry-run] Skipping label creation"
else
  gh label create "backport" --description "Automated backport workflow" --color "0366d6" 2>/dev/null || true
  gh label create "backport-failed" --description "Backport failed" --color "d73a49" 2>/dev/null || true
fi

echo "🌿 Caching branch information..."
ALL_BRANCHES=$(git for-each-ref --format='%(refname:strip=3)' refs/remotes/origin | grep -v '^HEAD$' | sort)
declare -ra TARGET_BRANCH_TEMPLATES=(
  "branch_{{major}}x"
  "branch_{{major}}_{{minor}}"
)

find_target_branch() {
  local version="$1"
  local major
  local minor=""

  major=$(echo "$version" | sed -E 's/^([0-9]+).*/\1/')
  if [[ "$version" =~ ^[0-9]+\.([0-9]+) ]]; then
    minor="${BASH_REMATCH[1]}"
  fi

  for template in "${TARGET_BRANCH_TEMPLATES[@]}"; do
    local candidate="${template//\{\{version\}\}/$version}"
    candidate="${candidate//\{\{major\}\}/$major}"
    candidate="${candidate//\{\{minor\}\}/$minor}"

    local target
    target=$(echo "$ALL_BRANCHES" | grep -Fx "$candidate" | head -1 || true)
    if [ -n "$target" ]; then
      echo "$target"
      return 0
    fi
  done
}

declare -a requested_versions=()
if [ -n "$PR_MILESTONE" ]; then
  normalized_milestone=$(normalize_version "$PR_MILESTONE")
  if [ -n "$normalized_milestone" ]; then
    requested_versions+=("$normalized_milestone")
  fi
fi

while IFS= read -r label; do
  [ -z "$label" ] && continue
  normalized_label=$(normalize_version "${label#*/}")
  if [ -n "$normalized_label" ]; then
    requested_versions+=("$normalized_label")
  fi
done <<< "$PR_LABELS"

declare -a deduped_requested_versions=()
declare -A seen_versions=()
for version in "${requested_versions[@]}"; do
  [ -n "$version" ] || continue
  if [ -z "${seen_versions[$version]:-}" ]; then
    seen_versions[$version]=1
    deduped_requested_versions+=("$version")
  fi
done

echo "📋 Requested versions: ${deduped_requested_versions[*]}"

declare -a valid_backports=()
declare -a failed_versions=()

echo "🔍 Pre-validating target branches..."
for version in "${deduped_requested_versions[@]}"; do
  target=$(find_target_branch "$version")

  if [ -n "$target" ]; then
    valid_backports+=("$version:$target")
    echo "✅ $version -> $target"
  else
    failed_versions+=("$version")
    echo "❌ $version -> NOT FOUND"
  fi
done

if [ ${#failed_versions[@]} -gt 0 ]; then
  available_sample=$(echo "$ALL_BRANCHES" | head -5 | tr '\n' ', ' | sed 's/,$//')
  failed_list=""
  attempted_patterns=""
  for version in "${failed_versions[@]}"; do
    failed_list="${failed_list}- \`${version}\`"$'\n'
    major=$(echo "$version" | sed -E 's/^([0-9]+).*/\1/')
    if [[ "$version" =~ ^[0-9]+\.([0-9]+) ]]; then
      minor="${BASH_REMATCH[1]}"
      attempted_patterns="${attempted_patterns}- \`branch_${major}x\`, \`branch_${major}_${minor}\`"$'\n'
    else
      attempted_patterns="${attempted_patterns}- \`branch_${major}x\`"$'\n'
    fi
  done

  create_comment "❌ **Backport failed for some versions - Missing target branches**

Could not find target branches for:
$failed_list

**Sample available branches:** $available_sample

**Expected patterns:**
- \`branch_{major}x\` (Lucene style)
- \`branch_{major}_{minor}\` (Lucene release branch style)

**Attempted branch names per missing version:**
$attempted_patterns

Please create the missing branches or update the backport labels/milestone.

**Note:** Valid backports will still be processed."
  add_label "backport-failed"
fi

if [ ${#valid_backports[@]} -eq 0 ]; then
  echo "❌ No valid backports found."
  publish_outputs "false" "[]"
  exit 0
fi

targets_json=$(printf '%s\n' "${valid_backports[@]}" \
  | jq -R 'select(length > 0) | split(":") | {version: .[0], target: .[1]}' \
  | jq -cs '.')

echo "✅ Prepared ${#valid_backports[@]} valid backport target(s)."
  if [ "$DRY_RUN" = "true" ]; then
  echo "[dry-run] Planned targets: $targets_json"
  for backport in "${valid_backports[@]}"; do
    IFS=':' read -r version target <<< "$backport"
    backport_branch="backport-${PR_NUMBER}-to-${target}"
    echo "[dry-run] -----------------------------------------------------------------"
    echo "[dry-run] target_branch=${target}, version=${version}, merge_commit=${PR_MERGE_COMMIT_SHA}"
    echo "[dry-run] Would run equivalent git operations:"
    echo "[dry-run]   git checkout -b ${backport_branch} origin/${target}"
    echo "[dry-run]   git cherry-pick -x ${PR_MERGE_COMMIT_SHA}"
    echo "[dry-run]   git push origin ${backport_branch}"
    echo "[dry-run] Would run equivalent PR creation:"
    echo "[dry-run]   gh pr create --base ${target} --head ${backport_branch} --title \"[Backport ${target}] ${PR_TITLE}\" --label backport"
    echo "[dry-run]   PR body payload:"
    echo "[dry-run]     ## 🔄 Automatic Backport"
    echo "[dry-run]     Backport of #${PR_NUMBER} to \`${target}\`."
    echo "[dry-run]     **Target:** \`${target}\`"
    echo "[dry-run]     **Version:** \`${version}\`"
  done
fi

publish_outputs "true" "$targets_json"
