#!/usr/bin/env bash

set -euo pipefail

REPOSITORY="${REPOSITORY:?REPOSITORY must be set}"
DRY_RUN="${BACKPORT_DRY_RUN:-true}"
PUSH_BEFORE_SHA="${PUSH_BEFORE_SHA:-}"
PUSH_AFTER_SHA="${PUSH_AFTER_SHA:-}"

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

create_comment() {
  local pr_number="$1"
  local body="$2"

  if [ "$DRY_RUN" = "true" ]; then
    echo "[dry-run] Would comment on PR #$pr_number:"
    echo "$body"
    return 0
  fi

  gh pr comment "$pr_number" --repo "$REPOSITORY" --body "$body"
}

add_label() {
  local pr_number="$1"
  local label="$2"

  if [ "$DRY_RUN" = "true" ]; then
    echo "[dry-run] Would add label '$label' on PR #$pr_number"
    return 0
  fi

  gh pr edit "$pr_number" --repo "$REPOSITORY" --add-label "$label" 2>/dev/null || true
}

echo "🏷️ Ensuring backport labels exist..."
if [ "$DRY_RUN" = "true" ]; then
  echo "[dry-run] Skipping label creation"
else
  gh label create "backport" --description "Automated backport workflow" --color "0366d6" 2>/dev/null || true
  gh label create "backport-failed" --description "Backport failed" --color "d73a49" 2>/dev/null || true
fi

echo "🌿 Caching branch information..."
ALL_BRANCHES=$(git for-each-ref --format='%(refname:strip=3)' refs/remotes/origin | grep -v '^HEAD$' | sort)

find_target_branch() {
  local version="$1"
  local major
  local minor=""
  local stable_branch
  local release_branch

  major=$(echo "$version" | sed -E 's/^([0-9]+).*/\1/')
  if [[ "$version" =~ ^[0-9]+\.([0-9]+) ]]; then
    minor="${BASH_REMATCH[1]}"
  fi

  stable_branch="branch_${major}x"
  if echo "$ALL_BRANCHES" | grep -Fxq "$stable_branch"; then
    echo "$stable_branch"
    return 0
  fi

  if [ -n "$minor" ]; then
    release_branch="branch_${major}_${minor}"
    if echo "$ALL_BRANCHES" | grep -Fxq "$release_branch"; then
      echo "$release_branch"
      return 0
    fi
  fi
}

resolve_pr_data() {
  local commit_sha="$1"
  gh api -H "Accept: application/vnd.github+json" \
    "repos/${REPOSITORY}/commits/${commit_sha}/pulls" \
    --jq 'map(select(.merged_at != null))[0] // empty'
}

if [ -z "$PUSH_AFTER_SHA" ]; then
  echo "ℹ️ Push payload did not provide an after SHA. Nothing to do."
  publish_outputs "false" "[]"
  exit 0
fi

if [ -n "$PUSH_BEFORE_SHA" ] && [[ ! "$PUSH_BEFORE_SHA" =~ ^0+$ ]]; then
  requested_commit_shas=$(git rev-list --reverse "${PUSH_BEFORE_SHA}..${PUSH_AFTER_SHA}")
else
  requested_commit_shas="$PUSH_AFTER_SHA"
fi

if [ -z "$requested_commit_shas" ]; then
  echo "ℹ️ No commits found in pushed range. Nothing to do."
  publish_outputs "false" "[]"
  exit 0
fi

declare -a targets=()

while IFS= read -r commit_sha; do
  [ -n "$commit_sha" ] || continue

  echo "📊 Resolving merged pull request from commit ${commit_sha}..."
  pr_data=$(resolve_pr_data "$commit_sha")
  if [ -z "$pr_data" ]; then
    echo "ℹ️ No merged pull request associated with commit ${commit_sha}. Skipping."
    continue
  fi

  pr_number=$(echo "$pr_data" | jq -r '.number')
  pr_labels=$(echo "$pr_data" | jq -r '.labels[].name | select(test("^backport/"; "i"))')
  pr_milestone=$(echo "$pr_data" | jq -r '.milestone.title // ""')
  pr_title=$(echo "$pr_data" | jq -r '.title // "Unknown title"')

  if [ -z "$pr_labels" ] && [ -z "$pr_milestone" ]; then
    echo "ℹ️ PR #$pr_number has no backport labels or milestone. Skipping."
    continue
  fi

  declare -a requested_versions=()
  if [ -n "$pr_milestone" ]; then
    normalized_milestone=$(normalize_version "$pr_milestone")
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
  done <<< "$pr_labels"

  declare -a deduped_requested_versions=()
  declare -A seen_versions=()
  for version in "${requested_versions[@]}"; do
    [ -n "$version" ] || continue
    if [ -z "${seen_versions[$version]:-}" ]; then
      seen_versions[$version]=1
      deduped_requested_versions+=("$version")
    fi
  done

  echo "📋 PR #$pr_number requested versions: ${deduped_requested_versions[*]}"

  declare -a failed_versions=()
  for version in "${deduped_requested_versions[@]}"; do
    target_branch=$(find_target_branch "$version")
    if [ -n "$target_branch" ]; then
      echo "✅ PR #$pr_number $version -> $target_branch"
      target_json=$(jq -cn \
        --argjson pr_number "$pr_number" \
        --arg pr_title "$pr_title" \
        --arg merge_commit_sha "$commit_sha" \
        --arg version "$version" \
        --arg target "$target_branch" \
        '{pr_number: $pr_number, pr_title: $pr_title, merge_commit_sha: $merge_commit_sha, version: $version, target: $target}')
      targets+=("$target_json")
    else
      echo "❌ PR #$pr_number $version -> NOT FOUND"
      failed_versions+=("$version")
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

    create_comment "$pr_number" "❌ **Backport failed for some versions - Missing target branches**

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
    add_label "$pr_number" "backport-failed"
  fi
done <<< "$requested_commit_shas"

if [ ${#targets[@]} -eq 0 ]; then
  echo "❌ No valid backports found in this push."
  publish_outputs "false" "[]"
  exit 0
fi

targets_json=$(printf '%s\n' "${targets[@]}" | jq -cs '.')

echo "✅ Prepared ${#targets[@]} valid backport target(s)."
if [ "$DRY_RUN" = "true" ]; then
  echo "[dry-run] Planned targets: $targets_json"
  while IFS= read -r target; do
    [ -n "$target" ] || continue
    pr_number=$(echo "$target" | jq -r '.pr_number')
    pr_title=$(echo "$target" | jq -r '.pr_title')
    merge_commit_sha=$(echo "$target" | jq -r '.merge_commit_sha')
    version=$(echo "$target" | jq -r '.version')
    target_branch=$(echo "$target" | jq -r '.target')
    backport_branch="backport-${pr_number}-to-${target_branch}"

    echo "[dry-run] -----------------------------------------------------------------"
    echo "[dry-run] pr_number=${pr_number}, target_branch=${target_branch}, version=${version}, merge_commit=${merge_commit_sha}"
    echo "[dry-run] Would run equivalent git operations:"
    echo "[dry-run]   git checkout -b ${backport_branch} origin/${target_branch}"
    echo "[dry-run]   git cherry-pick -x ${merge_commit_sha}"
    echo "[dry-run]   git push origin ${backport_branch}"
    echo "[dry-run] Would run equivalent PR creation:"
    echo "[dry-run]   gh pr create --base ${target_branch} --head ${backport_branch} --title \"[Backport ${target_branch}] ${pr_title}\" --label backport"
    echo "[dry-run]   PR body payload:"
    echo "[dry-run]     ## Automatic Backport"
    echo "[dry-run]     Backport of #${pr_number} to \`${target_branch}\`."
    echo "[dry-run]     **Target:** \`${target_branch}\`"
    echo "[dry-run]     **Version:** \`${version}\`"
  done <<< "$(printf '%s\n' "${targets[@]}")"
fi

publish_outputs "true" "$targets_json"
