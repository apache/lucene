#!/bin/bash

# script inputs, with some defaults.
# date format examples:
#  - now
#  - 3 months ago
#  - yyyy-mm-dd

: "${SINCE:?The SINCE env variable is required (now, 3 months ago, yyyy-mm-dd)}"
: "${UNTIL:=now}"
: "${REPO:=apache/lucene}"

# compute yyyy-mm-dd formats
SINCE_TS=$(date -u -d "$SINCE" +"%Y-%m-%d")
UNTIL_TS=$(date -u -d "$UNTIL" +"%Y-%m-%d")

# https://cli.github.com/manual/gh_help_environment
export GH_FORCE_TTY=160
export NO_COLOR=true

echo "# Repository Activity Report ($REPO)"
echo "Date range covered by this report: $SINCE_TS .. $UNTIL_TS"

echo "## Commits and issue summary:"
echo -n "* The number of commits to the main branch: "
git log main --pretty='format:%h,%as,%an,%s' --since="$SINCE" --before="$UNTIL" | wc -l 

echo -n "* The number of commits to any branch: "
git log --all --pretty='format:%h,%as,%an,%s' --since="$SINCE" --before="$UNTIL" | wc -l 

echo -n "* The number of issues filed: "
gh issue list --state all --search "created:$SINCE_TS..$UNTIL_TS" --repo $REPO --limit 1000 --json id | jq length

echo -n "* The number of closed issues out of those filed: "
gh issue list --state all --search "created:$SINCE_TS..$UNTIL_TS is:closed" --repo $REPO --limit 1000 --json id | jq length

echo -n "* The number of pull requests: "
gh pr list --state all --search "created:$SINCE_TS..$UNTIL_TS" --repo $REPO --limit 1000 --json id | jq length

echo -n "* The number of closed pull requests out of those filed: "
gh pr list --state all --search "created:$SINCE_TS..$UNTIL_TS is:closed" --repo $REPO --limit 1000 --json id | jq length

echo
echo "## Top contributors in the given time period (all commits, any branch)"
echo '```'
git log --all --pretty='format:%an' --since="$SINCE" --before="$UNTIL" | sort | uniq -c | sort -r -n
echo '```'

echo
echo "## Top non-committer contributors in the given time period (all commits, any branch)"
echo '```'
git log --all --pretty='format:%an | %ae' --since="$SINCE" --before="$UNTIL" | sort | uniq -c | sort -r -n | grep -v -f .github/workflows/activity-report-known-committers.txt
echo '```'

echo
echo "## All pull requests:"
echo '```'
gh pr list --state all --search "created:$SINCE_TS..$UNTIL_TS" --repo $REPO --limit 1000 \
  --json number,author,title,createdAt,state \
  --template '{{range .}}{{tablerow (printf "#%v" .number ) .state .title .author.name (timeago .createdAt)}}{{end}}'
echo '```'

echo
echo "## All issues:"
echo '```'
gh issue list --state all --search "created:$SINCE_TS..$UNTIL_TS" --repo $REPO --limit 1000 \
  --json number,author,title,createdAt,state \
  --template '{{range .}}{{tablerow (printf "#%v" .number ) .state .title .author.name (timeago .createdAt)}}{{end}}'
echo '```'

echo
echo "## All commits to the main branch, including hash, author, commit title (one-liners):"
echo '```'
git log main --pretty='format:%h %as %<(20)%an %s' --since="$SINCE_TS" --before="$UNTIL_TS"
echo
echo '```'
