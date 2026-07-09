---
name: approve-pr
description: Approve the pull request for the currently checked-out branch. Posts the review from this conversation as an approving review and adds the "QUEUED FOR MERGE" label. Use after review-pr when you decide to approve.
allowed-tools: bash read write
metadata:
  argument-hint: "[optional PR number or URL to override auto-detection]"
---

# Approve a QuestDB client pull request

Approve the pull request for the branch that is currently checked out. This skill
is meant to be run **right after `review-pr`** in the same conversation: it takes
the review you just produced and posts it as an approving review. Do not
re-review here — post the existing review.

This repo (`java-questdb-client`) is a submodule of `questdb`. Approving the
client PR does not approve any tandem PR — if the review required a tandem OSS or
Enterprise PR (see the review-pr Step 2.7 gate), that PR must be approved
separately in its own repo.

Actions performed, in order:
1. Post the review as the PR's review body (this is the "PR comment").
2. Approve the PR.
3. Add the `QUEUED FOR MERGE` label (creating it if this repo does not have it yet).

## Step 0: Confirm the review actually clears the bar

Only approve if the most recent `review-pr` report in this conversation has a
verdict of **approve** with, per the review-pr Step 4 gates:
- ZERO open Critical findings, and zero open correctness / concurrency /
  resource findings;
- the **Zero-GC gate** satisfied — no open steady-state (per-row / per-producer-call)
  allocation or reachable algorithmic inefficiency on the ingestion path;
- the **Test & tandem gate** satisfied — no UNTESTED Critical rows in the Step 2.6
  coverage map, and every required tandem PR (OSS e2e / Enterprise / Enterprise
  e2e-python) is present, linked, and running its suite.

If any such finding is still open — including a required-but-missing tandem PR —
do NOT approve. Tell the user the PR does not meet the bar and suggest
`reject-pr`.

- If no review report exists in this conversation, STOP and tell the user to run
  `/skill:review-pr` first — do not approve unreviewed code.
- Post the review verbatim; do not shorten or rewrite it.

## Step 1: Detect the PR

Auto-detect the PR from the checked-out branch. An explicit PR number/URL in the
arguments overrides detection.

```bash
PR='<explicit PR number/URL from arguments, else empty>'
[ -z "$PR" ] && PR=$(gh pr view --json number -q .number 2>/dev/null)
if [ -z "$PR" ]; then
  echo "No PR found for the current branch. Run 'gh pr checkout <n>' or pass a PR number."; exit 1
fi
gh pr view "$PR" --json number,title,author,headRefName,url,state,labels
echo "Approving PR #$PR"
```

State one line to the user: which PR (`#number — title`) you are about to approve,
then proceed.

## Step 2: Write the review body to a file

Write the review body to `/tmp/approve-pr-$PR.md` using the `write` tool (never
inline it into a shell command — reviews contain backticks, quotes, and `$` that
break shell quoting). The body is the full verbatim `review-pr` report.

## Step 3: Post the approval

```bash
gh pr review "$PR" --approve --body-file /tmp/approve-pr-$PR.md

# This repo may not have the label yet — create it idempotently before adding.
if ! gh label list --limit 200 | grep -qx "QUEUED FOR MERGE"; then
  gh label create "QUEUED FOR MERGE" \
    --color f5e70d \
    --description "Approved PR in the merge queue. Do not merge master into this PR." \
    --force
fi
gh pr edit "$PR" --add-label "QUEUED FOR MERGE"
```

Notes:
- GitHub does not allow approving your **own** PR. If `gh pr review --approve`
  fails for that reason, fall back to
  `gh pr comment "$PR" --body-file /tmp/approve-pr-$PR.md`, tell the user the PR
  could not be formally approved because it is self-authored, and still add the
  `QUEUED FOR MERGE` label.

## Step 4: Confirm

Report back in one or two lines:
- PR number + title, "approved" posted (or the fallback used).
- Whether the `QUEUED FOR MERGE` label was added (and whether it had to be created).
- If the review required a tandem PR, remind the user to approve that tandem in its own repo.
- The PR URL.
