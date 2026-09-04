This folder holds the validation rules applied to GitHub pull request titles, and
the job that reports on them.

- `validate.js` — the rules themselves: `type(subType): description`.
- `check.js` — reads the title from the workflow event, posts the `PR title` commit
  status, and leaves a comment explaining a rejection. The comment is updated in
  place while the title stays wrong and deleted once it is fixed.
- Tests run with node and need no dependencies: `node ./validate.test.js` and
  `node ./check.test.js`.

Run by [.github/workflows/pr_title.yml](../../.github/workflows/pr_title.yml) on
pull requests and on merge groups. It authenticates with the workflow's own
`GITHUB_TOKEN`, so it needs no bot account and no personal access token.

## Replacing Danger

This used to run [Danger JS](https://danger.systems/js/), which read the title and
reported through the questdb-butler account using a personal access token held in
the `DANGER_GITHUB_TOKEN` secret. Nothing about the job needed a separate identity,
and the token's expiry would have quietly stopped the check. `check.js` does the
same work on the workflow's own token, and `dangerfile.js` and the `yarn global add
danger` step are gone. Once this has settled, `DANGER_GITHUB_TOKEN` can be deleted
from the repository secrets.

The status context is `PR title` rather than the `Danger` that Danger posted. That
rename is safe here because the branch protection on `main` requires only the four
`questdb.java-questdb-client` Azure contexts, so nothing waits on `Danger`. The
copy in questdb/questdb cannot do the same: its `master` ruleset names `Danger`
exactly, and renaming it there without editing the ruleset in the same change
blocks every pull request.

The subType list is deliberately shorter than the server repositories': this is a
client, so `sql`, `wal`, `repl` and the rest are rejected on purpose.
