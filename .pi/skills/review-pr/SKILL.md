---
name: review-pr
description: Review a GitHub pull request against QuestDB coding standards. Use when asked to review a PR (by number or URL), optionally with a depth level 0..3. Performs an adversarial, blocking, mission-critical code review covering correctness, concurrency, zero-GC performance, resource management, cross-repo tandem test coverage (OSS / Enterprise / e2e-python), test efficacy, test-code quality, and QuestDB conventions, then verifies every finding against source before reporting.
allowed-tools: bash read subagent
---

# Review PR

Review the pull request identified by the user's arguments. The arguments (a PR number or URL, optionally with a level token) are appended to this skill as `User: <args>`, or appear in the user's request. Treat that text as `$ARGUMENTS` below.

## Tooling note (pi)

This skill was ported from a Claude Code skill. Map the tools as follows:
- Reading files → the `read` tool.
- Searching the repo (the Claude `Grep`/`Glob` steps) → the `bash` tool with `rg`, `grep`, `find`, `ls`. Every "use Grep/Glob" instruction below means "run a real `rg`/`find` search and show it" — do not reason about callsites from memory.
- `gh` commands → the `bash` tool.
- Spawning parallel review "agents" → the `subagent` tool. Launch builtin `reviewer` agents with `context: "fresh"` so each works adversarially from the repo and diff directly, not from this conversation. Example: `subagent({ tasks: [ { agent: "reviewer", task: "..." }, ... ], context: "fresh", concurrency: N })`. Verification passes (Step 3b) are likewise `reviewer` agents launched in parallel where findings are independent. The parent session stays the single decision-maker and writes the final report; reviewers are review-only (do not edit project files).

## Review mindset

You are a senior QuestDB engineer performing a blocking code review. QuestDB is mission-critical software deployed on spacecraft — bugs can cause data loss or system failures that cannot be patched after deployment. There is zero tolerance for correctness issues, resource leaks, or undefined behavior. Be critical, thorough, and opinionated. Your job is to catch problems before they ship, not to be nice.

- **Assume nothing is correct until you've verified it.** Read surrounding code to understand context — don't just look at the diff in isolation.
- **The diff is a hint, not the boundary of the review.** The highest-value bugs almost always live at callsites outside the diff that depend on contracts the diff quietly changed. Treat the diff as the entry point, not the scope.
- **Flag every issue you find**, no matter how small. Do not soften language or hedge. Say "this is wrong" not "this might be an issue".
- **Do not praise the code.** Skip "looks good", "nice work", "clever approach". Focus entirely on problems and risks.
- **Think adversarially.** For each change, ask: what inputs break this? What happens under concurrent access? What if this runs on a 10-billion-row table? What if the column is NULL? What if the partition is empty?
- **Zero-GC is not optional — this is a client on the ingestion hot path.** Every producer call (`table`, `symbol`, `column`, `at`, `flush`) runs per row, millions of times a second. A single object allocation on that path is a defect: it feeds GC pressure that stalls the caller's application and defeats the purpose of a high-throughput ingestion client. Treat any per-row / per-call allocation — `new`, autoboxing, capturing lambda, `java.util.*` structure, `String`/substring/concat, fresh iterator — as a blocking finding unless it is provably one-off (construction, compile-time, or config parse), not steady-state. "Endless object allocation" is never acceptable; demand buffer reuse and `io.questdb.std` primitives.
- **Check what's missing**, not just what's there. Missing tests, missing error handling, missing edge cases, missing documentation for non-obvious behavior.
- **Untested code is broken code — but the test may live in another repo.** This client repo has only unit tests; it cannot run e2e against a real server. Real-server, failover, and process-kill coverage lives in tandem PRs in `questdb` (OSS) and `questdb-enterprise` (see Step 2.7). Treat any new or changed behavior that ships without a test proving it — here OR in a linked tandem PR — as a defect. "The change is simple" and "existing tests probably cover it" are not evidence; a named test (local or tandem), located by a recorded search or an explicit tandem-PR link, with a stated failure link, is.
- **A missing required tandem PR is a red flag, not a convenience.** When a change needs server-integration or HA coverage that can only exist in another repo, the absence of the matching tandem PR means the change is effectively untested. Do not wave it through — scrutinise it far more thoroughly (Step 2.7) and treat the missing coverage as blocking.
- **Verify every claim.** If the PR title says "fix", verify the bug actually existed and the fix is correct. If it says "improve performance", look for benchmarks or reason about the algorithmic change — does it actually improve things, or could it regress in other cases? If it says "simplify", verify the new code is actually simpler and doesn't drop behavior. Treat the PR description as an unverified hypothesis, not a statement of fact.
- **Read the full context of changed files** when the diff alone is ambiguous. Use `read` and `bash` (rg/grep/find) to inspect the surrounding code, callers, and related tests.
- **Assess reachability before reporting.** For every potential bug, trace the actual callers and inputs. If a problem
  requires physically impossible conditions (billions of columns, corrupted JNI inputs, values that no caller can
  produce), it is not a real finding — drop it. Focus on bugs that real workloads can trigger, not theoretical edge
  cases that exist only in the type system.
- **QuestDB runs with Java assertions enabled (`-ea`).** Assertions are a valid guard for invariants that indicate
  corruption or internal bugs. Do NOT flag `assert` as insufficient — it is the preferred mechanism for conditions
  that should never occur in a non-corrupt database. Only flag an `assert` if the condition can plausibly be triggered
  by normal (non-corrupt) user operations.

## Review level

Parse `$ARGUMENTS` for a level token: `--level=N`, `-lN`, or a bare single digit `0`-`3`. **If no level is given, default to 0.** Strip the level token before feeding the remainder (PR number or URL) to `gh` commands.

The level controls how much of the review below actually runs. Lower levels keep the same review *spirit* — adversarial, blocking, no praise — but cut the breadth of the analysis. Higher levels have significantly higher token cost; reserve level 3 for high-stakes PRs (replication, JNI boundary changes, on-disk format, public API, security/ACL).

| Level | What runs |
|-------|-----------|
| **0 (default)** | Steps 1, 2, 2.6, 2.7, 4. Skip Step 2.5. Skip Step 3 — no subagent spawn; review the diff inline in the main loop, using `read`/`bash` searches on demand to resolve ambiguities. Skip Step 3b — verify each finding inline as you write it. Single-pass review covering correctness, NULL handling, **zero-GC / allocation discipline**, test coverage, and QuestDB standards on the diff itself. Step 2.6 (cross-repo test coverage map) and Step 2.7 (tandem-PR gate) are mandatory here as at every level — build the coverage map inline before writing findings, deriving behavioral-change rows directly from the diff since 2.5a is skipped. When the diff touches test code, also apply the test-efficacy and test-code-quality anti-pattern checks inline (vacuous assertions, reflection overuse, whitebox coupling to internals, reinvented helpers, javadoc bloat). |
| **1** | Adds Step 2.5a (semantic delta only — skip 2.5b/2.5c/2.5d) plus Step 2.5e when test code is present. In Step 3, launch reviewer 1 (correctness), reviewer 3 (performance & zero-GC), reviewer 5 (test coverage), reviewer 6 (code quality), and — when the diff touches test code — reviewer 12 (test efficacy) and reviewer 13 (test-code quality) in parallel. Skip all other reviewers. Skip Step 3b — verify findings inline as you draft the report. |
| **2** | Full Step 2.5 (including 2.5e when test code is present), but in 2.5b restrict the callsite inventory to `public`/`protected` symbols (skip package-private and `pub(crate)`). In Step 3, launch reviewers 1-8 (reviewer 8 only if `.rs` files are present), plus reviewer 11 (adversarial performance & zero-GC), plus reviewers 12 and 13 when the diff touches test code. Skip reviewer 9 (cross-context), reviewer 10 (adversarial fresh-context), and reviewer 14 (regression-test efficacy verification). Step 3b uses a single batched verification reviewer for all findings instead of one per finding. |
| **3** | Every step below as written, all 14 reviewers, per-finding verification. The full mission-critical pass. |

State the chosen level in one line at the start of the review so the user knows what they're getting (e.g., "Reviewing PR #1234 at level 2"). If the level was defaulted, mention that level 3 exists for full review.

## Step 1: Gather PR context

Capture the PR identifier in `$PR` (the part of `$ARGUMENTS` left after stripping the level token), then fetch metadata, diff, and review comments in a single bash call so `$PR` is in scope for all three `gh` invocations:

```bash
PR='<PR number or URL from $ARGUMENTS, with any --level=N / -lN / bare-digit level token removed>'
gh pr view "$PR" --json number,title,body,labels,state
gh pr diff "$PR"
gh pr diff "$PR" --numstat   # binary files show as `-<TAB>-<TAB><path>`
gh pr view "$PR" --comments
```

**Committed-binary gate (runs at every level).** Scan the `--numstat` output for
any added/modified file git reports as binary (`-`/`-` in the added/deleted
columns). This repo builds its native/C libraries from source in CI and does not
commit build outputs, so any such file is a **Critical** finding regardless of
review level — report it even at level 0. See the "Committed build artifacts"
checklist for the rationale and the acceptable-exception (genuine test-input
fixtures only).

## Step 2: PR title and description

Check against CLAUDE.md conventions:
- Title follows Conventional Commits: `type(scope): description`
- Description repeats the verb (e.g., `fix(sql): fix ...` not `fix(sql): DECIMAL ...`)
- Description speaks to end-user impact, not implementation internals
- If fixing an issue, `Fixes #NNN` is at the top of the body
- Tone is level-headed and analytical, no superlatives or bold emphasis on numbers
- Labels match the PR scope (SQL, Performance, Core, etc.)

## Step 2.5: Map the change surface

Before launching review reviewers, produce a structured change surface map. This step is mandatory and must use real `rg`/`find` searches via `bash` — do not reason about callsites from memory. The output of this step is required input for every reviewer in Step 3.

### 2.5a Semantic delta per changed symbol

For every modified or added function, method, trait, struct field, SQL operator/function, or public constant, write:

- **Symbol:** fully-qualified name
- **Before:** signature, return type, error/exception behavior, panic behavior, mutation (`&self` vs `&mut self`, `final` vs not), ordering/idempotency guarantees, allocation behavior, thread-safety
- **After:** same fields
- **Delta:** one line stating what semantically changed

"Refactored", "cleaned up", "improved", "simplified" are not acceptable deltas. State the actual behavioral difference. If nothing semantically changed, write "no behavioral change" — but only after checking, not as a default.

### 2.5b Callsite inventory

For every changed symbol that is `public`, `protected`, package-private, or exported (`pub` / `pub(crate)` in Rust), run `rg` across the entire repository to find every callsite, implementation, override, or reference outside the diff.

Produce a list grouped by file. For Java, also search for:
- subclasses that override the method
- interfaces that declare it
- reflection-based callers (`getMethod`, `getDeclaredField`, `Class.forName`)
- SQL function/operator registrations (`FunctionFactory`, `OperatorRegistry`)
- service loader entries

For Rust, also search for:
- trait impls
- macro expansions
- JNI exports and their Java callers
- `extern "C"` boundaries

A changed `pub`/`protected`/package-private symbol with zero recorded `rg` calls in the trace is a skill violation. The model is not allowed to assert "this is only used here" without showing the search.

### 2.5c Implicit contract list

For each changed symbol, walk this checklist and write one line per item, stating before vs after:

- Panics or throws on which inputs
- Error variants returned and which `?`/`throws` chains propagate them
- Iteration order, sort stability, NULL ordering
- Idempotency and re-entrancy
- Lock acquisition order and which locks are held on return
- Allocation on hot vs compile-time path
- `Send`/`Sync`, thread-affinity, JFR/JNI thread attachment requirements
- Whether `null` and sentinel-NULL (`Numbers.LONG_NULL`, `Numbers.INT_NULL`, etc.) are still distinguished
- Cancellation/drop behavior (Rust) and finally/close behavior (Java)
- SQL: does the symbol now appear in new clauses (WHERE, GROUP BY, JOIN ON, ORDER BY, window frames, partition predicates, materialized view definitions) where it didn't before? List which.

### 2.5d Cross-context exposure list

End this step with an explicit list of "places this change is visible from but the diff does not touch". This is the highest-priority input for the bug-hunting reviewers in Step 3.

The list groups the callsites from 2.5b by execution context: hot data paths, SQL compilation, async runtime, JNI boundary, replication, materialized views, parallel execution workers, etc. Every entry on this list must be reviewed in Step 3.

### 2.5e Test surface & helper inventory

Run this only when the PR adds or changes test code. It is the test-code counterpart to 2.5b and feeds Reviewers 12-14. Use real `rg`/`find` searches via `bash` — do not reason about helpers from memory.

- **Existing-infrastructure inventory:** search the changed test files' package and module for base test classes, shared `@Before`/`@After`, helper methods, fixtures, and assertion utilities the new tests could reuse (`rg` for `extends Abstract.*Test`, `class .*TestUtils`, `assertMemoryLeak`, `assertQuery`, `assertSql`, shared `protected` helpers in the base class). This list is the baseline Reviewer 13 uses to flag reinvented boilerplate — a "you stamped boilerplate instead of reusing helper X" finding requires X to appear in this inventory.
- **Changed shared helpers as symbols:** if the PR changes a shared test base class, helper, or fixture, run the 2.5b callsite inventory for it too — a changed test base class can silently break every subclassing test.
- **Exercised-symbol map:** for each new or changed test, list which production symbols from 2.5a it actually exercises, so Reviewers 12 and 14 can check efficacy and regression value.
- **Tandem coverage note:** for behavior that can only be proven against a real server (see Step 2.7), the exercising test may live in a tandem OSS/Enterprise PR rather than in this repo. Record the tandem test class/method name (from the linked PR) here so the coverage map (Step 2.6) can cite it.

## Step 2.6: Cross-repo test coverage map (mandatory at every level)

This step runs at EVERY review level, for EVERY PR that touches production code — including (especially) PRs that add or change no test code at all. A PR with zero test changes does not skip test scrutiny; it concentrates it here. Because this repo has only unit tests, a row's covering test may be **local** (a unit test in this repo) or **tandem** (a test in a linked `questdb`/`questdb-enterprise` PR — see Step 2.7). A behavioral change with neither is UNTESTED. At level 0, derive the behavioral-change rows directly from the diff (2.5a is skipped); at level 1+ use the 2.5a semantic deltas.

Build a coverage table with one row per behavioral change: every changed symbol whose delta is not "no behavioral change", broken down by every new or changed branch, error path, and NULL/boundary case inside it. For each row, record:

- **Change:** symbol + the specific behavior/branch/path.
- **Test (local or tandem):** the exact test class and method that exercises it — found via a real `rg`/`find` search in this repo, OR named from a linked tandem PR (cite the repo + PR link + test name from Step 2.7). Citing a test without a recorded search command or a tandem-PR link is a skill violation, same as 2.5b. "Existing tests probably cover it" is banned.
- **Failure link:** one line stating what that test asserts and why the assertion fails if this specific change regresses. "The test calls the method" is not a failure link — the assertion must observe the changed behavior.
- **Dimensions:** which applicable dimensions are covered, each marked covered / uncovered / N-A: happy path, error/exception path, NULL inputs/results, boundaries (empty buffer, single row, max-length symbol/column, buffer-full), concurrency (if shared state is touched), resource cleanup / `assertMemoryLeak()` (if native memory is allocated), **and — where the behavior is only observable end-to-end — real-server / failover / process-kill (only provable via a tandem PR, Step 2.7)**. An N-A mark requires a one-line reason; an unexplained N-A counts as uncovered.

Rows with no test (local or tandem), or with a test that has no plausible failure link, are marked **UNTESTED** and carry a default severity:

- **Critical (blocking):** UNTESTED new or changed user-visible behavior (wire/protocol output, `Sender`/config API, config key), UNTESTED bug fix — **a fix PR with no regression test is automatically Critical, no reviewer analysis needed** — UNTESTED error/exception path introduced by the change, UNTESTED concurrency-sensitive change, UNTESTED native-memory or resource-lifecycle change, AND any behavior that requires real-server/HA/process-kill proof but has no tandem PR (Step 2.7).
- **Moderate:** UNTESTED internal branch that is reachable but hard to trigger in isolation — the finding must name what a test would need to do to reach it.
- **Exempt:** only rows whose delta is verified "no behavioral change" (pure rename, dead-code removal, comment/doc/CI-only). The exemption must be stated per row, citing the verified delta. "Refactor" claimed by the PR description is not an exemption — only a verified no-behavioral-change delta is.

The coverage map is required input for Reviewer 5, Reviewers 12-14, and the Step 4 verdict. Every UNTESTED row must surface as a finding in the Step 4 report under its severity section, and the full map must be rendered in the report's "Coverage map" section. At level 0, rows may be kept per-symbol to bound cost, but new error/exception paths and NULL/boundary handling introduced by the change must still get their own rows.

## Step 2.7: Cross-repo tandem-PR testing gate (mandatory at every level)

This repo is a Git submodule of `questdb` (OSS) and ships as `io.questdb:questdb-client`. **It has no capability to run e2e tests against a real server — its own CI runs unit tests only.** Anything that can only be proven with a running server, a failover, or a killed process is tested in another repo, via a **tandem PR**. Your job in this step is to determine which tandem(s) this change requires, verify they exist and are linked, and treat any missing required tandem as blocking.

### The testing topology (where coverage lives)

| Coverage kind | Repo | Notes |
|---|---|---|
| Client unit tests | **this repo** (`java-questdb-client`) | Logic, buffer/encoding, config parsing, state machines with mocked/loopback transport. The only tests this repo's CI runs. |
| Real-server e2e (non-HA) | **`questdb` (OSS)** | Client driven against a real single-node server. Requires a **tandem OSS PR** with a **matching branch name**; the two PR descriptions must cross-link. The OSS repo pulls this client as a submodule, so its CI exercises the tandem automatically. |
| High-availability features (failover, replication, store-and-forward across a switch, role gating) | **`questdb-enterprise`** | Any change that *is* an HA feature or *touches* HA-facing code needs a **tandem Enterprise PR**. |
| HA scenarios needing a process killed with SIGKILL (`kill -9` of client or server) | **`questdb-enterprise` e2e python infrastructure** | Crash/kill-recovery, store-and-forward-across-crash, hard-failover behavior. These live ONLY in the Enterprise python e2e suite — they cannot be expressed as JVM unit tests. |

### Which tandem does THIS change require?

Classify the diff against these triggers (a PR can trip more than one):

- **Needs an OSS e2e tandem** — the change alters anything observable only against a real server: wire/ILP or QWP protocol bytes, handshake/auth/upgrade, TLS, HTTP transport, flush/retry/reconnect semantics, server-error handling, config that changes on-the-wire behavior. Pure in-JVM logic with no server-observable effect does not.
- **Needs an Enterprise tandem** — the change is, or touches, a high-availability feature: the store-and-forward drainer, primary reconnect/failover, `SenderPool`/`QueryClientPool` startup, `lazy_connect`/`initial_connect_retry`, replication/role-gating interactions. When in doubt whether something is "HA", it is — require the Enterprise tandem.
- **Needs an Enterprise e2e-python (kill) tandem** — correctness depends on surviving a `kill -9` of the client or the server (crash recovery, store-and-forward across a crash, hard failover, durable-ack replay after a kill). A JVM-level test cannot kill the process mid-flight, so this coverage is only real in the python suite.
- **Unit-testable-only** — buffer formatting, encoding, parsing, config validation, and state machines drivable with a mock/loopback transport. These need a **local** test here and no tandem.

### Verify the tandem exists and is linked (record the commands)

Do not assume — check, and show the search (a claim of "tandem exists" with no recorded command is a skill violation):

```bash
HEAD=$(gh pr view "$PR" --json headRefName -q .headRefName)   # matching-branch convention
gh pr view "$PR" --json body -q .body | rg -i 'questdb/questdb(-enterprise)?(/pull/|#)|tandem|e2e'  # links in the description
gh pr list --repo questdb/questdb --head "$HEAD" --state all            # OSS tandem by matching branch
gh pr list --repo questdb/questdb-enterprise --head "$HEAD" --state all  # Enterprise tandem by matching branch
```

Also confirm the tandem link is bidirectional (both descriptions reference each other) and that the tandem PR's CI actually runs the relevant suite (the OSS submodule bump / the Enterprise python e2e job). A tandem PR that exists but does not run the covering suite does not count.

### Disposition

- **Required tandem present, linked, and running the suite:** cite its repo + PR link + the covering test in the Step 2.6 coverage map as the row's *tandem* test.
- **Required tandem missing (or present but not linked / not running the suite):** this is a **Critical, blocking** finding. State exactly which tandem is required (OSS e2e / Enterprise / Enterprise-python-kill), why (the trigger above), and what test it must add. Then **escalate scrutiny**: every behavioral change that would have been covered by that tandem is now UNTESTED in the Step 2.6 map and inherits Critical severity — review the diff assuming no integration safety net exists.
- **`gh` cannot reach the Enterprise repo (permissions):** say so explicitly, fall back to verifying the description link and matching-branch reference, and still require the author to name the tandem PR. Do not silently pass the gate.

The outcome of this step (required tandems, their status) is required input for Reviewer 5 and the Step 4 test gate.

## Step 3: Parallel review

Launch the reviewers below with the `subagent` tool in `context: "fresh"` mode, in parallel (`subagent({ tasks: [...], context: "fresh", concurrency: N })`). Every reviewer task must include:
1. The PR diff
2. The full change surface map from Step 2.5 (semantic deltas, callsite inventory, implicit contracts, cross-context exposure list)
3. The cross-repo test coverage map from Step 2.6 and the required-tandem status from Step 2.7

(Exception: reviewer 10 receives only the diff and changed file names; reviewer 11 receives the diff plus the full source of the touched files. Neither receives the Step 2.5/2.6 maps or the checklists — see their entries below.)

### Anti-anchoring directive (applies to all reviewers)

- **Bugs at callsites outside the diff outrank bugs inside the diff.** A confirmed bug in a file the PR did not touch but that calls a changed symbol is a P0 finding.
- **"Looks correct in isolation" is not a valid conclusion.** Before clearing a changed symbol, the reviewer must walk the callsite inventory from 2.5b and explicitly state, per callsite, whether the new behavior is still correct there.
- **The diff is the entry point, not the scope.** If the change surface map shows the symbol is reachable from N other files, the review covers N+1 files.
- A single finding of the form "in `FooReader.java` the new behavior of `Bar.x()` causes Y" is worth more than five findings inside the diff.

### Reviewers

Launch the following reviewers in parallel.

**Reviewer 1 — Correctness & bugs:** NULL handling, edge cases, logic errors, off-by-one, operator precedence, error paths. Cross-reference every changed symbol against its callsite inventory and verify the new behavior is correct at each callsite. When the diff touches the store-and-forward sender, the async drainer / send loop, primary reconnect/failover, or pool startup (`lazy_connect` / `initial_connect_retry` / `SenderPool` / `QueryClientPool`), also verify the "Store-and-forward & pool startup invariants" checklist — a running drainer that propagates a transport error to the caller, imposes a reconnect time budget, or hard-fails on a transient outage is a Critical (data-loss) finding.

**Reviewer 2 — Concurrency:** Race conditions, shared mutable state, missing volatile, lock ordering, thread-safety of data structures. Use the implicit contract list (lock order, thread-affinity) and check every callsite from 2.5b for violations of the new contract.

**Reviewer 3 — Performance, allocations & zero-GC:** This is an ingestion client on the caller's hot path — **zero-GC is the governing rule, and a single steady-state allocation is a blocking finding.** For every changed method reachable from a producer call (`table`/`symbol`/`column`/`at`/`flush`) or any per-row/per-batch loop, hunt allocations: `new` objects, autoboxing (`int`→`Integer`, primitives into generics or `java.util.*`), capturing lambdas (a lambda closing over locals/fields allocates per call — static/non-capturing ones are fine), `String`/substring/concat/`String.format`, boxed iterators, and any `java.util.*` collection where an `io.questdb.std` equivalent exists (`ObjList`, `IntList`, `CharSequenceObjHashMap`, `DirectUtf8Sink`, etc.). Demand buffer reuse over reallocation and `CharSink`/`Utf8Sink` over `+`. Algorithmic complexity: for each new loop/traversal/data structure, state how it scales (rows, columns, buffer size) and flag O(n^2)-or-worse. Distinguish compile-time / construction / config-parse allocations (acceptable) from steady-state per-row ones (never acceptable). For changed symbols now reachable from new contexts (per 2.5d), check whether any new context is a per-row path that turns an otherwise-fine allocation into a hot-path leak.

**Reviewer 4 — Resource management:** Leaks on all code paths (especially errors), try-with-resources, native memory, pool management. Walk every callsite from 2.5b that constructs, owns, or transfers ownership of changed types and verify cleanup on all paths.

**Reviewer 5 — Test coverage:** Coverage gaps, error path tests, NULL tests, boundary conditions, regression tests exist, `assertMemoryLeak()` usage. Cross-reference 2.5d: every cross-context exposure should have a test that exercises the changed symbol from that context. Missing tests for cross-context callsites is a high-priority finding. Test *efficacy* (whether those tests actually exercise the change and could fail) and test-*code* quality are handled by Reviewers 12-14 — here focus only on whether coverage exists for every new or changed path. Also consume the Step 2.6 coverage map and the Step 2.7 tandem status: every UNTESTED row and every required-but-missing tandem is a coverage finding here.

**Reviewer 6 — Code quality & standards:** Code smell, member ordering, naming conventions, modern Java features, dead code, third-party dependencies. Also scan the diff for any committed compiled binary / build artifact (run `git diff --numstat`/`--stat` and flag files git reports as binary) — the native/C libraries are built from source in CI, so a committed binary is a **Critical** finding (see the "Committed build artifacts" checklist).

**Reviewer 7 — PR metadata & conventions:** Title format, description quality, commit messages, labels, SQL style in tests.

**Reviewer 8 — Rust safety (only if PR contains .rs files):** Check for any code that can panic at runtime — `unwrap()`,
`expect()`, array indexing without bounds checks, `panic!()`, `unreachable!()`, `todo!()`, integer overflow in release
mode, `slice::from_raw_parts` with invalid inputs. In mission-critical software a panic in Rust code called via JNI/FFI
will abort the entire JVM process with no recovery. Every fallible operation must use `Result`/`Option` with proper
error propagation. Flag every potential panic site.

**Reviewer 9 — Cross-context caller impact:** Walk the callsite inventory from 2.5b. For every callsite, fetch the surrounding code (the calling function plus its callers up two levels) and answer:

- Does this caller pass inputs the new behavior handles incorrectly?
- Does this caller depend on a contract from the implicit contract list (2.5c) that the change broke?
- Is this caller in a context (WHERE clause, async runtime, JNI thread, holding lock X, error path, hot loop, parallel worker, replication path, materialized view refresh) where the new behavior misbehaves even if the inputs are valid?
- For SQL functions/operators: is the symbol now resolvable in clauses where it didn't compile before (WHERE on indexed column, JOIN ON, GROUP BY key, ORDER BY, window frame, materialized view definition), and does it actually work there end to end?
- For changed Java methods overridden by subclasses: do all overrides still satisfy the new contract?
- For changed Rust types with trait impls: do all impls still satisfy the new invariants?
- For changed JNI signatures: do all Java callers pass the right types and lifetimes?

This reviewer's output is structured per callsite, not per failure mode. Each callsite gets a verdict: SAFE / BROKEN / NEEDS VERIFICATION. Every BROKEN entry is a P0 finding regardless of whether the file is in the diff.

This reviewer is not optional even when the diff is small. Small diffs to widely-used symbols have the largest blast radius.

**Reviewer 10 — Fresh-context adversarial:** Dispatched separately from reviewers 1-9 to escape checklist anchoring. This reviewer operates under different rules from the rest:

- It receives ONLY the PR diff and the names of the changed files. It does NOT receive the change surface map from Step 2.5, the implicit contract list, the cross-context exposure list, or any of the review checklists below.
- Its sole instruction: "find ways this code is wrong". No category list, no failure-mode taxonomy, no QuestDB-specific style guide.
- It is free to use `read` and `bash` (rg/grep/find) to explore the repository however it wants.
- Findings are not pre-classified by category. Each finding states: what's wrong, why it's wrong, and the code path that demonstrates it.

The point of this reviewer is to surface bugs the structured reviewers cannot see because they are reasoning inside the same frame. A finding here that none of reviewers 1-9 produced is high signal — it means the structured review missed it. A finding here that overlaps with reviewers 1-9 is corroboration.

Run this reviewer in parallel with reviewers 1-9 and 11. It is mandatory regardless of diff size.

**Reviewer 11 — Adversarial performance & zero-GC:** Dispatched separately from Reviewer 3 to escape checklist anchoring. It operates under different rules:

- It receives the PR diff plus the full source of the files the diff touches (not just the changed lines). It does NOT receive the change surface map, the coverage map, Reviewer 3's findings, or any checklist.
- For every function the diff adds or modifies, read the full implementation and ask two questions: **"Does this allocate anything on a path that runs per row / per producer call?"** and **"What is the theoretically fastest way to implement this, and does the code match it?"**
- Work bottom-up from the code. Trace data flow — what is read, how many times, allocated where. Look for: allocations that could be hoisted or served from a reused/pooled buffer; boxing hidden in generics or `java.util.*`; capturing lambdas on hot paths; `String`/`CharSequence` conversions and copies a `DirectUtf8Sequence`/sink would avoid; passes over data that could be fused; lookups that could be O(1); work done unconditionally that is only needed conditionally.
- Use `read` and `bash` (rg/grep/find) freely. Read callers to learn real input sizes and call frequency — an allocation at `build()` time is fine; the same allocation inside `at()` is a defect.
- Each finding states: what the code does now (allocation site / complexity), the optimal approach, and why it matters (call frequency, hot-path placement). Do not duplicate style findings — focus purely on allocation and algorithmic efficiency.

Run this reviewer in parallel with reviewers 1-10. It is mandatory regardless of diff size.

**Test-code reviewers (Reviewers 12-14) — run only when the diff adds or changes test code.** Launch them in the same parallel batch as reviewers 1-11. Each receives the diff, the change surface map, and the test surface inventory from 2.5e. They are the test-code counterparts to the production reviewers: Reviewer 12 mirrors Reviewer 1 (correctness), Reviewer 13 mirrors Reviewer 6 (code quality), and Reviewer 14 verifies regression-test efficacy. Tests are not second-class code — apply the same adversarial rigor here as to production.

**Reviewer 12 — Test efficacy & correctness (adversarial):** Prove each test actually exercises the production change and could fail if that change regressed.
- **Vacuous assertions:** flag every assertion that cannot fail — `assertTrue(true)`, `assertFalse(false)`, `assertEquals(x, x)`, asserting a literal against the same literal, asserting on a value the test itself just hard-coded, or a `@Test` body with no assertion and no `expected=`/`assertThrows`.
- **Tests that don't reach the changed code:** the assertion passes whether or not the production change is present. Trace the data flow from the changed symbol to the assertion.
- **Happy-path-only:** no assertion on the error/exception/NULL path the production change added.
- **Concurrency-test correctness:** races in the test harness itself, missing latches/barriers, an `AssertionError` thrown on a spawned thread where it is swallowed instead of failing the test, `Thread.sleep`-based synchronization that is timing-dependent and flaky.
- **Test setup/teardown resource handling:** native memory allocated in setup/`@Before` that leaks on a failing path, missing `assertMemoryLeak()` wrapping.
- Each finding states the exact assertion and why it cannot fail or what it fails to cover.

**Reviewer 13 — Test-code quality & maintainability:** Review the test as code.
- **Reflection overuse:** flag `setAccessible(true)`, `getDeclaredField`/`getDeclaredMethod`, `Field.set`, `Class.forName`, and similar when a public API, an existing test helper, or a constructor reaches the same state. Reflection in tests is a last resort; if a neater non-reflective path exists, the reflection is a finding — name the alternative.
- **Whitebox tests — strongly prefer blackbox:** flag tests coupled to implementation internals rather than the observable contract. Whitebox markers: reflection into private / package-private fields or methods, asserting on private counters / flags / buffer offsets or exact internal data-structure shape, subclassing a production class solely to expose internals, verifying calls to *internal* collaborators (mock-interaction assertions on private methods), and depending on internal thread / timing state. These rot: a behavior-preserving internal refactor breaks them, and timing/state coupling makes them flaky ("passes on my machine") or quietly stops them exercising anything at all — exactly the failure modes we are trying to avoid. A blackbox test drives the public `Sender` / client API and asserts on observable outputs: the bytes / frames actually produced (captured buffer or loopback / mock server), thrown exceptions and their messages / positions, return values, config-driven behavior. For every whitebox assertion, name the blackbox alternative that observes the same behavior through the public contract. If none exists because the behavior is genuinely unobservable from outside, say so — that is a design smell to note, not a license to keep the whitebox assertion by default.
- **No code reuse / boilerplate stamping:** before accepting repeated setup or assertion blocks, run `rg`/`find` for existing helpers, base test classes, and fixtures (e.g., `extends Abstract.*Test`, `TestUtils`, `*TestUtils`, shared `assert*`, shared `@Before`) using the 2.5e inventory. If a helper already exists that the new test reimplements inline, flag it and name the helper. Duplicated blocks across new test methods that should be a single helper or a parameterized test are findings.
- **Javadoc bloat:** flag multi-paragraph javadoc on `@Test` methods, javadoc that merely restates the test name, and stacked/duplicated javadoc ("javadoc piled on javadoc"). Test intent belongs in a precise test name plus, at most, a one-line comment.
- **Residue and smells:** dead code, commented-out code, copy-paste leftovers (a `testFoo` that actually tests bar), `System.out.println` debugging, `@Ignore` without a referenced ticket, magic numbers >= 5 digits without `_` separators.
- **Which standards apply:** zero-GC and `io.questdb.std`-over-`java.util` do NOT apply to test code — do not flag `java.util` collections or allocations in tests. Member ordering, `is/has` boolean naming, and SQL style DO apply.

**Reviewer 14 — Regression-test efficacy verification:** For any PR that claims to fix a bug, verify the regression test would actually fail without the production change. Reason about reverting the production hunk and confirm the new or changed test's assertions would then fail. If the test still passes with the fix reverted, it is not a regression test — flag it. State, per test, which production line the test depends on and what its assertion would do if that line were reverted. Run only when the PR is a fix; skip for pure features or refactors.

Combine all reviewer findings into a single deduplicated **draft** report. Do NOT present this draft to the user yet — it goes straight into verification.

## Step 3b: Verify every finding against source code

The parallel review reviewers work from the diff plus the change surface map and frequently produce false positives — especially around memory ownership, polymorphic dispatch, Rust control-flow guarantees, and JNI lifecycle conventions. Every finding MUST be verified before it is reported.

For each finding in the draft report:

1. **Read the actual source code** at the exact lines cited. Do not rely on the reviewer's description alone.
2. **Trace the full code path**: follow callers, inheritance hierarchies, and runtime types. A method called on a base-class reference may dispatch to a subclass override (e.g., `PartitionDescriptor.clear()` vs `OwnedMemoryPartitionDescriptor.clear()`).
3. **Check both sides of JNI/FFI boundaries**: if a finding involves Java↔Rust interaction, read both the Java caller and the Rust JNI function. Verify ownership transfer, error propagation, and cleanup on both sides.
4. **For resource leak claims**: trace every allocation to its corresponding free/close on ALL code paths (happy path,
   error path, finally blocks). Check for polymorphic `close()`/`clear()` overrides. Before claiming a leak between
   allocation and cleanup registration, verify that the intervening code can actually throw.
5. **For Rust panic claims**: verify whether the panic site is actually reachable. Trace control flow backwards — a
   preceding guard or early return may make it unreachable.
6. **For Rust panic claims via JNI**: trace the Java caller to check whether it can actually pass parameters that
   trigger the panic. If every caller validates inputs before the JNI call, the panic is unreachable — drop it.
7. **For Rust numeric overflow claims**: check whether the overflow is reachable at realistic scale. QuestDB handles
   billions to a few trillion rows, thousands of tables, and thousands of columns — not billions of columns or
   quintillions of rows. If overflow requires values beyond that scale, drop it.
8. **For performance / allocation claims**: verify the analysis is technically correct (real allocation site, correct
   hot/cold classification, correct complexity). A per-row / per-producer-call allocation or an algorithmic
   inefficiency on a reachable path is a valid finding regardless of how small today's throughput seems — do NOT
   downgrade it as "negligible"; zero-GC on the ingestion path is the standard, and even a single GC allocation on a
   hot path is always worth flagging. The only valid downgrade is if the analysis is wrong (the "allocation" is
   one-off construction/config, or the "linear scan" is bounded by a small constant such as column count).
9. **For cross-context findings (Reviewer 9)**: re-read the callsite in full, including its callers up two levels, and confirm the broken behavior is reachable from production code paths. Cross-context findings are high-value but also the easiest to overstate — verify carefully.
10. **For test-efficacy findings (Reviewers 12, 14)**: re-read the cited assertion in full context and confirm it truly cannot fail — a "vacuous assertion" claim is a false positive if production code actually recomputes the asserted value. For "would pass without the fix" claims, trace what the assertion observes against the reverted production hunk before reporting.
11. **For test-code-quality findings (Reviewer 13)**: confirm a flagged reflective access really has a non-reflective alternative (some QuestDB internals genuinely require reflection in tests) before reporting it. For a whitebox finding, confirm the asserted behavior is actually observable through the public API before demanding a blackbox rewrite — if it genuinely is not observable from outside, downgrade the "use blackbox" demand to a design note rather than a blocking finding. Confirm a "reinvented helper" finding by actually locating the helper with `rg` and checking its signature fits the test's need.
12. **For coverage-gap findings (UNTESTED rows from Step 2.6)**: the ONLY valid reasons to downgrade are (a) a concrete test — local, or in a linked tandem PR — located by a recorded search or PR link, whose assertion demonstrably fails if the change regresses (name the test and the assertion), or (b) a verified no-behavioral-change delta. "The change is simple", "obviously correct", "hard to test", or "covered indirectly" are NOT valid downgrades.
13. **For missing-tandem findings (Step 2.7)**: re-run the `gh pr list --head "$HEAD"` and description-link checks before reporting. Confirm the change actually trips a tandem trigger (server-observable / HA / process-kill) and that no linked tandem PR covers it. If `gh` cannot reach the Enterprise repo, report the finding as "tandem unverifiable — author must confirm" rather than dropping it.
14. **Classify each finding** as:
    - **CONFIRMED in-diff** — the bug is real and inside the diff
    - **CONFIRMED at out-of-diff callsite** — the bug is in an unchanged file because the changed symbol is used there in a way that's now broken (cite the file and the contract from 2.5c that was violated)
    - **FALSE POSITIVE** — the code is actually correct (explain why)
    - **CONFIRMED with nuance** — the issue exists but is less severe than stated (explain)

**Move false positives to a separate "Downgraded" section** at the end of the report. For each, give a one-line explanation of why it was dismissed. This lets the PR author verify the reasoning and catch verification mistakes.

Launch verification reviewers (fresh-context `reviewer` subagents) in parallel where findings are independent. Each verification reviewer should read surrounding source files, not just the diff.

## Review checklists

Review the diff for:

### Correctness & bugs
- NULL handling: distinguish sentinel NULL vs actual NULL
- Edge cases and error paths
- SqlException positions point at the offending character, not the expression start
- Logic errors, off-by-one, incorrect bounds, wrong operator precedence
- **Reachability expansion:** for each changed symbol, list the SQL clauses, async contexts, error paths, parallel workers, and lock-held states it can now appear in but didn't before. Verify it works in each.

### Concurrency
- Race conditions: unsynchronized shared mutable state, missing volatile, unsafe publication
- Lock ordering issues that could cause deadlocks
- Thread-safety of data structures used across threads
- For every changed symbol, check whether it is now called from a thread or context (per 2.5d) where the previous concurrency assumptions don't hold

### Performance & zero-GC

**Zero-GC on the ingestion path is the governing rule of this client**: a single steady-state allocation per row / per producer call is a blocking finding, not a nit. Producer entry points (`table`, `symbol`, `column`, `at`, `flush`) and any per-row / per-batch loop must not allocate — reuse buffers, sinks, and `io.questdb.std` primitives instead. Construction-time, `build()`-time, and config-parse allocations are fine; steady-state ones are not.

- Performance regressions: changes that make hot paths slower or increase complexity
- Unnecessary allocations on data paths (zero-GC requirement) — even one per-row allocation is blocking
- Buffer reuse: growing/reallocating a backing buffer, or allocating a fresh `byte[]`/`char[]`/sink per call where a reused member would serve
- Fresh iterators / `Iterator` objects, `entrySet()`/`keySet()` traversals, and boxed `Optional` on data paths
- Use of `java.util.*` collections (HashMap, ArrayList, etc.) instead of QuestDB's own zero-GC collections in `io.questdb.std`
- String creation or concatenation on hot paths (use CharSink, StringSink, or direct char[] instead)
- Capturing lambdas on hot paths — lambdas that capture local variables or instance fields allocate a new object on every invocation. Non-capturing lambdas (static method refs, no closed-over state) are safe as the JVM caches them. Flag any capturing lambda on a data path.
- Autoboxing on hot paths — primitive-to-wrapper conversions (`int` → `Integer`, `long` → `Long`, etc.) allocate silently. Watch for primitives passed to generic methods, stored in `java.util.*` collections, or returned from methods with wrapper return types.
- Missing SIMD or vectorization opportunities
- Inefficient algorithms where QuestDB already provides optimized alternatives
- Algorithmic complexity at scale: for each new loop or traversal, what is the time complexity as a function of row count, partition count, or join fan-out? Flag O(n^2) or worse patterns. Consider: what happens with 1M outer rows? 10K partitions? 100-way fan-out per row?
- Compile-time vs data-path distinction: allocations and O(n) scans during SQL compilation/optimization are acceptable; the same on per-row data paths are not

### Code quality
- Code smell: overly complex methods, deep nesting, unclear intent, dead code
- No third-party Java dependencies on data paths

### Committed build artifacts
- **A newly committed compiled binary is always Critical.** This repo builds its
  native/C libraries from source in CI (`rebuild_native_libs.yml`,
  `build_native.yaml`, guarded by `check-glibc-floor.sh`) and does not commit
  build outputs. A binary added or modified in the diff cannot be reviewed,
  audited, or reproduced from source, can smuggle in unaudited or malicious
  code, and bloats the repo history irreversibly — so it blocks the merge.
- Detect it structurally, not by extension alone: run `git diff --stat` /
  `git diff --numstat` on the PR and flag every added/modified file git reports
  as binary (`numstat` shows `-`/`-` for added/deleted lines; `--stat` shows a
  `Bin … -> … bytes` marker). Typical offenders: `.so`, `.dylib`, `.dll`, `.a`,
  `.o`, `.lib`, `.exe`, `.class`, `.jar`, `.war`, `.wasm`, `.node`, `.bin`.
- The finding stands even when the binary "looks" legitimate (e.g. a rebuilt
  `libquestdb.*`): the correct source of these artifacts is the CI native-build
  pipeline plus release packaging, never a PR diff. The only acceptable binaries
  are genuine test-input fixtures/resources (data a test reads), not build
  outputs — and even those must be justified.
- Suggested fix: drop the binary from the PR, confirm a `.gitignore` entry
  covers it, and let CI native-build + release packaging produce it.

### QuestDB coding standards
- Class members grouped by kind (static vs instance) and visibility, sorted alphabetically
- Boolean names use `is...` / `has...` prefix
- This module (`questdb-client`) targets Java 11 — only legacy Java features are available. Flag uses of enhanced switch, multiline strings (text blocks), or pattern variables in `instanceof`, since they will not compile here.

### Resource management
- Resources properly closed in all code paths (especially error paths)
- try-with-resources used where applicable
- Native memory freed correctly

### Store-and-forward & pool startup invariants (QWP facade)
Apply this whenever the diff touches the SF sender, the async drainer / send
loop, primary reconnect/failover, `SenderPool` / `QueryClientPool` startup,
`lazy_connect`, or `initial_connect_retry`. A violation here is a **Critical**
finding: the whole point of store-and-forward is that a running producer never
loses data and never hard-fails on a transient outage.

**Drainer (steady state — once the pool is running).**
- Once the pool is running, an async drainer thread ships buffered SF data to
  the server. It MUST NOT propagate server / transport errors back to the
  client (`Sender` producer calls, `flush()`, the pooled handle). The ONLY
  error a running drainer may surface to the caller is **SF out of space** (the
  on-disk / backing buffer is full and can accept no more rows). Flag any other
  failure class (connect-refused, DNS, unreachable/black-hole, TLS/cert, auth,
  role-reject, upgrade/protocol timeout, reset) that can escape the drainer
  onto a producer or borrow call.
- Primary reconnect MUST be fully contained inside the drainer thread and MUST
  have **no time limit** — no `reconnect_max_duration_millis`-style budget, no
  deadline, no "give up and latch terminal after N ms". A budget that latches
  the sender terminal on a long outage is a Critical violation: it drops a
  producer that store-and-forward promised to keep alive. Flag any bounded
  reconnect loop, `deadlineNanos` / `while (now < deadline)`, or terminal
  `SenderError` reachable from the running drainer's reconnect path.
- The drainer must retry with **exponential backoff** and handle every connect
  failure class gracefully, without a hard fail — it keeps buffering and keeps
  retrying until the wire is back. The per-attempt backoff may be capped (a max
  delay between attempts), but the RETRY LOOP ITSELF must be unbounded. Flag a
  capped total retry duration or an attempt-count cap on the steady-state
  drainer.
- **Sanctioned terminals (orphan-slot drainer only).** The orphan drainer
  (`BackgroundDrainer`) MAY quarantine its slot (`.failed` sentinel,
  human-in-the-loop) on conditions that are terminal by design: auth failure,
  a non-421 upgrade reject, and a genuine cluster-wide durable-ack capability
  gap that exhausted its documented settle budget (16 consecutive
  capability-gap sweeps, or a wall-clock budget anchored at the FIRST
  capability-gap error of the episode — whichever is hit first). These are
  NOT violations of the no-budget rule above. The settle budget applies ONLY
  to consecutive capability-gap attempts: transient classes (role reject,
  transport error) must never increment it or burn its wall clock — a
  transient state consuming the terminal budget (shared attempt counter,
  entry-anchored deadline) IS a Critical violation of this checklist.
- **Mid-stream server NACKs (no drop policy).** The NACK policy must mirror
  the connect-time tiering. A rejection category that a transient cluster
  state can produce (`WRITE_ERROR`, `INTERNAL_ERROR`, `UNKNOWN` — and any
  future status byte) is RETRIABLE: recycle the wire and replay from
  `ackedFsn+1`. It must NEVER drop the batch and NEVER latch a terminal /
  quarantine a slot on first sight. Only rejections deterministic under
  byte-identical replay (`SCHEMA_MISMATCH`, `PARSE_ERROR`, `SECURITY_ERROR`
  on a writable node) may go TERMINAL. A client that advances the ack
  watermark past a NACKed frame is silently losing data — Critical. A frame
  repeatedly rejected with no ack progress must escalate through the
  poison-frame detector (bounded consecutive strikes at the same head FSN),
  not through a WS close-code list — close codes carry no policy semantics.
  `UNKNOWN` must fail OPEN (retry), never closed (terminal): a status byte
  from a newer server must degrade to retry, not to a dead sender.

**Pool startup — two modes; the mode decides who sees connectivity errors.**
- `lazy_connect=true`: `build()` MUST succeed with **no server present**. The
  producing `Sender` must work immediately (writes buffer via SF), and once the
  server comes up the read side must also connect and read (reads are deferred,
  not disabled). Verify `build()` does not fail-fast, the sender does not throw
  on the first write while the server is down, and a later `borrowQuery()`
  succeeds once the server is up.
- `lazy_connect=false` (default): `build()` / the initial connect MUST expose
  connectivity problems to the caller — DNS errors, connect-refused /
  unreachable, TLS/cert, authentication/authorization, and connect/upgrade
  timeouts must all surface as a thrown exception at startup, not be swallowed.
  Verify each of those failure classes reaches the user during initialization.
- **In BOTH modes the boundary is the same:** connectivity errors are only
  ever the caller's problem DURING initialization. Once the client has
  connected and is past initialization, the running drainer reverts to the
  steady-state contract above — it must NEVER expose transport problems, NEVER
  impose a reconnect time budget, and NEVER hard-fail on a transient outage.
  Anything that undermines the store-and-forward guarantee past init is
  Critical.

### SQL conventions (if tests or SQL involved)
- Keywords in UPPERCASE
- `expr::TYPE` cast syntax preferred over CAST()
- Underscores in numbers >= 5 digits (e.g., 1_000_000)
- Multiline strings for complex queries
- No DELETE statements (suggest DROP PARTITION or soft delete)
- Tests use `assertMemoryLeak()`, `assertQueryNoLeakCheck()`, `execute()` for DDL
- Single INSERT for multiple rows

### Enterprise permissions & ACL (if PR introduces new SQL statements or ALTER operations)
- New ALTER TABLE operations almost always require a new enterprise permission. If the PR adds a new ALTER statement (or any new SQL statement that modifies state), flag it if there is no corresponding `SecurityContext.authorize*()` call in the execution path.
- New features in OSS should have an enterprise counterpart that wires up ACL. Check whether the PR introduces `authorize*` methods in `SecurityContext` and whether all enterprise `SecurityContext` implementations (`EntSecurityContextBase`, `AdminSecurityContext`, `AbstractReplicaSecurityContext`, and test mocks) are updated.
- New permissions must be registered in `Permission.java` (constant, name maps, and included in `TABLE_PERMISSIONS`/`ALL_PERMISSIONS` as appropriate).
- The `PermissionParser` must be able to parse GRANT/REVOKE for the new permission name — especially if the name contains SQL keywords like `ON`, `TO`, or `FROM` that could conflict with parser grammar.
- Replica security contexts must deny new write operations (`deniedOnReplica()`).

### Cross-repo tandem-PR testing (this repo has no real-server e2e)
Apply the full Step 2.7 gate. In short:
- This repo's CI runs unit tests only. Real-server e2e lives in a tandem `questdb` (OSS) PR with a matching branch, cross-linked in both descriptions; OSS CI exercises it via the client submodule.
- Any high-availability feature, or change touching HA-facing code (store-and-forward drainer, reconnect/failover, pool startup, `lazy_connect`, replication/role gating), needs a tandem `questdb-enterprise` PR.
- Any scenario that depends on a `kill -9` of the client or server (crash recovery, store-and-forward across a crash, hard failover) needs a test in the Enterprise **e2e python** infrastructure — it cannot be a JVM unit test.
- Verify the required tandem exists and is linked (record the `gh pr list --head "$HEAD"` search). A required-but-missing tandem is a **Critical**, blocking finding, and every behavior it would have covered is UNTESTED — scrutinise the diff assuming no integration safety net exists.

### Test review
- **Coverage map is blocking:** consume the Step 2.6 coverage map. Every UNTESTED row carries its default severity (Critical for new wire/API behavior, bug fixes, error paths, concurrency, resource lifecycles, and anything needing a missing tandem). Do not downgrade an UNTESTED Critical because the change "looks simple".
- **Cross-repo tandem coverage:** for behavior observable only against a real server, a failover, or a process kill, the covering test lives in a tandem PR (Step 2.7) — verify the required tandem exists, is linked (matching branch), and runs the suite. A required-but-missing tandem is a Critical coverage gap.
- **Coverage gaps:** For every new or changed code path, verify a corresponding test exists. If not, flag it explicitly as "missing test for X".
- **Cross-context coverage:** For every entry in the cross-context exposure list (2.5d), verify a test exercises the changed symbol from that context. Missing cross-context tests are high-priority findings.
- **Error path coverage:** Are failure cases, exceptions, and edge conditions tested — not just the happy path?
- **NULL tests:** Are NULL inputs, NULL columns, and NULL expression results tested?
- **Boundary conditions:** Empty tables, empty partitions, single-row tables, max-value inputs, zero-length strings.
- **Concurrency tests:** If the code touches shared state, are there tests that exercise concurrent access?
- **Resource leak tests:** Tests must use `assertMemoryLeak()` for anything that allocates native memory.
- **Test quality (blackbox-first):** Are tests actually asserting the right thing? Watch for tests that pass trivially, assert on wrong values, or test implementation details instead of behavior. Strongly prefer blackbox tests (public API + observable output) over whitebox tests (reflection / internal state) — whitebox tests become flaky or stop working after refactors; see "Prefer blackbox over whitebox" in the "Test code quality" checklist.
- **Regression tests:** If this PR fixes a bug, is there a test that reproduces the original bug and would fail without the fix?
- Use `read`/`bash` (rg/find) to find existing test files for the changed classes and verify they cover the new behavior.

### Test code quality
- **No vacuous assertions.** Every assertion must be able to fail. Flag `assertTrue(true)`, `assertFalse(false)`, `assertEquals(x, x)`, asserting a literal against the same literal, or a `@Test` body with no assertion and no `expected=`/`assertThrows`.
- **Reflection is a last resort.** Flag `setAccessible(true)`, `getDeclaredField`/`getDeclaredMethod`, `Field.set`, `Class.forName` when a public API, existing helper, or constructor would reach the same state. Name the non-reflective path.
- **Prefer blackbox over whitebox.** Strongly prefer tests that drive the public `Sender` / client API and assert on observable outputs (produced bytes / frames, thrown exceptions, return values, config-driven behavior) over tests coupled to internals (reflection into private state, asserting private counters / offsets / internal structure, subclassing to expose internals, verifying internal-collaborator calls, depending on internal thread / timing state). Whitebox tests break on behavior-preserving refactors and drift into flakiness or become no-ops — flag them and name the blackbox alternative. Reflection is only acceptable when the behavior is genuinely unobservable via the public contract.
- **Reuse before reinventing.** Search for existing helpers, base classes, and fixtures before accepting inline setup. Duplicated setup/assert blocks an existing helper or a parameterized test would cover are findings; name the helper.
- **No javadoc bloat.** No multi-paragraph javadoc on `@Test` methods, no javadoc that restates the test name, no stacked/duplicated javadoc. Prefer a precise test name and at most a one-line comment.
- **Test-appropriate standards.** zero-GC and `io.questdb.std`-over-`java.util` rules do NOT apply to tests — do not flag them there. Member ordering, `is/has` naming, and SQL style DO apply.
- **No debugging residue.** No `System.out.println`, no commented-out code, no `@Ignore` without a referenced ticket.

### Unresolved TODOs and FIXMEs
- Scan the diff for `TODO`, `FIXME`, `HACK`, `XXX`, and `WORKAROUND` comments. For each one found:
  - Is it a pre-existing comment that was just moved/reformatted, or newly introduced in this PR?
  - If newly introduced: does it represent unfinished work that should block the merge, or a known limitation that is acceptable to ship? Flag any that look like deferred bugs or incomplete implementations.
  - If the TODO references a ticket/issue number, verify the reference exists.

### Commit messages
- Plain English titles (no Conventional Commits prefix), under 50 chars
- Full long-form body description, line breaks at 72 chars
- Active voice, naming the acting subject

## Step 4: Output

Present ONLY verified findings (false positives are excluded). Structure as:

### Critical
Issues that must be fixed before merge. **A newly committed compiled binary or
other build artifact (see the "Committed build artifacts" checklist) is always
Critical, no matter how legitimate it looks — native/C libraries are built from
source in CI, so a binary in the diff is never acceptable.** Every UNTESTED-Critical
row from the Step 2.6 coverage map, every required-but-missing tandem PR (Step 2.7),
and every confirmed steady-state (per-row / per-producer-call) allocation on the
ingestion path is likewise Critical. Each must include:
- Exact file path and line numbers (including out-of-diff files)
- Whether the finding is **in-diff** or **out-of-diff**
- Code path trace showing why the bug is real
- For out-of-diff findings: the contract from 2.5c that was violated and the callsite that triggers it
- Suggested fix

### Moderate
Issues worth addressing but not blocking.

### Minor
Style nits and suggestions.

### Downgraded (false positives)
Findings from the initial review that were dismissed after source code verification. For each, state:
- The original claim (one line)
- Why it was dismissed (one line, citing the specific code that disproves it)

### Coverage map
Render the full Step 2.6 coverage map: one row per behavioral change with its test (local or tandem — cite the tandem PR link), failure link, dimension marks (including justified N-As), and TESTED / UNTESTED / EXEMPT verdict. EXEMPT rows must show the verified no-behavioral-change delta. This section is mandatory whenever the PR touches production code — it is the audit trail for the test gate below.

### Summary
- One-line verdict: approve, request changes, or needs discussion
- **Test & tandem gate (hard rule):** the verdict cannot be "approve" while (a) any UNTESTED Critical row remains in the Step 2.6 coverage map, (b) the PR claims a fix but has no regression test (local or tandem) with a verified failure link, (c) new server-observable behavior ships without a linked, running OSS e2e tandem, (d) an HA change ships without a linked Enterprise tandem, or (e) a `kill -9`-dependent scenario ships without an Enterprise e2e-python test. If the PR changes production code and adds zero tests here AND has no required tandem, the verdict is "request changes" unless every behavioral delta is verified "no behavioral change" — state that justification explicitly.
- **Zero-GC gate (hard rule):** the verdict cannot be "approve" while any confirmed steady-state (per-row / per-producer-call) allocation, or reachable algorithmic inefficiency, on the ingestion path remains open. These are Critical, never Minor.
- Highlight any regressions or tradeoffs
- State the coverage-map totals (e.g., "coverage map: 12 behavioral changes, 9 tested [3 via tandem], 3 UNTESTED") — the totals must match the rendered Coverage map section row-for-row
- State the required-tandem status (e.g., "requires OSS e2e tandem: linked #123; no Enterprise tandem required")
- State how many draft findings were verified vs dropped as false positives (e.g., "8 findings verified, 4 false positives removed")
- State the in-diff vs out-of-diff split (e.g., "5 findings in-diff, 3 findings out-of-diff"). If the diff is non-trivial and out-of-diff is zero, the cross-context pass likely underran — re-invoke Reviewer 9 with a wider grep before finalizing.
