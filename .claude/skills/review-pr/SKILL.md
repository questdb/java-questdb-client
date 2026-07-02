---
name: review-pr
description: Review a GitHub pull request against QuestDB coding standards. Performs an adversarial, blocking, mission-critical code review covering correctness, concurrency, performance, resource management, test coverage, test efficacy, test-code quality, and QuestDB conventions, then verifies every finding against source before reporting.
argument-hint: [PR number or URL] [--level=0..3]
allowed-tools: Bash(gh *), Read, Grep, Glob, Agent
---

Review the pull request `$ARGUMENTS`.

## Review mindset

You are a senior QuestDB engineer performing a blocking code review. QuestDB is mission-critical software deployed on spacecraft — bugs can cause data loss or system failures that cannot be patched after deployment. There is zero tolerance for correctness issues, resource leaks, or undefined behavior. Be critical, thorough, and opinionated. Your job is to catch problems before they ship, not to be nice.

- **Assume nothing is correct until you've verified it.** Read surrounding code to understand context — don't just look at the diff in isolation.
- **The diff is a hint, not the boundary of the review.** The highest-value bugs almost always live at callsites outside the diff that depend on contracts the diff quietly changed. Treat the diff as the entry point, not the scope.
- **Flag every issue you find**, no matter how small. Do not soften language or hedge. Say "this is wrong" not "this might be an issue".
- **Do not praise the code.** Skip "looks good", "nice work", "clever approach". Focus entirely on problems and risks.
- **Think adversarially.** For each change, ask: what inputs break this? What happens under concurrent access? What if this runs on a 10-billion-row table? What if the column is NULL? What if the partition is empty?
- **Check what's missing**, not just what's there. Missing tests, missing error handling, missing edge cases, missing documentation for non-obvious behavior.
- **Verify every claim.** If the PR title says "fix", verify the bug actually existed and the fix is correct. If it says "improve performance", look for benchmarks or reason about the algorithmic change — does it actually improve things, or could it regress in other cases? If it says "simplify", verify the new code is actually simpler and doesn't drop behavior. Treat the PR description as an unverified hypothesis, not a statement of fact.
- **Read the full context of changed files** when the diff alone is ambiguous. Use Read/Grep/Glob to inspect the surrounding code, callers, and related tests.
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
| **0 (default)** | Steps 1, 2, 4. Skip Step 2.5. Skip Step 3 — no agent spawn; review the diff inline in the main loop, using Read/Grep on demand to resolve ambiguities. Skip Step 3b — verify each finding inline as you write it. Single-pass review covering correctness, NULL handling, test coverage, and QuestDB standards on the diff itself. When the diff touches test code, also apply the test-efficacy and test-code-quality anti-pattern checks inline (vacuous assertions, reflection overuse, reinvented helpers, javadoc bloat). |
| **1** | Adds Step 2.5a (semantic delta only — skip 2.5b/2.5c/2.5d) plus Step 2.5e when test code is present. In Step 3, launch Agent 1 (correctness), Agent 5 (test coverage), Agent 6 (code quality), and — when the diff touches test code — Agent 11 (test efficacy) and Agent 12 (test-code quality) in parallel. Skip all other agents. Skip Step 3b — verify findings inline as you draft the report. |
| **2** | Full Step 2.5 (including 2.5e when test code is present), but in 2.5b restrict the callsite inventory to `public`/`protected` symbols (skip package-private and `pub(crate)`). In Step 3, launch Agents 1-7, plus Agent 8 if `.rs` files are present, plus Agents 11 and 12 when the diff touches test code. Skip Agent 9 (cross-context), Agent 10 (adversarial fresh-context), and Agent 13 (regression-test efficacy verification). Step 3b uses a single batched verification agent for all findings instead of one per finding. |
| **3** | Every step below as written, all 13 agents, per-finding verification. The full mission-critical pass. |

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

Before launching review agents, produce a structured change surface map. This step is mandatory and must use Grep/Glob — do not reason about callsites from memory. The output of this step is required input for every agent in Step 3.

### 2.5a Semantic delta per changed symbol

For every modified or added function, method, trait, struct field, SQL operator/function, or public constant, write:

- **Symbol:** fully-qualified name
- **Before:** signature, return type, error/exception behavior, panic behavior, mutation (`&self` vs `&mut self`, `final` vs not), ordering/idempotency guarantees, allocation behavior, thread-safety
- **After:** same fields
- **Delta:** one line stating what semantically changed

"Refactored", "cleaned up", "improved", "simplified" are not acceptable deltas. State the actual behavioral difference. If nothing semantically changed, write "no behavioral change" — but only after checking, not as a default.

### 2.5b Callsite inventory

For every changed symbol that is `public`, `protected`, package-private, or exported (`pub` / `pub(crate)` in Rust), run Grep across the entire repository to find every callsite, implementation, override, or reference outside the diff.

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

A changed `pub`/`protected`/package-private symbol with zero recorded Grep calls in the trace is a skill violation. The model is not allowed to assert "this is only used here" without showing the search.

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

End this step with an explicit list of "places this change is visible from but the diff does not touch". This is the highest-priority input for the bug-hunting agents in Step 3.

The list groups the callsites from 2.5b by execution context: hot data paths, SQL compilation, async runtime, JNI boundary, replication, materialized views, parallel execution workers, etc. Every entry on this list must be reviewed in Step 3.

### 2.5e Test surface & helper inventory

Run this only when the PR adds or changes test code. It is the test-code counterpart to 2.5b and feeds Agents 11-13. Use real Grep/Glob searches — do not reason about helpers from memory.

- **Existing-infrastructure inventory:** search the changed test files' package and module for base test classes, shared `@Before`/`@After`, helper methods, fixtures, and assertion utilities the new tests could reuse (Grep for `extends Abstract.*Test`, `class .*TestUtils`, `assertMemoryLeak`, `assertQuery`, `assertSql`, shared `protected` helpers in the base class). This list is the baseline Agent 12 uses to flag reinvented boilerplate — a "you stamped boilerplate instead of reusing helper X" finding requires X to appear in this inventory.
- **Changed shared helpers as symbols:** if the PR changes a shared test base class, helper, or fixture, run the 2.5b callsite inventory for it too — a changed test base class can silently break every subclassing test.
- **Exercised-symbol map:** for each new or changed test, list which production symbols from 2.5a it actually exercises, so Agents 11 and 13 can check efficacy and regression value.

## Step 3: Parallel review

Every agent receives:
1. The PR diff
2. The full change surface map from Step 2.5 (semantic deltas, callsite inventory, implicit contracts, cross-context exposure list)

### Anti-anchoring directive (applies to all agents)

- **Bugs at callsites outside the diff outrank bugs inside the diff.** A confirmed bug in a file the PR did not touch but that calls a changed symbol is a P0 finding.
- **"Looks correct in isolation" is not a valid conclusion.** Before clearing a changed symbol, the agent must walk the callsite inventory from 2.5b and explicitly state, per callsite, whether the new behavior is still correct there.
- **The diff is the entry point, not the scope.** If the change surface map shows the symbol is reachable from N other files, the review covers N+1 files.
- A single finding of the form "in `FooReader.java` the new behavior of `Bar.x()` causes Y" is worth more than five findings inside the diff.

### Agents

Launch the following agents in parallel.

**Agent 1 — Correctness & bugs:** NULL handling, edge cases, logic errors, off-by-one, operator precedence, error paths. Cross-reference every changed symbol against its callsite inventory and verify the new behavior is correct at each callsite. When the diff touches the store-and-forward sender, the async drainer / send loop, primary reconnect/failover, or pool startup (`lazy_connect` / `initial_connect_retry` / `SenderPool` / `QueryClientPool`), also verify the "Store-and-forward & pool startup invariants" checklist — a running drainer that propagates a transport error to the caller, imposes a reconnect time budget, or hard-fails on a transient outage is a Critical (data-loss) finding.

**Agent 2 — Concurrency:** Race conditions, shared mutable state, missing volatile, lock ordering, thread-safety of data structures. Use the implicit contract list (lock order, thread-affinity) and check every callsite from 2.5b for violations of the new contract.

**Agent 3 — Performance & allocations:** Regressions, zero-GC violations, `java.util.*` collections vs `io.questdb.std`, string creation/concatenation on hot paths, SIMD opportunities. Algorithmic complexity: for each new loop, traversal, or data structure, analyze how it scales with data size (row count, partition count, join fan-out). Flag any O(n^2) or worse patterns that could regress on large tables (1M+ rows, 1000+ partitions). Check whether new code paths are compile-time-only or data-path — compile-time allocations are acceptable, data-path allocations are not. For changed symbols now reachable from new contexts (per 2.5d), check whether any of those new contexts is a hot path.

**Agent 4 — Resource management:** Leaks on all code paths (especially errors), try-with-resources, native memory, pool management. Walk every callsite from 2.5b that constructs, owns, or transfers ownership of changed types and verify cleanup on all paths.

**Agent 5 — Test coverage:** Coverage gaps, error path tests, NULL tests, boundary conditions, regression tests exist, `assertMemoryLeak()` usage. Cross-reference 2.5d: every cross-context exposure should have a test that exercises the changed symbol from that context. Missing tests for cross-context callsites is a high-priority finding. Test *efficacy* (whether those tests actually exercise the change and could fail) and test-*code* quality are handled by Agents 11-13 — here focus only on whether coverage exists for every new or changed path.

**Agent 6 — Code quality & standards:** Code smell, member ordering, naming conventions, modern Java features, dead code, third-party dependencies. Also scan the diff for any committed compiled binary / build artifact (run `git diff --numstat`/`--stat` and flag files git reports as binary) — the native/C libraries are built from source in CI, so a committed binary is a **Critical** finding (see the "Committed build artifacts" checklist).

**Agent 7 — PR metadata & conventions:** Title format, description quality, commit messages, labels, SQL style in tests.

**Agent 8 — Rust safety (only if PR contains .rs files):** Check for any code that can panic at runtime — `unwrap()`,
`expect()`, array indexing without bounds checks, `panic!()`, `unreachable!()`, `todo!()`, integer overflow in release
mode, `slice::from_raw_parts` with invalid inputs. In mission-critical software a panic in Rust code called via JNI/FFI
will abort the entire JVM process with no recovery. Every fallible operation must use `Result`/`Option` with proper
error propagation. Flag every potential panic site.

**Agent 9 — Cross-context caller impact:** Walk the callsite inventory from 2.5b. For every callsite, fetch the surrounding code (the calling function plus its callers up two levels) and answer:

- Does this caller pass inputs the new behavior handles incorrectly?
- Does this caller depend on a contract from the implicit contract list (2.5c) that the change broke?
- Is this caller in a context (WHERE clause, async runtime, JNI thread, holding lock X, error path, hot loop, parallel worker, replication path, materialized view refresh) where the new behavior misbehaves even if the inputs are valid?
- For SQL functions/operators: is the symbol now resolvable in clauses where it didn't compile before (WHERE on indexed column, JOIN ON, GROUP BY key, ORDER BY, window frame, materialized view definition), and does it actually work there end to end?
- For changed Java methods overridden by subclasses: do all overrides still satisfy the new contract?
- For changed Rust types with trait impls: do all impls still satisfy the new invariants?
- For changed JNI signatures: do all Java callers pass the right types and lifetimes?

This agent's output is structured per callsite, not per failure mode. Each callsite gets a verdict: SAFE / BROKEN / NEEDS VERIFICATION. Every BROKEN entry is a P0 finding regardless of whether the file is in the diff.

This agent is not optional even when the diff is small. Small diffs to widely-used symbols have the largest blast radius.

**Agent 10 — Fresh-context adversarial:** Dispatched separately from agents 1-9 to escape checklist anchoring. This agent operates under different rules from the rest:

- It receives ONLY the PR diff and the names of the changed files. It does NOT receive the change surface map from Step 2.5, the implicit contract list, the cross-context exposure list, or any of the review checklists below.
- Its sole instruction: "find ways this code is wrong". No category list, no failure-mode taxonomy, no QuestDB-specific style guide.
- It is free to use Read, Grep, and Glob to explore the repository however it wants.
- Findings are not pre-classified by category. Each finding states: what's wrong, why it's wrong, and the code path that demonstrates it.

The point of this agent is to surface bugs the structured agents cannot see because they are reasoning inside the same frame. A finding here that none of agents 1-9 produced is high signal — it means the structured review missed it. A finding here that overlaps with agents 1-9 is corroboration.

Run this agent in parallel with agents 1-9. It is mandatory regardless of diff size.

**Test-code agents (Agents 11-13) — run only when the diff adds or changes test code.** Launch them in the same parallel batch as agents 1-10. Each receives the diff, the change surface map, and the test surface inventory from 2.5e. They are the test-code counterparts to the production agents: Agent 11 mirrors Agent 1 (correctness), Agent 12 mirrors Agent 6 (code quality), and Agent 13 verifies regression-test efficacy. Tests are not second-class code — apply the same adversarial rigor here as to production.

**Agent 11 — Test efficacy & correctness (adversarial):** Prove each test actually exercises the production change and could fail if that change regressed.
- **Vacuous assertions:** flag every assertion that cannot fail — `assertTrue(true)`, `assertFalse(false)`, `assertEquals(x, x)`, asserting a literal against the same literal, asserting on a value the test itself just hard-coded, or a `@Test` body with no assertion and no `expected=`/`assertThrows`.
- **Tests that don't reach the changed code:** the assertion passes whether or not the production change is present. Trace the data flow from the changed symbol to the assertion.
- **Happy-path-only:** no assertion on the error/exception/NULL path the production change added.
- **Concurrency-test correctness:** races in the test harness itself, missing latches/barriers, an `AssertionError` thrown on a spawned thread where it is swallowed instead of failing the test, `Thread.sleep`-based synchronization that is timing-dependent and flaky.
- **Test setup/teardown resource handling:** native memory allocated in setup/`@Before` that leaks on a failing path, missing `assertMemoryLeak()` wrapping.
- Each finding states the exact assertion and why it cannot fail or what it fails to cover.

**Agent 12 — Test-code quality & maintainability:** Review the test as code.
- **Reflection overuse:** flag `setAccessible(true)`, `getDeclaredField`/`getDeclaredMethod`, `Field.set`, `Class.forName`, and similar when a public API, an existing test helper, or a constructor reaches the same state. Reflection in tests is a last resort; if a neater non-reflective path exists, the reflection is a finding — name the alternative.
- **No code reuse / boilerplate stamping:** before accepting repeated setup or assertion blocks, run Grep/Glob for existing helpers, base test classes, and fixtures (e.g., `extends Abstract.*Test`, `TestUtils`, `*TestUtils`, shared `assert*`, shared `@Before`) using the 2.5e inventory. If a helper already exists that the new test reimplements inline, flag it and name the helper. Duplicated blocks across new test methods that should be a single helper or a parameterized test are findings.
- **Javadoc bloat:** flag multi-paragraph javadoc on `@Test` methods, javadoc that merely restates the test name, and stacked/duplicated javadoc ("javadoc piled on javadoc"). Test intent belongs in a precise test name plus, at most, a one-line comment.
- **Residue and smells:** dead code, commented-out code, copy-paste leftovers (a `testFoo` that actually tests bar), `System.out.println` debugging, `@Ignore` without a referenced ticket, magic numbers >= 5 digits without `_` separators.
- **Which standards apply:** zero-GC and `io.questdb.std`-over-`java.util` do NOT apply to test code — do not flag `java.util` collections or allocations in tests. Member ordering, `is/has` boolean naming, and SQL style DO apply.

**Agent 13 — Regression-test efficacy verification:** For any PR that claims to fix a bug, verify the regression test would actually fail without the production change. Reason about reverting the production hunk and confirm the new or changed test's assertions would then fail. If the test still passes with the fix reverted, it is not a regression test — flag it. State, per test, which production line the test depends on and what its assertion would do if that line were reverted. Run only when the PR is a fix; skip for pure features or refactors.

Combine all agent findings into a single deduplicated **draft** report. Do NOT present this draft to the user yet — it goes straight into verification.

## Step 3b: Verify every finding against source code

The parallel review agents work from the diff plus the change surface map and frequently produce false positives — especially around memory ownership, polymorphic dispatch, Rust control-flow guarantees, and JNI lifecycle conventions. Every finding MUST be verified before it is reported.

For each finding in the draft report:

1. **Read the actual source code** at the exact lines cited. Do not rely on the agent's description alone.
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
8. **For performance claims**: check whether the cost is measurable in a realistic scenario. Downgrade to a nit if the
   saving is negligible relative to the surrounding work. Exception: GC allocations on a hot path are always worth
   flagging, even a single one.
9. **For cross-context findings (Agent 9)**: re-read the callsite in full, including its callers up two levels, and confirm the broken behavior is reachable from production code paths. Cross-context findings are high-value but also the easiest to overstate — verify carefully.
10. **For test-efficacy findings (Agents 11, 13)**: re-read the cited assertion in full context and confirm it truly cannot fail — a "vacuous assertion" claim is a false positive if production code actually recomputes the asserted value. For "would pass without the fix" claims, trace what the assertion observes against the reverted production hunk before reporting.
11. **For test-code-quality findings (Agent 12)**: confirm a flagged reflective access really has a non-reflective alternative (some QuestDB internals genuinely require reflection in tests) before reporting it. Confirm a "reinvented helper" finding by actually locating the helper with Grep and checking its signature fits the test's need.
12. **Classify each finding** as:
    - **CONFIRMED in-diff** — the bug is real and inside the diff
    - **CONFIRMED at out-of-diff callsite** — the bug is in an unchanged file because the changed symbol is used there in a way that's now broken (cite the file and the contract from 2.5c that was violated)
    - **FALSE POSITIVE** — the code is actually correct (explain why)
    - **CONFIRMED with nuance** — the issue exists but is less severe than stated (explain)

**Move false positives to a separate "Downgraded" section** at the end of the report. For each, give a one-line explanation of why it was dismissed. This lets the PR author verify the reasoning and catch verification mistakes.

Launch verification agents in parallel where findings are independent. Each verification agent should read surrounding source files, not just the diff.

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

### Performance
- Performance regressions: changes that make hot paths slower or increase complexity
- Unnecessary allocations on data paths (zero-GC requirement)
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
- Modern Java features: enhanced switch, multiline strings, pattern variables in instanceof

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

### Test review
- **Coverage gaps:** For every new or changed code path, verify a corresponding test exists. If not, flag it explicitly as "missing test for X".
- **Cross-context coverage:** For every entry in the cross-context exposure list (2.5d), verify a test exercises the changed symbol from that context. Missing cross-context tests are high-priority findings.
- **Error path coverage:** Are failure cases, exceptions, and edge conditions tested — not just the happy path?
- **NULL tests:** Are NULL inputs, NULL columns, and NULL expression results tested?
- **Boundary conditions:** Empty tables, empty partitions, single-row tables, max-value inputs, zero-length strings.
- **Concurrency tests:** If the code touches shared state, are there tests that exercise concurrent access?
- **Resource leak tests:** Tests must use `assertMemoryLeak()` for anything that allocates native memory.
- **Test quality:** Are tests actually asserting the right thing? Watch for tests that pass trivially, assert on wrong values, or test implementation details instead of behavior.
- **Regression tests:** If this PR fixes a bug, is there a test that reproduces the original bug and would fail without the fix?
- Use Grep/Glob to find existing test files for the changed classes and verify they cover the new behavior.

### Test code quality
- **No vacuous assertions.** Every assertion must be able to fail. Flag `assertTrue(true)`, `assertFalse(false)`, `assertEquals(x, x)`, asserting a literal against the same literal, or a `@Test` body with no assertion and no `expected=`/`assertThrows`.
- **Reflection is a last resort.** Flag `setAccessible(true)`, `getDeclaredField`/`getDeclaredMethod`, `Field.set`, `Class.forName` when a public API, existing helper, or constructor would reach the same state. Name the non-reflective path.
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
source in CI, so a binary in the diff is never acceptable.** Each must include:
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

### Summary
- One-line verdict: approve, request changes, or needs discussion
- Highlight any regressions or tradeoffs
- State how many draft findings were verified vs dropped as false positives (e.g., "8 findings verified, 4 false positives removed")
- State the in-diff vs out-of-diff split (e.g., "5 findings in-diff, 3 findings out-of-diff"). If the diff is non-trivial and out-of-diff is zero, the cross-context pass likely underran — re-invoke Agent 9 with a wider grep before finalizing.
