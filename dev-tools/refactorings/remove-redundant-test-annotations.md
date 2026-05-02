# Removing redundant @Test annotations from test* methods

In Lucene's test framework (RandomizedTesting / LuceneTestCase), any method whose name
starts with `test` is automatically discovered as a test — the `@Test` annotation is
redundant on those methods. These scripts find and remove them.

## Prerequisites

Docker (no local Java/Maven needed). The named volume `jbang-cache` persists downloaded
jars so the second run is instant.

## Step 1 — audit (find, don't touch)

```bash
docker run --rm \
  -v jbang-cache:/root/.jbang/cache \
  -v "$(pwd)":/workdir \
  jbangdev/jbang \
  /workdir/dev-tools/refactorings/FindAnnotatedTestMethods.java /workdir 2>/dev/null
```

Output: one line per match — `path/to/File.java:line: @Test methodSignature(...)`.
Pipe through `grep -v '@Test test'` to surface any non-plain `@Test(...)` survivors.

## Step 2 — remove

```bash
docker run --rm \
  -v jbang-cache:/root/.jbang/cache \
  -v "$(pwd)":/workdir \
  jbangdev/jbang \
  /workdir/dev-tools/refactorings/RemoveRedundantTestAnnotations.java /workdir 2>/dev/null
```

- Prints each modified file path to stdout.
- Uses `LexicalPreservingPrinter` — only the removed annotation node is touched,
  no other reformatting occurs.
- Also removes `import org.junit.Test;` from files where no `@Test` usages remain.
- Run it twice if you see output: the second run should produce nothing (idempotent).

## Known quirks

**Build-tool sources** — `build-tools/build-infra/**` contains Gradle plugin sources
that use Groovy-interop annotations; several fail to parse. None of them contain test
methods, so the failures are harmless.

## Re-running after new test files are added

Just run Step 2 again from the repo root. It is idempotent: files with no redundant
annotations are left untouched.
