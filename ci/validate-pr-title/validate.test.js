const assert = require("node:assert").strict;
const { validate, allowedTypes, allowedSubTypes } = require("./validate");

const testValid = (title) =>
  assert.doesNotThrow(
    () =>
      validate({
        title,
        onError: () => {
          throw new Error(`should accept "${title}"`);
        },
      }),
    `should accept "${title}"`,
  );

// onError has to be a real callback here. Passing a bare `onError` identifier makes
// this assertion pass on the ReferenceError that raises instead of on the title
// being rejected, which lets every negative case below succeed against a validator
// that accepts everything.
const testInvalid = (title) =>
  assert.throws(
    () =>
      validate({
        title,
        onError: () => {
          throw new Error(`rejected "${title}"`);
        },
      }),
    `should NOT accept "${title}"`,
  );

allowedTypes.forEach((type) => {
  allowedSubTypes.forEach((subType) => {
    testValid(`${type}(${subType}): foo`);
  });
});

testValid("build: 6.6");
testValid("build: hello world");
testInvalid("build");

testValid(`build: house`);
testInvalid(`build(house)`);

testInvalid(`foo: bar`);
testInvalid(`update(bar): baz`);
testInvalid(`ui: updating stuff`);

// Titles this repository actually merges.
testValid("feat(qwp): add table options API to name the designated timestamp column");
testValid("fix(ilp): fix a leaked socket when an HTTP sender fails");
testValid("chore(build): build client native library with Maven");
testValid("build: 6.6");

// The Conventional Commits breaking-change marker, on both accepted formats.
testValid("feat(qwp)!: drop the legacy sender constructor");
testValid("build!: require JDK 17");
testInvalid("feat(qwp)!");
testInvalid("feat(nonsense)!: still an unknown area");

// Subtypes that belong to the server repositories, not this client.
testInvalid("fix(sql): not an area of this repository");
testInvalid("fix(wal): not an area of this repository");
testInvalid("fix(repl): not an area of this repository");

// Only `build` may skip the subType. Every other type has to name one. This is
// what rejects the automated "Bump version to x.y.z-SNAPSHOT" release titles.
testInvalid("chore: bump a dependency");
testInvalid("Bump version to 1.3.10-SNAPSHOT");

console.log("all validate.js rules passed");
