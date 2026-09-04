const allowedTypes = [
  "feat",
  "fix",
  "chore",
  "docs",
  "style",
  "refactor",
  "perf",
  "test",
  "ci",
  "revert",
];

const allowedSubTypes = [
  "build",
  "log",
  "core",
  "ilp",
  "qwp",
  "http",
  "conf",
  "utils",
];

const errorMessage = `
Please update the PR title to match this format:
\`type(subType): description\`

Where \`type\` is one of:
${allowedTypes.map((t) => `\`${t}\``).join(", ")}

And: \`subType\` is one of:
${allowedSubTypes.map((t) => `\`${t}\``).join(", ")}

For Example:

\`\`\`
perf(sql): improve pattern matching performance for SELECT sub-queries
\`\`\`
`.trim();

/* The valid PR title formats are:
 * 1. allowedType(allowedSubType): description
 * 2. build: description
 *
 * Note that format 2 is available to `build` alone. Every other type has to name
 * a subType, so `feat: thing` is rejected while `build: 6.6` is accepted.
 *
 * A `!` before the colon is the Conventional Commits marker for a breaking
 * change, as in `feat(qwp)!: ...`, and is accepted on either format.
 * consult ./validate.test.js for a full list
 * */
const prTitleRegex = new RegExp(
  `^(((?:${allowedTypes.join("|")})\\((?:${allowedSubTypes.join(
    "|",
  )})\\))|build)!?: .*`,
);

function validate({ title, onError }) {
  // Early return for title that matches predefined regex.
  // No action required in such case.
  if (title.match(prTitleRegex)) {
    return;
  }

  onError(errorMessage);
}

module.exports = {
  allowedTypes,
  allowedSubTypes,
  validate,
};
