# TODO

## QWP UDP Sender

### Documented limitation: mixing `atNow()` with `atMicros()` / `atNanos()`

Current behavior in `QwpUdpSender` is to reject this pattern once committed rows already exist for the table:

1. Write row(s) with `atNow()` (server-assigned designated timestamp).
2. Start a later row and finish it with `atMicros()` or `atNanos()`.

The sender throws:

- `schema change in middle of row is not supported`

Why this happens:

- `atNow()` does not write the designated timestamp column.
- `atMicros()` / `atNanos()` writes designated timestamp into the empty-name column (`""`).
- With committed rows already present, introducing this column is treated as schema evolution.
- The UDP incremental-estimate policy forbids schema changes in the middle of an in-progress row.

Current workaround:

- Use one designated timestamp strategy consistently per table stream:
  - always `atNow()`, or
  - always `atMicros()` / `atNanos()`.

Future fix options:

- Add explicit support for switching designated timestamp strategy mid-stream by pre-materializing designated timestamp schema state, or
- Harmonize designated timestamp handling so `atNow()` and `atMicros()` / `atNanos()` do not diverge schema shape.
