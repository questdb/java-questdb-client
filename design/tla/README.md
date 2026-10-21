# TLA+ models of the QWP I/O loop wake protocols

The cursor I/O loop (`CursorWebSocketSendLoop`) does not poll: when idle it
blocks in `poll(2)` on the socket plus a stateful doorbell (eventfd,
kqueue EVFILT_USER or a Windows Event) and is woken by the doorbell, by socket readability,
or by a PING deadline. Producers waiting for acks park and are unparked by the
I/O thread. These two models check the hand-offs.

| Model | What it checks |
|---|---|
| `IdleWait.tla` | Producer publish/ring vs. the loop's idle decision (`ioParked`, `publishedBeforeWork` re-check, `recycledThisIteration`), with server drops, reconnect cursor rewinds and `close()`. Invariants: `NoStrandedWork`, `NoWaitAfterRecycle`, `TypeOK`. Liveness: every published frame is acked (`AllAcked`), `close()` stops the loop (`Terminates`). |
| `AckWait.tla` | The `AckWaiters` hand-off (register, re-check, park vs. advance, signal): a waiter never parks on progress it has not been signalled about (`NoLostAck`); reaching the target returns it (`Returns`). One waiter modelled; the registry holds any number, each following the same protocol. |

`IdleWait_bug.cfg` sets `RecycleFix = FALSE`, the design before a reconnect
counted as work for its iteration. TLC finds the stranded wait in 10 steps,
the hang `ReconnectTest.testReplayResendsUnackedFramesAcrossReconnect` hit.

Run (needs `tla2tools.jar` from https://github.com/tlaplus/tlaplus/releases):

```
java -cp tla2tools.jar tlc2.TLC -workers 8 -config IdleWait_fix.cfg IdleWait.tla
java -cp tla2tools.jar tlc2.TLC -workers 8 -config IdleWait_bug.cfg IdleWait.tla
java -cp tla2tools.jar tlc2.TLC -config AckWait.cfg AckWait.tla
```

Modeling notes: one Java statement per step where threads interact (volatile
write, then flag read), coarser elsewhere. `MaxFrames` and `MaxDrops` bound
the state space; drops are adversarial but not fair, acks are strongly fair.
