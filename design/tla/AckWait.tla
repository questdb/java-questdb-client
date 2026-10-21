-------------------------------- MODULE AckWait --------------------------------
(* Ack wait: QwpWebSocketSender.awaitAckedFsn() / drainOnClose() against the    *)
(* I/O thread's AckWaiters.signal(), replacing the 50us parkNanos poll.  The    *)
(* waiter registers once (AckWaiters.add), then loops: re-read ackedFsn, park.  *)
(* It deregisters when done.  One waiter is modelled; the registry holds any    *)
(* number, each following this protocol independently.                         *)
(*                                                                              *)
(* LockSupport semantics: unpark sets a one-shot permit; park consumes it and   *)
(* returns at once if it was already set.  We ignore deadlines and spurious     *)
(* wakeups -- both only make the wait return earlier, and the caller re-checks. *)
EXTENDS Naturals

CONSTANT Target   \* the FSN the waiter waits for

VARIABLES acked,   \* engine.ackedFsn(), written by the I/O thread
          seen,    \* the watermark the waiter read on its last check
          waiter,  \* registered in AckWaiters
          permit,  \* the waiter thread's park permit
          ppc,     \* waiter pc
          iopc     \* I/O thread pc

vars == <<acked, seen, waiter, permit, ppc, iopc>>

Init == /\ acked = 0 /\ seen = 0 /\ waiter = FALSE /\ permit = FALSE
        /\ ppc = "register" /\ iopc = "idle"

----------------------------------------------------------------------------
(* Waiter: ackWaiters.add(me); loop { check; parkNanos }; ackWaiters.remove.   *)
Register == /\ ppc = "register" /\ waiter' = TRUE /\ ppc' = "read"
            /\ UNCHANGED <<acked, seen, permit, iopc>>

Read == /\ ppc = "read" /\ seen' = acked          \* getAckedFsn() >= targetFsn ?
        /\ IF acked >= Target THEN waiter' = FALSE /\ ppc' = "done"
                               ELSE waiter' = waiter /\ ppc' = "park"
        /\ UNCHANGED <<acked, permit, iopc>>

(* parkNanos: blocks until the permit is available, consumes it.             *)
Park == /\ ppc = "park" /\ permit
        /\ permit' = FALSE /\ ppc' = "read"
        /\ UNCHANGED <<acked, seen, waiter, iopc>>

----------------------------------------------------------------------------
(* I/O thread: engine.acknowledge() (volatile write) then signal() reads the  *)
(* registry and unparks.  Two steps: the write and the read are not atomic    *)
(* together.                                                                  *)
Ack == /\ iopc = "idle" /\ acked < Target
       /\ acked' = acked + 1 /\ iopc' = "signal"
       /\ UNCHANGED <<seen, waiter, permit, ppc>>

Signal == /\ iopc = "signal"
          /\ permit' = (permit \/ waiter) /\ iopc' = "idle"
          /\ UNCHANGED <<acked, seen, waiter, ppc>>

Next == Register \/ Read \/ Park \/ Ack \/ Signal
        \/ (ppc = "done" /\ acked = Target /\ UNCHANGED vars)

Spec == Init /\ [][Next]_vars
        /\ WF_vars(Register \/ Read \/ Park)
        /\ WF_vars(Ack \/ Signal)

----------------------------------------------------------------------------
(* SAFETY: the waiter is never parked with unsignalled progress.  If it is    *)
(* parked and the watermark moved past what it saw, the permit is set or the  *)
(* I/O thread is about to set it (it has the registration visible).           *)
NoLostAck == (ppc = "park" /\ acked /= seen) => (permit \/ (iopc = "signal" /\ waiter))

(* LIVENESS: reaching the target eventually returns the waiter.               *)
Returns == (acked >= Target) ~> (ppc = "done")
=============================================================================
