-------------------------------- MODULE AckWait --------------------------------
(* Producer-side ack wait: CursorWebSocketSendLoop.awaitAckProgress() and the  *)
(* I/O thread's signalAckProgress(), replacing the 50us parkNanos poll in       *)
(* QwpWebSocketSender.awaitAckedFsn()/drainOnClose().                           *)
(*                                                                              *)
(* LockSupport semantics: unpark sets a one-shot permit; park consumes it and   *)
(* returns at once if it was already set.  We ignore deadlines and spurious     *)
(* wakeups -- both only make the wait return earlier, and the caller re-checks. *)
EXTENDS Naturals

CONSTANT Target   \* the FSN the producer waits for

VARIABLES acked,   \* engine.ackedFsn(), written by the I/O thread
          seen,    \* the watermark the producer read before deciding to wait
          waiter,  \* ackWaiter != null
          permit,  \* the producer thread's park permit
          ppc,     \* producer pc
          iopc     \* I/O thread pc

vars == <<acked, seen, waiter, permit, ppc, iopc>>

Init == /\ acked = 0 /\ seen = 0 /\ waiter = FALSE /\ permit = FALSE
        /\ ppc = "read" /\ iopc = "idle"

----------------------------------------------------------------------------
(* Producer: QwpWebSocketSender loop body + awaitAckProgress(seen, deadline). *)
Read == /\ ppc = "read" /\ seen' = acked          \* long seen = engine.ackedFsn()
        /\ ppc' = (IF acked >= Target THEN "done" ELSE "setWaiter")
        /\ UNCHANGED <<acked, waiter, permit, iopc>>

SetWaiter == /\ ppc = "setWaiter" /\ waiter' = TRUE /\ ppc' = "recheck"
             /\ UNCHANGED <<acked, seen, permit, iopc>>

Recheck == /\ ppc = "recheck"
           /\ IF acked /= seen THEN waiter' = FALSE /\ ppc' = "read"
                               ELSE waiter' = waiter /\ ppc' = "park"
           /\ UNCHANGED <<acked, seen, permit, iopc>>

(* parkNanos: blocks until the permit is available, consumes it.             *)
Park == /\ ppc = "park" /\ permit
        /\ permit' = FALSE /\ waiter' = FALSE /\ ppc' = "read"
        /\ UNCHANGED <<acked, seen, iopc>>

----------------------------------------------------------------------------
(* I/O thread: engine.acknowledge() (volatile write) then signalAckProgress() *)
(* reads ackWaiter and unparks.  Two steps: the write and the read are not    *)
(* atomic together.                                                           *)
Ack == /\ iopc = "idle" /\ acked < Target
       /\ acked' = acked + 1 /\ iopc' = "signal"
       /\ UNCHANGED <<seen, waiter, permit, ppc>>

Signal == /\ iopc = "signal"
          /\ permit' = (permit \/ waiter) /\ iopc' = "idle"
          /\ UNCHANGED <<acked, seen, waiter, ppc>>

Next == Read \/ SetWaiter \/ Recheck \/ Park \/ Ack \/ Signal
        \/ (ppc = "done" /\ acked = Target /\ UNCHANGED vars)

Spec == Init /\ [][Next]_vars
        /\ WF_vars(Read \/ SetWaiter \/ Recheck \/ Park)
        /\ WF_vars(Ack \/ Signal)

----------------------------------------------------------------------------
(* SAFETY: the producer is never parked with unsignalled progress.  If it is  *)
(* parked and the watermark moved past what it saw, the permit is set or the  *)
(* I/O thread is about to set it (it has the waiter flag visible).            *)
NoLostAck == (ppc = "park" /\ acked /= seen) => (permit \/ (iopc = "signal" /\ waiter))

(* LIVENESS: reaching the target eventually returns the producer.             *)
Returns == (acked >= Target) ~> (ppc = "done")
=============================================================================
