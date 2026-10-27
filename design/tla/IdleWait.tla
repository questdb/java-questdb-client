------------------------------- MODULE IdleWait -------------------------------
(* Idle-wait protocol of CursorWebSocketSendLoop (java-questdb-client).         *)
(*                                                                              *)
(* Threads: the producer (publishes frames, rings the doorbell), the I/O loop   *)
(* (sends, receives acks, reconnects, blocks when idle) and the environment     *)
(* (server acks, connection drops, close()).                                    *)
(*                                                                              *)
(* Doorbell = eventfd / EVFILT_USER / Event polled with the socket: a STATEFUL  *)
(* flag a wait consumes.  parkedFlag = ioParked.  snap = publishedBeforeWork.   *)
(* recycled = recycledThisIteration (RecycleFix = FALSE models the bug found    *)
(* by ReconnectTest: a reconnect rewinds the cursor and the loop blocks on it). *)
(* A publish is TWO volatile stores, publishedFsn then the bytes; the loop can  *)
(* only send below the bytes.  StampFix = TRUE snapshots publishStamp (bumped   *)
(* after the bytes); FALSE snapshots publishedFsn, the review-found stranding.  *)
EXTENDS Naturals

CONSTANTS MaxFrames,   \* frames the producer publishes in total
          MaxDrops,    \* connection drops the environment may inject
          RecycleFix,  \* TRUE: swapClient() counts as work for the iteration
          StampFix     \* TRUE: the idle re-check keys on publishStamp

VARIABLES published,  \* frames whose FSN is published (volatile publishedFsn)
          bytes,      \* frames whose bytes are published (publishedOffset)
          sent,       \* send cursor: frames sent on the current connection
          acked,      \* server-acknowledged watermark
          bell,       \* doorbell rung and not yet consumed
          parkedFlag, \* ioParked
          pc,         \* I/O thread program counter
          snap,       \* publishedBeforeWork
          recycled,   \* recycledThisIteration
          didWork,
          running,    \* loop not closed
          prodPc,     \* producer program counter
          serverDrop, \* the current connection has been dropped by the peer
          drops       \* drops injected so far

vars == <<published, bytes, sent, acked, bell, parkedFlag, pc, snap, recycled,
          didWork, running, prodPc, serverDrop, drops>>

Init == /\ published = 0 /\ bytes = 0 /\ sent = 0 /\ acked = 0
        /\ bell = FALSE /\ parkedFlag = FALSE
        /\ pc = "top" /\ snap = 0 /\ recycled = FALSE /\ didWork = FALSE
        /\ running = TRUE /\ prodPc = "idle"
        /\ serverDrop = FALSE /\ drops = 0

----------------------------------------------------------------------------
(* Producer: store publishedFsn, store the bytes (+ stamp), THEN read ioParked *)
(* and ring.  Key = what the loop snapshots.                                    *)
Key == IF StampFix THEN bytes ELSE published

PublishFsn == /\ prodPc = "idle" /\ published < MaxFrames
              /\ published' = published + 1 /\ prodPc' = "bytes"
              /\ UNCHANGED <<bytes, sent, acked, bell, parkedFlag, pc, snap,
                             recycled, didWork, running, serverDrop, drops>>

PublishBytes == /\ prodPc = "bytes"
                /\ bytes' = published /\ prodPc' = "ring"
                /\ UNCHANGED <<published, sent, acked, bell, parkedFlag, pc, snap,
                               recycled, didWork, running, serverDrop, drops>>

Ring == /\ prodPc = "ring"
        /\ bell' = (bell \/ parkedFlag)          \* wakeIoIfParked()
        /\ prodPc' = "idle"
        /\ UNCHANGED <<published, bytes, sent, acked, parkedFlag, pc, snap, recycled,
                       didWork, running, serverDrop, drops>>

(* close(): drainOnClose() first (acked = published), then running := FALSE,  *)
(* then ring unconditionally.                                                 *)
Close == /\ prodPc = "idle" /\ running /\ published = MaxFrames /\ acked = MaxFrames
         /\ running' = FALSE /\ bell' = TRUE /\ prodPc' = "closed"
         /\ UNCHANGED <<published, bytes, sent, acked, parkedFlag, pc, snap, recycled,
                        didWork, serverDrop, drops>>

----------------------------------------------------------------------------
(* Environment: the peer drops the connection.                                *)
ServerDrop == /\ ~serverDrop /\ drops < MaxDrops /\ running
              /\ serverDrop' = TRUE /\ drops' = drops + 1
              /\ UNCHANGED <<published, bytes, sent, acked, bell, parkedFlag, pc, snap,
                             recycled, didWork, running, prodPc>>

----------------------------------------------------------------------------
(* I/O thread.                                                                *)
Top == /\ pc = "top"
       /\ IF running THEN snap' = Key /\ pc' = "send"
                     ELSE snap' = snap /\ pc' = "done"
       /\ UNCHANGED <<published, bytes, sent, acked, bell, parkedFlag, recycled,
                      didWork, running, prodPc, serverDrop, drops>>

(* trySendOne(): send the next frame, or find nothing, or hit the drop.       *)
SendOne == /\ pc = "send" /\ ~serverDrop /\ sent < bytes
           /\ sent' = sent + 1 /\ didWork' = TRUE /\ pc' = "recv"
           /\ UNCHANGED <<published, bytes, acked, bell, parkedFlag, snap, recycled,
                          running, prodPc, serverDrop, drops>>

SendNone == /\ pc = "send" /\ ~serverDrop /\ sent = bytes
            /\ didWork' = FALSE /\ pc' = "recv"
            /\ UNCHANGED <<published, bytes, sent, acked, bell, parkedFlag, snap,
                           recycled, running, prodPc, serverDrop, drops>>

(* fail() -> connectLoop() -> swapClient(): cursor rewinds to acked+1.        *)
Reconnect(from, to) == /\ pc = from /\ serverDrop
                       /\ sent' = acked /\ recycled' = TRUE /\ serverDrop' = FALSE
                       /\ didWork' = (IF from = "send" THEN FALSE ELSE didWork)
                       /\ pc' = to
                       /\ UNCHANGED <<published, bytes, acked, bell, parkedFlag, snap,
                                      running, prodPc, drops>>

(* tryReceiveAcks(): an ack arrives, or nothing is there, or EOF (drop).      *)
RecvAck == /\ pc = "recv" /\ ~serverDrop /\ acked < sent
           /\ acked' \in (acked + 1)..sent /\ didWork' = TRUE /\ pc' = "check"
           /\ UNCHANGED <<published, bytes, sent, bell, parkedFlag, snap, recycled,
                          running, prodPc, serverDrop, drops>>

RecvNone == /\ pc = "recv" /\ pc' = "check"
            /\ UNCHANGED <<published, bytes, sent, acked, bell, parkedFlag, snap,
                           recycled, didWork, running, prodPc, serverDrop, drops>>

(* Idle decision, including the recycledThisIteration consumption.            *)
Check == /\ pc = "check"
         /\ LET work == didWork \/ (RecycleFix /\ recycled)
            IN /\ recycled' = FALSE
               /\ pc' = (IF work THEN "top" ELSE "setParked")
         /\ UNCHANGED <<published, bytes, sent, acked, bell, parkedFlag, snap, didWork,
                        running, prodPc, serverDrop, drops>>

SetParked == /\ pc = "setParked" /\ parkedFlag' = TRUE /\ pc' = "recheck"
             /\ UNCHANGED <<published, bytes, sent, acked, bell, snap, recycled,
                            didWork, running, prodPc, serverDrop, drops>>

Recheck == /\ pc = "recheck"
           /\ IF Key /= snap
                THEN parkedFlag' = FALSE /\ pc' = "top"
                ELSE parkedFlag' = parkedFlag /\ pc' = "wait"
           /\ UNCHANGED <<published, bytes, sent, acked, bell, snap, recycled, didWork,
                          running, prodPc, serverDrop, drops>>

(* awaitReadableOrWake(): returns on doorbell, on socket readability (an ack  *)
(* or EOF is coming because frames are in flight or the peer dropped), or     *)
(* never otherwise.  Consumes the doorbell.                                   *)
Wake == /\ pc = "wait"
        /\ (bell \/ acked < sent \/ serverDrop)
        /\ bell' = FALSE /\ parkedFlag' = FALSE /\ pc' = "top"
        /\ UNCHANGED <<published, bytes, sent, acked, snap, recycled, didWork,
                       running, prodPc, serverDrop, drops>>

IoStep == Top \/ SendOne \/ SendNone \/ Reconnect("send", "recv")
          \/ RecvAck \/ RecvNone \/ Reconnect("recv", "check")
          \/ Check \/ SetParked \/ Recheck \/ Wake

Next == PublishFsn \/ PublishBytes \/ Ring \/ Close \/ ServerDrop \/ IoStep
        \/ (pc = "done" /\ prodPc = "closed" /\ UNCHANGED vars)

(* The I/O thread and the producer's ring are scheduled fairly; acks are      *)
(* eventually delivered (strong: RecvNone may interleave).  Drops are not     *)
(* fair: the adversary may stop dropping, and is bounded anyway.              *)
Fairness == /\ WF_vars(IoStep) /\ WF_vars(PublishBytes) /\ WF_vars(Ring)
            /\ WF_vars(Close)
            /\ SF_vars(RecvAck)

Spec == Init /\ [][Next]_vars /\ Fairness

----------------------------------------------------------------------------
(* SAFETY.  Never block with unannounced work: whenever the I/O thread is in  *)
(* the blocking wait and frames are unsent, either the doorbell is rung, or   *)
(* the producer is one step from ringing it and will see the parked flag, or  *)
(* the socket is about to become readable.                                    *)
NoStrandedWork ==
    (pc = "wait" /\ sent < bytes) =>
        (bell \/ (prodPc = "ring" /\ parkedFlag) \/ serverDrop \/ acked < sent)

(* The loop never waits on a cursor it rewound in the same iteration.         *)
NoWaitAfterRecycle == pc = "wait" => ~recycled

TypeOK == /\ published \in 0..MaxFrames /\ bytes \in 0..MaxFrames
          /\ sent \in 0..MaxFrames /\ acked \in 0..MaxFrames
          /\ acked <= sent /\ sent <= bytes /\ bytes <= published

(* LIVENESS.  Every published frame is eventually acknowledged, and close()   *)
(* eventually stops the loop.                                                 *)
AllAcked == \A f \in 1..MaxFrames : (published >= f) ~> (acked >= f)
Terminates == (~running) ~> (pc = "done")
=============================================================================
