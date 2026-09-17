# Segmented payload gossip: status and open items

Behind `--enable-segmented-payload-gossip` (off by default), a beacon node publishes an execution
payload envelope it receives from its validator as a group of authenticated segments on the
`execution_payload_segment` topic, in addition to the whole envelope, and reassembles segments it
receives from peers. The group is the envelope compressed first, cut into as many segments as
16 KiB gives its SSZ bytes, with as many Reed-Solomon parity segments again; any half of the group
completes a receiver. On that topic only, gossipsub runs phase forwarding with a disciplined pull
path: a segment is pushed to a few mesh peers not known to hold it, four to 256 KiB of payload,
three to 1 MiB and two above, and announced to the rest; a receiver keeps one request outstanding
per segment, moves on after 200 ms, parks an announcer that leaves a request unserved for 400 ms,
re-asks the next remembered announcer, and stops asking for a group's segments once it has
completed the group. Segment message ids carry the segment's group root and index ahead of the
content id, which is what lets a receiver decline at announcement time. The policy lives in a
go-libp2p-pubsub fork, branch `variant-a` on v0.17.0.

The measurement behind it is the study this branch was extracted from. On a 500-node simulated
mesh with 1 MiB payloads over home links, the plain 32 KiB segments with the disciplined policy
cut median completion from 4.9 s to 0.73 s and received bytes from 4.4 to 1.4 payload copies per
node, against whole-message diffusion; 16 KiB takes about a tenth more off; the push width by size
and the coded, compress-first group each take more off the tail. The post and the branch that made
every figure are linked at the end. This file says what of the design is in this branch, what is
proposed protocol, what exists only in the study's harness, and what is known to be open.

## What is what

The series is built in blocks so that each tier of the study's recommendation is a prefix of it:
plain 16 KiB segments with the structured id (tier 1), then the fork's policy with the push width by
size (tier 2), then the coded, compress-first group with stop-pull (tier 3).

| part | status |
|---|---|
| segment format, Merkle commitment, per-segment proof, coded groups, content encoding (`container/segments`; the wire type is the SSZ container `ExecutionPayloadSegment`) | in this branch, as proposed |
| segment topic, batch publish, publish off the RPC path, structured message ids on the topic (`beacon-chain/p2p`, `beacon-chain/rpc`) | in this branch |
| validation cheapest-first, per-peer budget on opening groups, bounded reassembler completing once at the required count, decompression, hand-off to the pending envelope queue (`beacon-chain/sync`) | in this branch |
| phase forwarding with the push width by payload size, IWANT discipline, commitment park, offer table (fork) | in this branch, installed on the segment topic only |
| the request gate (fork) | in this branch as stop-pull: it declines the segments of a group this node has completed. Its other use, declining what no block has committed to, waits for the commitment |
| **authentication of a group** | **proposed:** the builder commits to the segmentation in its signed bid, and a receiver admits only groups the bid names. **In this branch:** `segmentauth.NewFirstSeen` admits the first group offered in a slot, keyed on the node's own clock. It bounds groups and establishes nothing about who published them; it must not ship as the real scheme, and the code says so |
| a pending-segment store | not built: a segment that arrives before its block is dropped with Ignore and must arrive again |
| the adversaries of the stress cells, the per-run knobs of the study | harness only, in the study's snapshot branch; measured in the post, not in this branch |

## Open items

Found by the reviews of this series and left open deliberately, with where each lives.

1. **The offer horizon outlives the message cache** (fork; liveness, medium). An announcer is
   remembered for 30 s while gossipsub's cache serves for about 4 s (HistoryLength × heartbeat), so
   a late re-ask can go to a peer that no longer holds the segment. The whole-envelope topic
   bounds the damage. Fix: select announcers only within the cache's window, from a first-seen
   timestamp that re-announcements cannot refresh; keep the 30 s only for memory.
2. **The announcement limit is raised router-wide** (this branch and the fork). Phase forwarding
   announces each forwarded segment in its own IHAVE, so `MaxIHaveMessages` goes from 10 to 1024
   while the flag is on. The parameter is per router, so every topic's limit rises. Fix: per-topic
   accounting in the fork, then drop the raise here.
3. **A phase announcement is one-shot** (fork; low to medium). A peer that misses the IHAVE gets no
   second one from that neighbour, since heartbeat gossip excludes mesh peers; other neighbours
   cover in practice. Fix: re-offer trimmed pushes through heartbeat gossip.
4. **A reassembled group that is not an envelope is rejected** (this branch; design-bound). The
   relay that delivered the completing segment pays for the builder's fault; the same holds for a
   group whose bytes do not decompress or whose segments are not a codeword. Under a bid
   commitment the fault is attributable to the builder; until then Ignore, and dropping the group,
   is the honest mapping.
5. **The SSZ decoder ignores the container's maximum** (upstream, methodical-ssz; low). An
   oversized segment container is decoded before it is rejected. To be reported upstream.
6. **First-seen hardening** (this branch; interim). Refuse descriptors whose segment size, hash,
   encoding or shape are not the production defaults, or whose total length exceeds the envelope
   maximum. The real fix is the bid commitment.
7. **The message id's width is a choice** (this branch). Control traffic scales with the id: the
   study's harness carried a 101-byte id, this branch a 56-byte one (root, index, content id).
   Measured through the study's harness at 500 nodes and 1 MiB, ten seeds: the 56-byte id costs
   about 119 KB of control per node on the plain group and 129 KB on the coded one, against 56 KB
   at Prysm's 20-byte id and 192 KB at the harness's 101 bytes, so the post's control panels
   overstate this branch by about a factor of 1.6; completion moves by two percent or less.
8. **The Reed-Solomon code is the table implementation** (this branch; performance). Correct and
   dependency-free, one to two orders of magnitude slower than a SIMD library; the benchmarks in
   `container/segments` say what it costs at 1 MiB. Switch to klauspost/reedsolomon before any
   deployment.
9. **Envelopes above 2 MiB of SSZ go uncoded** (this branch). The code has 256 points and the
   group uses two per 16 KiB of envelope, so a larger envelope is published as a plain compressed
   group. A wider field or a larger segment size at that scale is a design decision, not made here.
Smaller, also open: the request gate's `Replay` deadlocks if called from inside `Allow` or
`Committed` (fork; the pull gate installs no deferral); when every announcer of a segment is
temporarily ineligible the retry chain ends until a new announcement arrives (fork; latency only,
honest load).

## Deferred until the bid carries the commitment

- **Admission design.** Authority as an OR of sources, an unstructured-id bypass, global count
  ceilings, eviction before admissibility, and the cap's refund all wait for a commitment to test
  against. Fixes are outlined in the study's notes.
- **IDONTWANT spoofing collapses the push budget.** The forwarding budget is the degree minus the
  peers believed to hold the message, and that belief comes from unauthenticated IDONTWANTs. Two
  colluding mesh peers can force a node into pull-only forwarding without suppressing honest pulls.
  A measured performance risk, not a safety one; a bid commitment would not prove possession, so
  the fix is a push floor, priced in bytes.
- **Per-peer measured timers.** The 200 ms window and 400 ms promise are constants tuned for home
  links. The next step takes samples and deadlines from one reference point, a mid quantile for
  the window and a high one for the promise, with a floor, a ceiling and a population prior, and
  re-measures the ladder.

## Verification

Every commit of this series builds and passes its tests on its own, under `go test` and under
Bazel. The tier 1 policy was run through the study's own harness against the research fork it was
extracted from: at 500 nodes and 1 MiB, six seeds at 32 KiB and at 16 KiB, medians within 2
percent and received bytes within 1 percent of the research arm. The records are in the study's
notes. Tiers 2 and 3 were run the same way, this branch's publisher, option bundle and pull gate
inside the harness against the study's size-adaptive and compress-first coded arms: at 500 nodes
and 1 MiB, ten seeds, with the ids at the harness's width the paired medians are within 3 percent
on completion, within 2 percent on received data for tier 2 and within 2.2 percent for tier 3
(the harness's own coded arm on this fork sits 3 percent above the research rows, so the
difference is the fork's fix commits, not this code), and within 5 percent on control. With this
branch's 56-byte id the timing is the same and control is a third lower; see open item 7.

## Pointers

- The post: (link at publication).
- The branch that made every figure: `payload-segmentation-snapshot` in this repository, with
  the runner, the cell lists behind each figure, the extractor and the figure script under
  `testing/segstudy/`; a research harness, not a proposal.
- The fork: `github.com/cskiraly/go-libp2p-pubsub`, branch `variant-a`.
