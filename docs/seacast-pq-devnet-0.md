# SEACAST's payload path on pq-devnet-0

This branch carries SEACAST's payload path: the execution payload envelope is cut into chunks and spread over
gossipsub, through a fork of go-libp2p-pubsub vendored in `third_party/go-libp2p-pubsub`. The path was first written
on `develop`. This note records where this branch differs from that version and the choices behind each difference,
so that each can be reviewed and undone.

Every choice below can be undone. Where the undo isn't obvious, the note says how.

## The bases

- **`develop`:** the path was written on `develop` at `0c1e3d248a` ("add hdiff kv metrics (#17893)").
- **This branch:** on `pq-devnet-0` at `3c86e2d1b0` ("Stop the beacon REST server closing connections the validator
  still uses", 2026-11-23), that exact commit, not the moving branch.
- `pq-devnet-0` leaves `develop` at `844d8b8df5` ("Drop Gloas from unsupportedVersions (#17853)"), 15 commits before
  `0c1e3d248a`.

These 15 `develop` commits are therefore not on this branch:

- #17893 add hdiff kv metrics
- #17891 Metrics for archive node and backfill progress
- #17892 add stategen metrics
- #17868 Do not walk all validators with `ExitInformation` for post-Electra blocks with voluntary exits or slashings
- #17871 Request a missing block from the best peers if it is an ancestor of a block from the proposer expected by
  the head state; otherwise request it only from the peer that referenced it, and downscore that peer if it does
  not serve it
- #17889 Update changelog for v7.3.2
- #17765 Fix broken section link in state diff design doc
- #17885 Fix LC P2P request decoding for bootstrap and updates-by-range
- #17869 Skip null entries when importing slashing protection into the minimal database
- #17886 fix gloas light client gossip timing
- #17856 Check finalized descendant when no fork choice blocks are missing
- #17859 Avoid overflow in envelopes by range response slot check
- #17857 Pack slashings on a copy of the head state during block production
- #17861 Mark proposer slashings seen before inserting them into the pool
- #17884 Fix overflow in LC REST handler

To bring one back, cherry-pick it onto this branch. Only #17871 touches code the path uses; see the first item
under "Behaviour".

`pq-devnet-0` widens the envelope code so that it can hold PQ envelopes as well as Gloas ones, in the functions the
path changes:

- The sync service's pending envelopes are held as `interfaces.ROSignedExecutionPayloadEnvelope`, not
  `*ethpb.SignedExecutionPayloadEnvelope`.
- `validateExecutionPayloadEnvelope` and `executionPayloadEnvelopeSubscriber` switch on the message's type.
- The envelopes-by-root fetch returns the read-only interface.

## Behaviour that differs from the path on develop

1. **A pending envelope's unknown block is asked of the best peers**, on the gossip path and on the segment path.
   On `develop`, the gossip path asked the peer that sent the envelope, and penalised it if it didn't serve the block
   (`requestBlocksAccountable`, from #17871). `pq-devnet-0` doesn't have #17871, so `admitExecutionPayloadEnvelope`
   and `queuePendingPayloadEnvelopeFrom` have no `pid` parameter. To undo, cherry-pick #17871 and restore the
   accountable request on the gossip path.
2. **The gossip path accepts PQ envelopes, as `pq-devnet-0` does.** The validator wraps either type and passes the
   read-only interface to `admitExecutionPayloadEnvelope`; `msg.ValidatorData` takes `e.Proto()`. The segment path
   carries both types too (see "The PQ envelope on the segment path" below). If wrapping a reassembled envelope
   fails, the segment is still accepted and the envelope isn't imported, as with any other refusal on that path.
3. **A malformed pending entry is one whose `Envelope()` fails**, as in `pq-devnet-0`'s own loop: a nil message or
   payload, or a block root or parent root that isn't 32 bytes long. On `develop` the path checked `Message` and
   `Payload` by hand. Here the segment path's room rule (`makeRoomForReassembledEnvelope`, through
   `pendingEnvelopeOf`) and the pruning use `Envelope()` as well. So an entry with a root of the wrong length
   counts as malformed there too.
4. **A fetch mark identifies the message, not the wrapper**: `isFetchedEnvelope` compares `Proto()` pointers. The
   read-only wrapper is a struct holding one pointer, so comparing the interfaces would give the same answer today.
   It would panic, though, if a wrapper ever held a slice.
5. **With the segment path off, the pending queue behaves as `pq-devnet-0`'s does.** The first well-formed entry
   decides the gossip path's slot check for its root, and the first entry the map yields decides its pruning.
   `develop`'s code did the same.
6. **`--seacast-payload-only` keeps every envelope off the network, Gloas and PQ alike.** The segment path carries
   both types (below), so the pending queue rebroadcasts no envelope whole under the switch, as on `develop`.

## The PQ envelope on the segment path

On `pq-devnet-0` the PQ fork is active from genesis, so every envelope is a `SignedExecutionPayloadEnvelopePQ`.
SEACAST carries it as it carries a Gloas one:

- **Publishing.** `PublishExecutionPayloadEnvelopeContentsPQ` sends the envelope the way the Gloas call does: with
  `--seacast-payload-only` as segments only, falling back to the whole envelope for the same reasons and with the same
  counter; without it, the whole envelope and its segments beside it. `SegmentMessagesForEnvelope` segments either
  type from its own SSZ encoding.
- **Reassembling.** A completed group decodes as the envelope of the segment topic's fork: the topic's digest gives
  the fork's epoch, and the envelope topic's type at that epoch is the one decoded (PQ from `PQ_FORK_EPOCH`, Gloas
  before it). The type follows the digest, not the node's clock, so a group published just before a fork boundary
  decodes as its own fork's envelope. Bytes that don't decode as that type are rejected.
- **Unchanged.** The segment topic, its message ids, the first-seen authority and the router's payload type carry
  bytes and don't depend on the envelope's type. The envelope's own checks, its import and the relay without
  payload-only are `pq-devnet-0`'s, which take either type.

Tests: `TestSegmentMessagesForEnvelopePQ`, `TestSegmentValidatorTypeFollowsTheDigest`,
`TestPublishPQEnvelopeSegments` and `TestPendingPQEnvelopeRebroadcast`. Two controls in them: a Gloas envelope on the
Gloas fork's topic, and a PQ envelope's rebroadcast without payload-only.
