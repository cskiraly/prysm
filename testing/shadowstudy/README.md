# shadowstudy — the harness under Shadow

The cross-check of the in-process harness under a second simulator: the
published 500-node cells re-run with one real process per node under the
[Shadow](https://shadow.github.io/) discrete-event simulator, on real sockets (TCP+yamux or
UDP+quic-go), with Shadow's per-host bandwidth and per-pair latency set from the harness's own
topology draw.

What stays the same: the arm builders, the knob-to-option mapping, the payload, the tracer and the
completion rule. They are the harness's, because the node is a test function in the harness
package (`beacon-chain/p2p/segmentintegrationtest/shadow_node_test.go`) and the fleet runner's
environment selects the cell. What changes: the network is Shadow's, and the driver's barriers
(peer up, mesh settled, publish) are fixed instants on the simulated clock.

## Pieces

The generic half is [eth-networking-lab](https://github.com/cskiraly/eth-networking-lab)'s
`shadowsim` package and its `shadowsim/tools/`: the node's schedule and host on real sockets, the
topology format, the report line, the generator, the stall watchdog, the extractor, the pairing
script and the lane driver. `go.mod` points at a checkout beside this tree
(`replace ... => ../eth-networking-lab`; `LAB` relocates it for the scripts). This directory is the
study's half:

- `TestShadowExport` (in the harness package): `SHADOW_EXPORT=topo.json` plus the cell's
  `SEGMENT_*` environment writes the graph, the per-node links and the one-way latency matrix,
  drawn by the harness's own generators.
- `TestShadowNode`: one node. Reads the lab's `SHADOW_*` schedule, resolves the cell from the
  fleet runner's `SEGMENT_*` environment (`q6CellFromEnv`), and runs the published arm builders,
  tracer and completion rule. Node 0 publishes; every node prints one `SHADOWSTAT {json}` line.
- `arms.sh`: the arm catalogue, verbatim from the fleet runner, plus `NET` (home, dc20, dcb20,
  upN) for the posts' network scenarios. Read by both substrates.
- `cell.sh SUBSTRATE ARM SEED TRANSPORT N PAY [NET]`: one cell on `shadow` or `harness`,
  idempotent (skips a cell whose record is ok), so the lab's `lane.sh` resumes and refills.
- `runcell.sh ARM SEED [TRANSPORT] [N] [PAY]` (Shadow: export, generate, run under the watchdog,
  extract) and `harnesscell.sh ARM SEED [N] [PAY]` (the in-process driver on the same cell).
- `cells500.txt`: the published 500-node cell, ten seeds, four arms, QUIC then TCP.

```sh
# the whole lane, one cell at a time, resumable
STUDY=$PWD ../../../eth-networking-lab/shadowsim/tools/lane.sh cells500.txt
# pair with the same-tree twins and with the published cells
python3 ../../../eth-networking-lab/shadowsim/tools/compare.py --shadow ../results \
  --harness ../results ../segstudy/results/fu_results.json --transport quic
```

## Building the node binary

Shadow interposes with `LD_PRELOAD`, so the binary must be dynamically linked. Build it on the
machine that runs Shadow, against its glibc, and leave the commit next to the binary:

```sh
cd /path/to/prysm
CGO_ENABLED=1 go test -c -ldflags=-linkmode=external -o testing/bin/segshadow.test ./beacon-chain/p2p/segmentintegrationtest
git rev-parse HEAD > testing/bin/segshadow.test.commit   # extract.py puts it in every record
ldd testing/bin/segshadow.test | head -3   # must not say "not a dynamic executable"
```

`testing/bin/segshadow.test` is where the scripts look by default; `BIN` relocates it. Records
and logs go to `testing/results/` (`RES`), Shadow's working directories to `testing/out/` (`OUT`).

## Running

```sh
# known answer: two nodes, one link, whole message
./runcell.sh line 1 quic 2
# a published cell, on each substrate
./cell.sh shadow wholend 7 quic 500 p1m
./cell.sh shadow aadaptcf_16k 7 tcp 500 p1m
./cell.sh harness atuned_32k 7 quic 500 p1m
```

Determinism: identical inputs and seeds do not guarantee identical results, even within a
session (the lab's tools README has the measurements). Treat each run as one sample and compare
medians over paired seeds.
