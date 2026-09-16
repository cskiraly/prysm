# The payload-segmentation study: runner, cells, extractor, figures

Everything behind the figures of the follow-up post, in the tree that produced them. This
directory and the branch it sits on are a research harness, not a proposal. The code the post
recommends is the `variant-a` series (prysm branch `variant-a`, go-libp2p-pubsub branch
`variant-a`, one rung per commit, behind a flag); this branch is the snapshot that made the
numbers, byte for byte, with the two forks it measured pinned as modules.

## What is here

| file | role |
|---|---|
| `runfu.sh` | the per-arm recipe. `runfu.sh ARM SEED FAULT NET PAY` maps an arm name to the harness's `SEGMENT_*` environment and runs one cell of `TestQ6RealisticMesh` (the A arms and whole message) or of the B and C tests. The header lists the arms, faults, networks and payload sizes; `N` in the environment sets the node count (500 by default) |
| `runcell.sh` | one fleet cell: `runcell.sh ARM SEED FAULT NET PAY [N]` runs `runfu.sh` under `/usr/bin/time -v` and writes `logs/<tag>.log`, `.time` and `.done`; a cell whose `.done` exists is skipped, so a cell list can be rerun to fill gaps |
| `cells/figure<N>_<name>.txt` | the cells each figure of the post draws, one per line in `runcell.sh` argument order (`ARM SEED FAULT NET PAY N`); `part1_all.txt` is their union, `results_all.txt` every cell in the results file, `fixedcount_all.txt` the fixed-count cells of section 5 (kept in their own results file because their arm names collide with the fixed-size cells) |
| `fu_extract.py` | one JSON record per cell log: `fu_extract.py logs/ results/mine.json` |
| `fu_figures.py` | the figures and the tables: `fu_figures.py results/fu_results.json figures/` renders the post's nine figures; `--all` adds the second post's and the background's, which also read the side files in `results/` |
| `results/fu_results.json` | the extracted records behind the published figures; `fu_results_fc.json` the fixed-count cells; `fu_q65_p99.json`, `tradeoff_realistic.py`, `bandwidth_sweep.py` and `payload_sweep.py` are data from the first post that the `--all` figures read |
| `render.sh` | extract `logs/` into `results/fu_results.json` and render the post's figures |

## Reproduce a cell

From the root of this branch:

```sh
go test -c -o testing/segstudy/segfu.test ./beacon-chain/p2p/segmentintegrationtest
cd testing/segstudy
./runcell.sh atuned 7 clean home p1m          # A tuned, seed 7, clean network, home links, 1 MiB
./runcell.sh wholend 7 clean home p1m         # whole message on stock gossipsub, the same seed
python3 fu_extract.py logs results/mine.json
```

A 500-node cell runs the mesh under Go's `synctest` virtual clock, so it takes a few minutes of
wall time and 6 to 10 GB of memory whatever the payload, and its latencies are virtual-clock
readings, not wall-clock ones. Run cells one at a time or under a memory cap; several at once on
one host share nothing but memory. A whole figure is its cell list fed to `runcell.sh`, for
example with GNU parallel:

```sh
parallel -j 4 --colsep ' ' -a cells/figure2_ladder.txt ./runcell.sh {1} {2} {3} {4} {5} {6}
./render.sh
```

## How the published cells were run

- **Code.** The first commit of this branch is the research tree at the commit the fleet binary
  was built from, minus the notes; the second pins the two forks as published modules in place
  of the vendored copies, byte-identical to them. The binary was `go test -c` of
  `beacon-chain/p2p/segmentintegrationtest` (md5 `e51a66896b3fea9ff7d28d7c74db22ce`), built with
  go1.26.5 on linux/amd64. Bazel builds the same packages and the beacon
  node from the same pins; nogo's `maligned` analyzer flags two structs in the harness's test
  file, left as they ran, so build with `--norun_validations`.
- **Hosts.** Two: a 40-thread Xeon E5-2630 v4 box with 251 GB running 6 to 20 cells at a time
  under GNU parallel, and a Ryzen 9 8945HS laptop running one to three. Every cell is virtual
  time, so counts and virtual-clock latencies do not depend on the host; the agreement gate
  between the two (one cell, same seed, same environment) was counts within 1 to 2 percent and
  latency percentiles within the same-seed noise floor of about 5 percent, which is also how much
  scheduling alone moves a fixed-seed uncoded cell between two runs on one host. The coded arms
  with stop-pull swing more: two runs of one compress-first cell differed by 10 percent in the
  median and 8 percent in bytes, because the stop rule races the clock. Compare medians across
  seeds, never one cell against one cell.
- **Per cell.** `GOMAXPROCS=2`, the Go garbage collector at its defaults, `-test.count=1` so the
  test cache never replays a result, `SEGMENT_DET_RAND=1` with `SEGMENT_SEED` from the cell list
  so two arms at one seed see the same topology and the same random draws. The `.time` file of
  each cell records its wall time and peak memory.
- **Seeds.** The cell lists name them; the post's rule is ten seeds per published point, and a
  figure plots the median across seeds of each seed's statistic with bars of one sample standard
  deviation.
- **The extractor's checks.** A log without its `.done` marker is skipped (the cell did not
  finish). A log without a result line is recorded with `harness: unparsed` and excluded by the
  figures' record selection. A percentile the harness could not compute, because a receiver never
  completed inside the horizon, is recorded as null and drawn hollow. A cell that reached the
  test timeout is flagged `timeout`. Fault cells carry the harness's own exposure lines, so a
  withholding cell that withheld nothing is visible as such.
- **Raw logs.** The per-cell logs, time records and markers of every published cell are attached
  to the release tag as a gzipped tarball; `fu_extract.py` over that directory regenerates
  `results/fu_results.json`.

## Reading the results file

One record per cell with, among others: `arm`, `seed`, `fault`, `net`, `pay`, `nsize`;
`p50_s`, `p90_s`, `p99_s` (per-node completion, seconds, virtual clock); `completed`/`total`,
`rate_k`/`rate_n` (nodes complete by the deadline); `rx_node_bytes` and `ctrl_rx_node_bytes`
(bytes received per node, data and control); `publisher_bytes`; `dups`, `idw_tx`; `ok`,
`timeout`, `exposure`. `fu_figures.py`'s `pick()` is the selection the figures use, and its
`ARMS` table names every arm.
