#!/usr/bin/env python3
"""Extract one record per follow-up cell log into JSON (and a flat CSV).

Log names: <arm>_<seed>_<fault>_<net>_<pay>[_n<N>].log in the logs directory.
    python3 fu_extract.py logs/ results/fu_results.json
Handles the three result-line formats: q6 (A arms, whole), variantC, variantB (two lines).
Durations are converted to seconds; bytes stay bytes. rate3 is the fraction complete by the
SEGMENT_DEADLINE (3 s in these cells; the deadline value is recorded).
"""
import csv, glob, json, os, re, sys

Q6 = re.compile(r"(\d+)Mbps L=(\S+)\s+n=\s*(\d+)\s+(\S+)\s+(\d+)/(\d+)\s+virtual\s+(\S+) \(D-copy floor\s+\S+\)\s+"
                r"p50 (\S+) p90 (\S+) p99 (\S+) rate@(\d+)s (\d+)/(\d+)\s+dups\s+(\d+)\s+IDONTWANT tx\s+(\d+)\s+dup-per-idw\s+\S+\s+"
                r"publisherTx\s+(\d+)B\s+rx/node\s+(\d+)B\s+ctrl rx/node\s+(\d+)B\s+wall (\S+)")
VC = re.compile(r"variantC n=\s*(\d+)\s+T=(\d+)(?: K=(\d+) S=K\+(\d+))?\s+(\d+)/(\d+)\s+virtual\s+(\S+)\s+"
                r"p50 (\S+) p90 (\S+) p99 (\S+) rate@(\d+)s (\d+)/(\d+)\s+dups\s+(\d+)\s+IDONTWANT tx\s+(\d+)\s+dup-per-idw\s+\S+\s+"
                r"publisherTx\s+(\d+)B\s+rx/node\s+(\d+)B\s+ctrl rx/node\s+(\d+)B\s+wall (\S+)")
VB1 = re.compile(r"arm=(\S+)\s+n=(\d+) K=(\d+)/(\d+) completion (\S+)\s+p50 (\S+) p90 (\S+) p99 (\S+) rate@(\d+)s (\d+)/(\d+)\s+"
                 r"segments planned: pushed (\d+) served (\d+) requested (\d+); received (\d+) \(([\d.]+) copies/node\)")
VB2 = re.compile(r"reissued requests (\d+); partial traffic: tx (\d+) B over (\d+) RPCs, rx (\d+) B over (\d+) RPCs "
                 r"\(([\d.]+)x payload rx/node\); publisherTx (\d+) B; dropped RPCs (\d+); full-gossip rx/node (\d+) B")
PAY = {"p128k": 131072, "p256k": 262144, "p384k": 393216, "p512k": 524288, "p640k": 655360, "p768k": 786432, "p896k": 917504, "p1m": 1048576, "p1536k": 1572864, "p2m": 2097152}


TAIL = re.compile(r"tail-hedge: (\d+) extra asks issued, (\d+) nodes entered the tail")
# The observation horizon (final9, 2026-09-13): every node read at the deadline after publish, per population.
HB = re.compile(r"bytes@(\d+)ms \[(\w+) (\d+)\]: completed (\d+)/(\d+) rx/node (\d+)B ctrl rx/node (\d+)B dups (\d+) post-completion rx/node (\d+)B")
HR = re.compile(r"requests@(\d+)ms \[(\w+) (\d+)\]: ids asked (\d+) answered (\d+) overtaken (\d+) open (\d+) \(age p50 (\d+)ms\)(?: abandoned (\d+) dropped-asks (\d+))? pushes (\d+); "
                r"asks/id ([\d.]+) answered-dups (\d+); service p50 (\d+)ms p90 (\d+)ms p99 (\d+)ms; top answerer p50 ([\d.]+) answerers p50 (\d+)")
HO = re.compile(r"outbound@(\d+)ms \[(\w+) (\d+)\]: peak queued/peer p50 (\d+)KB p90 (\d+)KB; writer blocked/node p50 (\d+)ms p90 (\d+)ms, busiest peer p50 (\d+)ms")
HP = re.compile(r"outbound@(\d+)ms \[publisher\]: peak queued/peer (\d+)KB; writer blocked (\d+)ms, busiest peer (\d+)ms")

def horizon(text, rec):
    """The horizon lines into rec["pops"][population] and, for the honest population, flat h_* fields."""
    pops = {}
    for m in HB.finditer(text):
        h, pop, n, done, tot, rx, ctrl, dups, post = m.groups()
        pops.setdefault(pop, {}).update(nodes=int(n), horizon_ms=int(h), completed=int(done), rx_node_bytes=int(rx),
                                        ctrl_rx_node_bytes=int(ctrl), dups=int(dups), post_rx_node_bytes=int(post))
    for m in HR.finditer(text):
        (h, pop, n, asked, answered, overtaken, opn, age, aband, dropped, pushes, asks_per_id, adups, s50, s90, s99, top, ans) = m.groups()
        pops.setdefault(pop, {}).update(asked=int(asked), answered=int(answered), overtaken=int(overtaken), open=int(opn),
                                        open_age_p50_ms=int(age), abandoned=int(aband) if aband else None, dropped_asks=int(dropped) if dropped else None,
                                        pushes=int(pushes), asks_per_id=float(asks_per_id),
                                        answered_dups=int(adups), service_p50_ms=int(s50), service_p90_ms=int(s90),
                                        service_p99_ms=int(s99), top_answerer_p50=float(top), answerers_p50=int(ans))
    for m in HO.finditer(text):
        h, pop, n, pk50, pk90, wb50, wb90, bp50 = m.groups()
        pops.setdefault(pop, {}).update(peak_queued_p50_kb=int(pk50), peak_queued_p90_kb=int(pk90),
                                        writer_blocked_p50_ms=int(wb50), writer_blocked_p90_ms=int(wb90), busiest_peer_p50_ms=int(bp50))
    if pops:
        rec["pops"] = pops
        for k, v in pops.get("honest", {}).items():
            rec["h_" + k] = v
    m = HP.search(text)
    if m:
        rec.update(pub_peak_queued_kb=int(m.group(2)), pub_writer_blocked_ms=int(m.group(3)), pub_busiest_peer_ms=int(m.group(4)))

def dur(s):
    """Go duration -> seconds ('773ms', '1.368s', '2m3.2s', '1h2m3s')."""
    parts = re.findall(r"([\d.]+)(ms|µs|us|ns|h|m|s)", s)
    if not parts:
        return None  # 'cens': the harness could not compute the percentile (a receiver never completed)
    total = 0.0
    for v, u in parts:
        v = float(v)
        total += v * {"h": 3600, "m": 60, "s": 1, "ms": 1e-3, "µs": 1e-6, "us": 1e-6, "ns": 1e-9}[u]
    return total



def nz(v):
    """A byte counter printed as 0 on a FAIL line is not a measurement; keep it out of every aggregate."""
    v = int(v)
    return v if v > 0 else None

def parse(path):
    base = os.path.basename(path)[:-4]
    parts = base.split("_")
    nsize = 500
    # <arm>_<seed>_<fault>_<net>_<pay>[_n<N>]; the arm may itself contain underscores (am_r0), so
    # take the fixed fields from the right and the arm is whatever precedes them.
    if len(parts) >= 6 and re.fullmatch(r"n\d+", parts[-1]):
        nsize = int(parts[-1][1:]); parts = parts[:-1]
    if len(parts) < 5 or not parts[-4].isdigit():
        return None
    arm, seed, fault, net, pay = "_".join(parts[:-4]), parts[-4], parts[-3], parts[-2], parts[-1]
    rec = {"arm": arm, "seed": int(seed), "fault": fault, "net": net, "pay": pay, "nsize": nsize,
           "payload_bytes": PAY[pay.replace("-fc", "")], "fixed_count": pay.endswith("-fc"),
           "log": base, "ok": None, "exposure": [], "timeout": False}
    text = open(path, errors="replace").read()
    rec["ok"] = ("\nPASS" in text or "--- PASS" in text) and "FAIL" not in text and "panic" not in text
    rec["timeout"] = "TIMEOUT" in text
    # A C cell whose meshes never reached the settle band published anyway (harness change
    # 2026-09-08); the record is a measurement on the mesh that formed, and the tables say so.
    rec["mesh_unsettled"] = "mesh unsettled after" in text
    m = TAIL.search(text)
    if m:
        rec["tail_extra"] = int(m.group(1)); rec["tail_entered"] = int(m.group(2))
    rec["exposure"] = [l.split("failure exposure: ", 1)[1].strip() for l in text.splitlines() if "failure exposure: " in l]
    horizon(text, rec)
    m = Q6.search(text)
    if m:
        (_, _, n, name, done, tot, last, p50, p90, p99, dl, r_k, r_n, dups, idw, pub, rx, ctrl, wall) = m.groups()
        rec.update(harness="q6", n=int(n), completed=int(done), total=int(tot), last_s=dur(last),
                   p50_s=dur(p50), p90_s=dur(p90), p99_s=dur(p99), deadline_s=int(dl), rate_k=int(r_k), rate_n=int(r_n),
                   dups=int(dups), idw_tx=int(idw), publisher_bytes=nz(pub), rx_node_bytes=nz(rx),
                   ctrl_rx_node_bytes=nz(ctrl), wall_s=dur(wall))
        return rec
    m = VC.search(text)
    if m:
        (n, T, K, R, done, tot, last, p50, p90, p99, dl, r_k, r_n, dups, idw, pub, rx, ctrl, wall) = m.groups()
        rec.update(harness="variantC", n=int(n), topics=int(T), K=int(K) if K else None, R=int(R) if R else None,
                   completed=int(done), total=int(tot), last_s=dur(last), p50_s=dur(p50), p90_s=dur(p90), p99_s=dur(p99),
                   deadline_s=int(dl), rate_k=int(r_k), rate_n=int(r_n), dups=int(dups), idw_tx=int(idw),
                   publisher_bytes=nz(pub), rx_node_bytes=nz(rx), ctrl_rx_node_bytes=nz(ctrl), wall_s=dur(wall))
        mm = re.search(r"mean (\d+) slots/node after diffusion", text)
        if mm:
            rec["mean_mesh_slots"] = int(mm.group(1))
        return rec
    m = VB1.search(text)
    if m:
        (name, n, K, N, last, p50, p90, p99, dl, r_k, r_n, pushed, served, requested, received, copies) = m.groups()
        n = int(n)
        rec.update(harness="variantB", n=n, K=int(K), N=int(N), last_s=dur(last), p50_s=dur(p50), p90_s=dur(p90),
                   p99_s=dur(p99), deadline_s=int(dl), rate_k=int(r_k), rate_n=int(r_n),
                   b_pushed=int(pushed), b_served=int(served), b_requested=int(requested), b_received=int(received),
                   b_copies=float(copies), completed=(n - 1) if not rec["timeout"] else None, total=n - 1)
        m2 = VB2.search(text)
        if m2:
            (reissued, ptx, ptxr, prx, prxr, prx_x, pub, dropped, fg_rx) = m2.groups()
            rec.update(b_reissued=int(reissued), partial_tx_bytes=int(ptx), partial_rx_bytes=int(prx),
                       partial_rx_rpcs=int(prxr), publisher_bytes=int(pub), dropped_rpcs=int(dropped),
                       full_gossip_rx_node_bytes=int(fg_rx),
                       # B's data travels as partial-message bytes: per-node receive = partial rx / n, plus any
                       # full-gossip bytes. Uncompressed wire (measurement plan §13).
                       rx_node_bytes=int(prx) // n + int(fg_rx))
        return rec
    rec["harness"] = "unparsed"
    return rec


def main():
    logdir = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
    out = sys.argv[2] if len(sys.argv) > 2 else os.path.join(logdir, "..", "results.json")
    recs = []
    for path in sorted(glob.glob(os.path.join(logdir, "*.log"))):
        if os.path.basename(path).startswith("chain"):
            continue
        if not os.path.exists(path[:-4] + ".done"):
            continue  # still running
        r = parse(path)
        if r:
            tf = path[:-4] + ".time"
            if os.path.exists(tf):
                t = open(tf).read()
                mm = re.search(r"Maximum resident set size \(kbytes\): (\d+)", t)
                if mm:
                    r["max_rss_kb"] = int(mm.group(1))
            recs.append(r)
    with open(out, "w") as f:
        json.dump(recs, f, indent=1, sort_keys=True)
    cols = ["arm", "seed", "fault", "net", "pay", "harness", "ok", "completed", "total", "rate_k", "rate_n",
            "p50_s", "p90_s", "p99_s", "last_s", "rx_node_bytes", "publisher_bytes", "ctrl_rx_node_bytes", "dups", "wall_s",
            "h_completed", "h_rx_node_bytes", "h_post_rx_node_bytes", "h_asked", "h_answered", "h_overtaken", "h_open",
            "h_open_age_p50_ms", "h_asks_per_id", "h_answered_dups", "h_service_p50_ms", "h_service_p90_ms", "h_service_p99_ms",
            "h_top_answerer_p50", "h_peak_queued_p50_kb", "h_peak_queued_p90_kb", "h_writer_blocked_p50_ms", "h_writer_blocked_p90_ms",
            "pub_peak_queued_kb", "pub_writer_blocked_ms"]
    with open(out.replace(".json", ".csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for r in recs:
            w.writerow(r)
    parsed = sum(1 for r in recs if r.get("harness") != "unparsed")
    print(f"{len(recs)} logs, {parsed} parsed, {sum(1 for r in recs if r['ok'])} PASS -> {out}")
    for r in recs:
        if r.get("harness") == "unparsed" or not r["ok"]:
            print("  check:", r["log"], r.get("harness"), "ok=", r["ok"], "timeout=", r["timeout"])


if __name__ == "__main__":
    main()
