#!/usr/bin/env python3
"""Fold one multi-message cell into a record, from either substrate.

A multi-message cell (SEGMENT_MESSAGES=M) diffuses M payloads a gap apart on one network; the
driver reports each message apart and closes with message 1 (cold) and the median and range of
messages 2..M (warm), each quantile taken per message first (multimessage_test.go). This does the
same for both substrates, so the two records pair field for field:

  --shadow-data DIR   a Shadow cell's data directory: every node prints one SHADOWMSG line per
                      message (shadow_messages_test.go). Per message: completion quantiles over the
                      non-publisher population under right-censoring, and the counters cut at the
                      last completion (each node's first snapshot at or after it, or its end-of-window
                      totals when the message timed out or completed after the snapshots), as the
                      lab's extract.py cuts a single-message cell.
  --harness-log FILE  the in-process driver's log of the same cell: one result line per message,
                      each followed by its link rx/node line.

Per message the record keeps p50/p90/p99/last, rx/node (the tracer's payload and control bytes over
n, the publisher included), ctrl rx/node, link rx/node (what each node's downlink delivered: UDP
payloads with every QUIC header; Shadow counts it at the node's QUIC connections, the harness at its
simulated links), dups and the publisher's bytes. The cold and warm summaries follow the driver's
closing line. Reuses extract.py (the lab's shadowsim tools, --tools) for the shared pieces.

usage: msgextract.py (--shadow-data DIR | --harness-log FILE) --tools DIR --n N --arm ARM --seed S
                     --transport T --pay PAY [--payload-bytes B] [--deadline 3] [--out record.json]
                     [--label CELL] [--shadow-rc RC] [--stderr shadow.stderr] [--binary PATH]
                     [--shadow PATH] [--commit SHA] [--extra KEY=VALUE ...]
"""
import argparse
import glob
import json
import os
import re
import sys

INF = float("inf")


def at_cut(st, cut):
    """A node's counters for one message at the cut: the first snapshot at or after it, else the
    end of the message's window."""
    for snap in st.get("snapshots") or []:
        if snap["t"] >= cut:
            return snap
    return st["end"]


# The v0.5 rules' counters (SEGMENT_V05), in the order the driver's "v0.5 this message" line
# prints them; a Shadow node reports its own under these keys.
V05_KEYS = ("promises_at_write", "asks_lost", "unpushed", "offers_owed", "owed_sent", "answers_owed",
            "last_resorts_armed", "last_resort_asks", "parked_asked", "beyond_list", "closed_promises",
            "ledger_asks", "expired", "expired_discards")
V05LINE = re.compile(r"v0\.5 this message: (.*)")


def v05_sum(stats):
    """The v0.5 counters summed over the nodes, None when no node ran the rules."""
    reps = [s["v05"] for s in stats if s.get("v05")]
    return {k: sum(r.get(k, 0) for r in reps) for k in V05_KEYS} if reps else None


def shadow_messages(data_dir, n, deadline, ex):
    """Per-message summaries from a Shadow cell's SHADOWMSG lines, and the cell's health."""
    by_msg, fails, dup_reports, msgs_declared = {}, [], 0, set()
    for path in sorted(glob.glob(os.path.join(data_dir, "hosts", "node*", "*.stdout"))):
        text = open(path, errors="replace").read()
        for line in text.splitlines():
            if line.startswith("SHADOWMSG "):
                js = json.loads(line[len("SHADOWMSG "):])
                per = by_msg.setdefault(js["msg"], {})
                if js["index"] in per:
                    dup_reports += 1
                per[js["index"]] = js
                msgs_declared.add(js["msgs"])
        if "--- FAIL" in text or "panic" in text:
            fails.append(os.path.basename(os.path.dirname(path)))
    m = max(msgs_declared) if msgs_declared else 0
    out, missing = [], 0
    for k in range(1, m + 1):
        stats = {i: s for i, s in by_msg.get(k, {}).items() if 0 <= i < n}
        missing += n - len(stats)
        pop = n - 1
        times = sorted(s["comp_s"] for i, s in stats.items() if i != 0 and s["completed"])
        timeout = len(times) < pop
        cut = max(times) if times and not timeout else INF
        cutv = {i: at_cut(s, cut) for i, s in stats.items()}
        pub, pubc = stats.get(0, {}), cutv.get(0, {})
        mesh = sorted(s["mesh_at_publish"] for s in stats.values())
        allv = list(stats.values())
        out.append({
            "msg": k, "completed": len(times), "total": pop, "timeout": timeout,
            "last_s": max(times) if times else None,
            "p50_s": ex.quantile(times, pop, 0.50), "p90_s": ex.quantile(times, pop, 0.90),
            "p99_s": ex.quantile(times, pop, 0.99),
            "rate_k": sum(1 for t in times if t <= deadline),
            "cut_s": None if cut == INF else cut,
            "dups": sum(c["dups"] for c in cutv.values()),
            "idw_tx": sum(c["idw_sent"] for c in cutv.values()),
            "drops": sum(c["drops"] for c in cutv.values()),
            "publisher_bytes": pubc.get("tx_bytes"),
            "publisher_link_tx": pubc.get("link_tx"),
            "rx_node_bytes": sum(c["rx_bytes"] for c in cutv.values()) // n if cutv else None,
            "ctrl_rx_node_bytes": sum(c["ctrl_rx"] for c in cutv.values()) // n if cutv else None,
            "link_rx_node_bytes": sum(c["link_rx"] for c in cutv.values()) // n if cutv else None,
            # What a node's QUIC connections received is what its socket handed up: the quantity of
            # the driver's socket rx/node (its link rx counts packets as the downlink queue takes them).
            "socket_rx_node_bytes": sum(c["link_rx"] for c in cutv.values()) // n if cutv else None,
            "rx_node_bytes_end": sum(s["end"]["rx_bytes"] for s in allv) // n if allv else None,
            "link_rx_node_bytes_end": sum(s["end"]["link_rx"] for s in allv) // n if allv else None,
            "mesh_min": mesh[0] if mesh else None, "mesh_median": mesh[len(mesh) // 2] if mesh else None,
            "mesh_max": mesh[-1] if mesh else None,
            "arm_name": pub.get("arm_name"), "arm_msgs": pub.get("arm_msgs"), "arm_need": pub.get("arm_need"),
            "unit_bytes": pub.get("unit_bytes"), "id_bytes": pub.get("id_bytes"),
            "declined_cum": sum(s.get("declined", 0) for s in allv),
            "start_s_min": min((s["start_s"] for s in allv), default=None),
            "publish_s": pub.get("publish_s"),
            "v05": v05_sum(allv),
        })
    health = {"missing": missing, "failed_nodes": len(fails), "dup_reports": dup_reports, "msgs": m}
    return out, health


# The driver's per-message result line (q6_mesh_test.go's report) and the link rx line after it.
RESULT = re.compile(r"n=\s*(\d+)\s+\S+\s+(\d+)/(\d+)\s+virtual\s+(\d+|cens)ms.*?p50 (\d+|cens)ms p90 (\d+|cens)ms "
                    r"p99 (\d+|cens)ms rate@\S+ (\d+)/(\d+)\s+dups\s+(\d+)\s+IDONTWANT tx\s+(\d+).*?publisherTx\s+(\d+)B"
                    r"\s+rx/node\s+(\d+)B\s+ctrl rx/node\s+(\d+)B.*?mesh min (\d+), median (\d+), max (\d+)")
LINKRX = re.compile(r"link rx/node (\d+)B")
SOCKRX = re.compile(r"socket rx/node (\d+)B")


def harness_messages(log):
    """Per-message summaries from the in-process driver's log, in message order."""
    ms = lambda v: None if v == "cens" else int(v) / 1000  # noqa: E731
    out, last = [], None
    for line in open(log, errors="replace"):
        r = RESULT.search(line)
        if r:
            g = r.groups()
            last = {"msg": len(out) + 1, "completed": int(g[1]), "total": int(g[2]), "timeout": int(g[1]) < int(g[2]),
                    "last_s": ms(g[3]), "p50_s": ms(g[4]), "p90_s": ms(g[5]), "p99_s": ms(g[6]), "rate_k": int(g[7]),
                    "dups": int(g[9]), "idw_tx": int(g[10]), "publisher_bytes": int(g[11]),
                    "rx_node_bytes": int(g[12]), "ctrl_rx_node_bytes": int(g[13]), "link_rx_node_bytes": None,
                    "socket_rx_node_bytes": None,
                    "mesh_min": int(g[14]), "mesh_median": int(g[15]), "mesh_max": int(g[16]), "v05": None}
            out.append(last)
            continue
        sr = SOCKRX.search(line)
        if sr and last is not None and last["socket_rx_node_bytes"] is None:
            last["socket_rx_node_bytes"] = int(sr.group(1))
            continue
        v = V05LINE.search(line)
        if v and last is not None and last["v05"] is None:
            nums = [int(x) for x in re.findall(r"-?\d+", v.group(1).replace("v0.5", ""))]
            if len(nums) == len(V05_KEYS):
                last["v05"] = dict(zip(V05_KEYS, nums))
            continue
        lr = LINKRX.search(line)
        if lr and last is not None and last["link_rx_node_bytes"] is None:
            last["link_rx_node_bytes"] = int(lr.group(1))
    text = open(log, errors="replace").read()
    health = {"missing": 0, "failed_nodes": 0 if "--- PASS" in text else 1, "dup_reports": 0, "msgs": len(out)}
    return out, health


def summary(per, keys=("p50_s", "p90_s", "p99_s", "rx_node_bytes", "link_rx_node_bytes", "socket_rx_node_bytes",
                       "ctrl_rx_node_bytes")):
    """The driver's closing line: message 1 apart, then median [min, max] over messages 2..M, each
    value per message first, a censored quantile ranking last."""
    cold = {k: per[0].get(k) for k in keys} if per else {}
    warm = {}
    rest = per[1:]
    for k in keys:
        xs = [p.get(k) for p in rest]
        if not xs or any(x is None for x in xs) and k not in ("p50_s", "p90_s", "p99_s"):
            warm[k] = warm[k + "_min"] = warm[k + "_max"] = None
            continue
        xs = sorted(xs, key=lambda x: INF if x is None else x)
        warm[k], warm[k + "_min"], warm[k + "_max"] = xs[len(xs) // 2], xs[0], xs[-1]
    return cold, warm


def main():
    ap = argparse.ArgumentParser()
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--shadow-data")
    src.add_argument("--harness-log")
    ap.add_argument("--tools", required=True, help="the lab's shadowsim/tools, for extract.py")
    ap.add_argument("--n", type=int, required=True)
    ap.add_argument("--arm", required=True)
    ap.add_argument("--seed", type=int, required=True)
    ap.add_argument("--transport", required=True)
    ap.add_argument("--pay", required=True)
    ap.add_argument("--payload-bytes", type=int, default=None)
    ap.add_argument("--deadline", type=float, default=3.0)
    ap.add_argument("--out")
    ap.add_argument("--label", default="")
    ap.add_argument("--shadow-rc", type=int, default=0)
    ap.add_argument("--stderr")
    ap.add_argument("--binary")
    ap.add_argument("--shadow")
    ap.add_argument("--commit", default=None)
    ap.add_argument("--extra", action="append", default=[], metavar="KEY=VALUE")
    a = ap.parse_args()
    sys.path.insert(0, a.tools)
    import extract as ex  # noqa: E402 -- the lab's single-message extractor

    if a.shadow_data:
        per, health = shadow_messages(a.shadow_data, a.n, a.deadline, ex)
        harness = "shadow"
    else:
        per, health = harness_messages(a.harness_log)
        harness = "q6"
    cold, warm = summary(per)
    wall, rss = ex.time_v(a.stderr)
    rec = {"harness": harness, "transport": a.transport, "arm": a.arm, "seed": a.seed, "pay": a.pay,
           "payload_bytes": a.payload_bytes, "n": a.n, "nsize": a.n, "label": a.label, "commit": a.commit,
           "deadline_s": a.deadline, "msgs": health["msgs"], "per_msg": per, "cold": cold, "warm": warm,
           "complete_all": bool(per) and all(not p["timeout"] for p in per),
           "missing": health["missing"], "failed_nodes": health["failed_nodes"],
           "dup_reports": health["dup_reports"], "shadow_rc": a.shadow_rc, "wall_s": wall, "max_rss_kb": rss}
    if a.shadow_data:
        rec["binary_sha256"] = ex.sha256(a.binary) if a.binary and os.path.exists(a.binary) else None
        rec["shadow_version"] = ex.shadow_version(a.shadow) if a.shadow else None
        rec.update(ex.shadow_config(a.shadow_data))
    rec["ok"] = (a.shadow_rc == 0 and health["missing"] == 0 and health["failed_nodes"] == 0
                 and health["dup_reports"] == 0 and health["msgs"] > 1)
    extra = dict(kv.split("=", 1) for kv in a.extra)
    clash = sorted(set(extra) & set(rec))
    if clash:
        sys.exit(f"--extra may not overwrite record fields: {clash}")
    rec.update(extra)
    if a.out:
        os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
        with open(a.out, "w") as f:
            json.dump(rec, f, indent=1, sort_keys=True)
    f = lambda x: "cens" if x is None else f"{x * 1000:.0f}"  # noqa: E731
    mib = lambda x: "?" if x is None else f"{x / 1048576:.3f}"  # noqa: E731
    for p in per:
        print(f"  msg {p['msg']}: {p['completed']}/{p['total']} p50 {f(p['p50_s'])} p90 {f(p['p90_s'])} p99 {f(p['p99_s'])} ms"
              f"  rx/node {p['rx_node_bytes']}B  link rx/node {p['link_rx_node_bytes']}B  dups {p['dups']}"
              f"  mesh {p['mesh_min']}/{p['mesh_median']}/{p['mesh_max']}")
    print(f"{harness}/{a.transport} n={a.n} {a.arm} seed {a.seed}: M={health['msgs']}; cold p99 {f(cold.get('p99_s'))} ms; "
          f"warm p99 {f(warm.get('p99_s'))} [{f(warm.get('p99_s_min'))}..{f(warm.get('p99_s_max'))}] ms, "
          f"rx/node {mib(warm.get('rx_node_bytes'))} MiB, link rx/node {mib(warm.get('link_rx_node_bytes'))} MiB; "
          f"ok {rec['ok']} (missing {health['missing']}, failed {health['failed_nodes']}, rc {a.shadow_rc})")
    return 0 if rec["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
