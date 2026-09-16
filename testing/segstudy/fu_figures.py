#!/usr/bin/env python3
"""The follow-up post's figures and tables from the extracted cell records.

    python3 fu_figures.py [results/fu_results.json] [figures/] [--all]

Needs matplotlib (adjustText optional). Every figure function selects records through pick();
the Part 1 figures are figure_d (1), figure_ladder (2), figure_discipline (3),
figure_memory_ladder (4), figure_branches (5), figure_memory_branches (6), figure_units_a (7),
figure_nodes (8) and figure_closing (9); the rest belong to the second post and the background.
Bars are ±1 sample s.d. across seeds (none with one seed); the seed set is named in the post.
"""
import ast, glob, json, os, re, statistics as st, sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from fu_extract import VC, dur  # noqa: E402

ARGS = [a for a in sys.argv[1:] if not a.startswith("--")]
RES = ARGS[0] if len(ARGS) > 0 else os.path.join(HERE, "results", "fu_results.json")
OUT = ARGS[1] if len(ARGS) > 1 else os.path.join(HERE, "figures")
FIGDIR = os.path.dirname(RES)  # side files next to the results (fu_q65_p99.json)
MB = 1e6
PLAIN_COPY_MB = 0.746  # one compressed uncoded 1 MiB payload on the wire
# One copy on the wire per payload size: the harness payload (gossipsim.MainnetLikePayload(n, 11)) compressed
# whole with snappy.Encode, in bytes (2026-09-12). Every bytes axis is drawn in copies of this, so segmentation's
# own compression loss (under 1% at 32 KiB) and the code's incompressible parity both show.
WIRE = {"p128k": 93953, "p256k": 188170, "p384k": 282151, "p512k": 375363, "p640k": 467512, "p768k": 559552,
        "p896k": 651955, "p1m": 746262, "p1536k": 1119767, "p2m": 1493450}
COPIES_LABEL = "copies of the compressed payload received per node"

ARMS = {  # arm -> (label, color, marker)
    "atuned":    ("A tuned: phase r=2, move-on/ban, offer table", "#1f77b4", "P"),
    "acoded":    ("A coded 32+32 + stop-pull", "#d62728", "^"),
    "acodedcf":  ("A coded 32+32 + stop-pull, payload compressed before coding (32 segments of the compressed bytes)", "#ff7f0e", "v"),
    "aplain400": ("A, plain 400 ms discipline (control)", "#7f7f7f", "P"),
    "whole":     ("whole message, one-outstanding IWANT (200 ms + 200 ms/MiB)", "#777777", "o"),
    "wholend":   ("whole message, stock gossipsub", "#111111", "o"),
    "wholeot":   ("whole message, A tuned's request policy (offer table, park; scaled window)", "#333333", "o"),
    "bare":      ("segments on stock gossipsub: full-mesh push, sequential publish, no discipline", "#8c564b", "v"),
    "lad_batch": ("+ batch publishing (full-mesh push, no discipline)", "#e377c2", "v"),
    "lad_phase": ("+ phase forwarding r=2 (no discipline)", "#bcbd22", "v"),
    "bfixed":    ("B tuned: phase r=2, claim cap 4, park k=1, adaptive; compressed parts", "#9467bd", "D"),
    "bcodedf":   ("B tuned + RS 32+32", "#c5b0d5", "D"),
    "bcodedfc1": ("B coded, one part per frame: B tuned + RS 32+32, PushChunk 1", "#7b4fa0", "D"),
    "btuned":    ("B before the 2026-09-08 fixes (raw wire, over-counted strikes)", "#b39ddb", "d"),
    "aphaseot":  ("A: phase r=2, 400 ms one-outstanding discipline, offer table", "#4c72b0", "P"),
    "ahedge":    ("A: phase r=2, hedged second ask at 200 ms, offer table", "#6baed6", "P"),
    "am_k64":    ("A tuned, 16 KiB segments", "#0b3d91", "s"),
    "am_k8k":    ("A tuned, 8 KiB segments", "#001f5b", "D"),
    "aadapt":    ("A, size-adaptive rules (16 KiB; r = 4/3/2 by size; discipline from 512 KiB)", "#d62728", "*"),
    "aadapt2":   ("A, refined rule (16 KiB; r = 4 to 256 KiB, 3 at 384, 2 from 512; discipline + tail hedge k=3 h=4 from 512 KiB)", "#7b0000", "X"),
    "am_r1":     ("A tuned, push r=1", "#4c72b0", "P"),
    "am_r3":     ("A tuned, push r=3", "#4c72b0", "P"),
    "am_r4":     ("A tuned, push r=4", "#4c72b0", "P"),
    "bcoded":    ("B before the fixes + RS 32+32", "#d1c4e9", "d"),
    "c64":       ("C, 64 topics (16 KiB)", "#2ca02c", "s"),
    "c32":       ("C, 32 topics (32 KiB)", "#2ca02c", "s"),
    "atail_k3h4": ("A tuned + tail hedge (k = 3, h = 4)", "#7b0000", "X"),
    "lad_split": ("segments on stock gossipsub + batch + announce instead of push (r = 2, no decay)", "#17becf", "d"),
    "c32rs":     ("C + RS 32+32 (64 topics)", "#17becf", "s"),
    "ccust":     ("C + RS, partial subscription S = K + R of N (R = 8 at 1 MiB)", "#ff7f0e", "P"),
    "ccust0":    ("C + RS, partial subscription S = K (R = 0)", "#e6550d", "P"),
}
SIX = ["atuned", "acoded", "bfixed", "c32", "c32rs", "ccust"]  # bfixed replaced btuned as B's reference (Q74 addendum 2026-09-08)

recs = json.load(open(RES))
SEEDS = sorted({r["seed"] for r in recs if r["arm"] == "atuned" and r["fault"] == "clean" and r["net"] == "home" and r["pay"] == "p1m"}) or sorted({r["seed"] for r in recs})  # the anchors define the seed set
SEEDTAG = f"seed {SEEDS[0]}" if len(SEEDS) == 1 else f"seeds {'/'.join(map(str, SEEDS))}"
VTAG = f"{SEEDTAG}; one-seed points carry no bars"


def pick(arm, fault="clean", net="home", pay="p1m", nsize=500):
    return [r for r in recs if r["arm"] == arm and r["fault"] == fault and r["net"] == net and r["pay"] == pay
            and r.get("nsize", 500) == nsize and r.get("harness") not in (None, "unparsed")]


def sd(vals):
    """Sample standard deviation across seeds; 0 for a single seed (no bar)."""
    return st.stdev(vals) if len(vals) > 1 else 0.0


def spread(vals):
    """(median, median - s.d., median + s.d.) — the bar ends drawn around every point."""
    m = st.median(vals); d = sd(vals)
    return m, m - d, m + d


def agg(rs, key):
    vals = [r[key] for r in rs if r.get(key) is not None]
    if not vals:
        return None, None, None
    return spread(vals)


def victims(rs):
    """Honest receivers that never completed, the attackers excluded: under spoof-plus-withhold the
    attackers themselves never complete (the harness counts them as stranded), so subtract the count
    the exposure line reports. Withholders and silent relays keep receiving and do complete."""
    out = 0
    for r in rs:
        if r.get("completed") is None:
            continue
        out = max(out, r["total"] - r["completed"] - attackers(r))
    return max(out, 0)


def attackers(r):
    """Nodes that never complete by construction: IDONTWANT spoofers under spoof-plus-withhold."""
    n = 0
    for e in r.get("exposure", []):
        m = re.search(r"(\d+) IDONTWANT-spoofing nodes", e)
        if m:
            n += int(m.group(1))
    return n


def rate3(r):
    """Share of honest receivers complete by the deadline; attackers leave the denominator."""
    if not r.get("rate_n"):
        return None
    a = attackers(r)
    return (r["rate_k"] - 0) / max(r["rate_n"] - a, 1)


def censored(r):
    """The p99 is censored when more than 1% of the honest receivers never completed; a single
    stranded receiver of 499 leaves the 99th percentile observable, and the harness reports it."""
    if r.get("completed") is None or not r.get("total"):
        return False
    honest = r["total"] - attackers(r)
    return (honest - r["completed"]) > honest / 100


def style(ax, xlab, ylab, title):
    ax.set_xlabel(xlab)
    ax.set_ylabel(ylab)
    ax.set_title(title, fontsize=9)
    ax.grid(True, which="both", alpha=0.25)
    from matplotlib.ticker import FuncFormatter, NullFormatter, LogLocator
    for axis in (ax.xaxis, ax.yaxis):
        if axis.get_scale() == "log":
            axis.set_major_locator(LogLocator(base=10, subs=(1.0, 1.5, 2.0, 3.0, 5.0, 7.0)))
            axis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
            axis.set_minor_formatter(NullFormatter())


BAR_ALPHA = 0.22  # the ±1 s.d. bars are dimmed, as in tradeoff_realistic.py


def errpt(ax, x, y, xerr=None, yerr=None, color="k", marker="o", label=None, hollow=False, ms=6):
    """A point with dimmed ±1 s.d. bars (the bars carry no marker; the marker is drawn on top)."""
    if xerr is not None or yerr is not None:
        ax.errorbar(x, y, xerr=xerr, yerr=yerr, color=color, ls="none", elinewidth=0.7, capsize=1.5, alpha=BAR_ALPHA)
    kw = dict(color=color, marker=marker, ms=ms, ls="none", label=label, alpha=0.95)
    if hollow:
        kw.update(mfc="none", mew=1.2)
    ax.plot([x], [y], **kw)


def curve(ax, xs, meds, los, his, color, marker, label, ms=5, lw=1.2, ls="-", hollow=False):
    """A line through seed medians with dimmed ±1 s.d. bars when more than one seed is present."""
    if any(h > l for l, h in zip(los, his)):
        ax.errorbar(xs, meds, yerr=[[m - l for m, l in zip(meds, los)], [h - m for m, h in zip(meds, his)]],
                    color=color, ls="none", elinewidth=0.7, capsize=1.5, alpha=BAR_ALPHA)
    kw = dict(color=color, marker=marker, ms=ms, lw=lw, ls=ls, label=label)
    if hollow:
        kw.update(mfc="none", mew=1.2)
    ax.plot(xs, meds, **kw)


def series(arm, xs_keys, key, **pickkw):
    """Per x: (median, median - s.d., median + s.d.) of `key` over the cells pick(arm, **pick_at(x)) — parallel lists."""
    xs, med, lo, hi = [], [], [], []
    for x, kw in xs_keys:
        rs = pick(arm, **kw)
        if not rs:
            continue
        vals = [r[key] for r in rs if r.get(key) is not None]
        if not vals:
            continue
        m, l, h = spread(vals)
        xs.append(x); med.append(m); lo.append(l); hi.append(h)
    return xs, med, lo, hi


def untangle(fig, ax, texts, xs, ys):
    """Label layout as in tradeoff_realistic.py: adjustText untangles overlaps, thin grey leaders keep
    moved labels attached."""
    if not texts:
        return
    try:
        from adjustText import adjust_text
        fig.canvas.draw()
        adjust_text(texts, x=xs, y=ys, ax=ax, expand_axes=False, ensure_inside_axes=True, time_lim=20,
                    arrowprops=dict(arrowstyle="-", color="#999999", lw=0.5, alpha=0.6, shrinkA=2, shrinkB=4))
    except Exception as e:  # keep rendering without the library
        print("adjustText skipped:", e)


def load_dict_literal(path, name):
    """Read `NAME = {...}` from a figure script without executing it."""
    src = open(path).read()
    m = re.search(rf"^{name}\s*=\s*", src, re.M)
    start = src.index("{", m.end())
    depth, i = 0, start
    while True:
        c = src[i]
        depth += c == "{"
        depth -= c == "}"
        i += 1
        if depth == 0:
            break
    return ast.literal_eval(src[start:i])


def load_list_literal(path, name):
    src = open(path).read()
    m = re.search(rf"^{name}\s*=\s*(\[.*?\])", src, re.M | re.S)
    return ast.literal_eval(m.group(1))


# ---------------------------------------------------------------- Figure A: the A family (existing n=1000 data)
def figure_a():
    data = load_dict_literal(os.path.join(FIGDIR, "tradeoff_realistic.py"), "DATA")
    p99 = json.load(open(os.path.join(FIGDIR, "fu_q65_p99.json")))  # receiver p99 per seed, matched to DATA (2026-09-08)
    lab = {
        "aplain": "segmented, full push, no discipline", "apull_nd": "pull-only, no discipline", "apull": "pull-only, disc.",
        "aphase_nd": "phase r=2, no discipline", "aphase": "phase r=2, 400 ms disc.", "aphase_ot": "+ offer table",
        "moveban_ot": "move-on/ban + offer table (A tuned)", "aphase_r1": "r=1", "aphase_r3": "r=3", "aphase_r4": "r=4",
        "codedphase_nd": "coded, no discipline", "codedphase_nosp": "coded+phase, no stop-pull",
        "codedphase": "coded+phase+stop-pull", "codedphase_ot": "coded + offer table (A coded)", "codedpull": "coded pull-only",
        "fullpush_d4": "full push D=4", "fullpush_d6": "full push D=6", "meshless": "meshless", "aphasedelay": "source delay",
        "k64only": "16 KiB segments (K=64)", "aphase_sp": "phase + stop-pull (uncoded)",
    }
    fig, ax = plt.subplots(figsize=(8.5, 6))
    pts = {}
    texts, txs, tys = [], [], []
    for arm, l in lab.items():
        if arm not in data:
            continue
        d = data[arm]
        bs = [v * PLAIN_COPY_MB for v in d["y"]]  # payload equivalents -> MB
        ts = p99.get(arm, {}).get("p99") or d["x"]  # receiver p99 (s); last-receiver completion only if no p99 was recovered
        x, y = st.median(ts), st.median(bs)        # x = time, y = bytes, as in the first post's Figure 3
        pts[arm] = (x, y)
        color = "#d62728" if arm.startswith("coded") else ("#17becf" if arm.startswith("fullpush") else "#1f77b4")
        marker = "^" if arm.startswith("coded") else ("o" if arm in ("aplain", "fullpush_d4", "fullpush_d6") else "P")
        errpt(ax, x, y, xerr=[[sd(ts)], [sd(ts)]], yerr=[[sd(bs)], [sd(bs)]], color=color, marker=marker)
        texts.append(ax.text(x, y, l, fontsize=7)); txs.append(x); tys.append(y)
    def line(arms, **kw):
        arms = [a for a in arms if a in pts]
        if len(arms) > 1:
            ax.plot([pts[a][0] for a in arms], [pts[a][1] for a in arms], **kw)
    line(["aphase_r1", "aphase", "aphase_r3", "aphase_r4"], color="#1f77b4", lw=1, alpha=0.6)            # push depth
    line(["aphase_nd", "aphase", "aphase_ot", "moveban_ot"], color="#1f77b4", lw=1, ls="--", alpha=0.6)   # discipline
    line(["codedphase_nosp", "codedphase"], color="#d62728", lw=1, alpha=0.6)                           # stop-pull
    line(["aphase", "codedphase"], color="#888", lw=0.8, ls=":", alpha=0.7)                             # coding
    line(["moveban_ot", "codedphase_ot"], color="#888", lw=0.8, ls=":", alpha=0.7)
    line(["aphase", "k64only"], color="#888", lw=0.8, ls=":", alpha=0.7)                                # segment size (16 KiB point also has structured ids)
    line(["apull_nd", "apull"], color="#1f77b4", lw=1, ls="--", alpha=0.6)
    ax.set_xscale("log"); ax.set_yscale("log")
    ax.axhline(PLAIN_COPY_MB, color="#999", lw=0.8, ls=":")
    ax.text(ax.get_xlim()[1] * 0.98, PLAIN_COPY_MB * 1.02, "one compressed copy", fontsize=7, color="#666", ha="right")
    style(ax, "receiver p99 completion (s, log)", "encoded application data received per node (MB, log)",
          "Frontier map (background) — the A family at 1000 nodes, first-post cells at the plain 400 ms discipline except the offer-table points.\n"
          "solid: push depth r=1..4 · dashed: request policy · dotted grey: coding on/off, 32 vs 16 KiB · red: coded arms")
    untangle(fig, ax, texts, txs, tys)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "fu_a_family.png"), dpi=140)
    plt.close(fig)


# ---------------------------------------------------------------- Figure 3: the A family at 500 nodes on the tuned rules
def figure_amap():
    lab = {  # arm -> (label, color, marker); the request memory (offer table + 200 ms move-on + park) is the tuned rules
        "am_seg":       ("segmented, full-mesh push", "#1f77b4", "o"),
        "am_pull_nd":   ("pull-only, no discipline", "#1f77b4", "P"),
        "am_nd":        ("phase r=2, no discipline", "#1f77b4", "P"),
        "aplain400":    ("phase r=2, 400 ms discipline", "#1f77b4", "P"),
        "aphaseot":     ("+ offer table", "#1f77b4", "P"),
        "am_disc200":   ("200 ms move-on + offer table", "#1f77b4", "P"),
        "atuned":       ("+ park k=1 (A tuned)", "#1f77b4", "P"),
        "ahedge":       ("hedged ask + offer table", "#1f77b4", "P"),
        "am_r0":        ("pull-only, tuned rules", "#1f77b4", "P"),
        "am_r1":        ("r=1", "#1f77b4", "P"),
        "am_r3":        ("r=3", "#1f77b4", "P"),
        "am_r4":        ("r=4", "#1f77b4", "P"),
        "am_k64":       ("16 KiB segments (K=64)", "#1f77b4", "s"),
        "am_sp":        ("+ stop-pull (uncoded)", "#1f77b4", "P"),
        "acoded":       ("coded 32+32 + stop-pull (A coded)", "#d62728", "^"),
        "am_coded_nosp": ("coded, no stop-pull", "#d62728", "^"),
        "am_codedpull": ("coded pull-only", "#d62728", "^"),
        "am_coded_nd":  ("coded, no discipline", "#d62728", "^"),
    }
    fig, ax = plt.subplots(figsize=(8.5, 6))
    pts = {}
    texts, txs, tys = [], [], []
    nseeds = set()
    for arm, (l, c, m) in lab.items():
        rs = pick(arm)
        if not rs:
            continue
        nseeds |= {r["seed"] for r in rs}
        x, xlo, xhi = agg(rs, "p99_s"); y, ylo, yhi = agg(rs, "rx_node_bytes")
        W = WIRE["p1m"]
        pts[arm] = (x, y / W)
        errpt(ax, x, y / W, xerr=[[x - xlo], [xhi - x]], yerr=[[(y - ylo) / W], [(yhi - y) / W]], color=c, marker=m)
        texts.append(ax.text(x, y / W, l, fontsize=7)); txs.append(x); tys.append(y / W)
    def line(arms, **kw):
        arms = [a for a in arms if a in pts]
        if len(arms) > 1:
            ax.plot([pts[a][0] for a in arms], [pts[a][1] for a in arms], **kw)
    line(["am_r0", "am_r1", "atuned", "am_r3", "am_r4"], color="#1f77b4", lw=1, alpha=0.6)                 # push depth
    line(["am_nd", "aplain400", "aphaseot", "am_disc200", "atuned"], color="#1f77b4", lw=1, ls="--", alpha=0.6)  # request policy
    line(["am_pull_nd", "am_r0"], color="#1f77b4", lw=1, ls="--", alpha=0.6)
    line(["am_coded_nosp", "acoded"], color="#d62728", lw=1, alpha=0.6)                                    # stop-pull
    line(["atuned", "acoded"], color="#888", lw=0.8, ls=":", alpha=0.7)                                    # coding
    line(["am_nd", "am_coded_nd"], color="#888", lw=0.8, ls=":", alpha=0.7)
    line(["atuned", "am_k64"], color="#888", lw=0.8, ls=":", alpha=0.7)                                    # segment size
    line(["atuned", "am_sp"], color="#888", lw=0.8, ls=":", alpha=0.7)                                     # uncoded stop-pull
    ax.set_xscale("log"); ax.set_yscale("log")
    ax.axhline(1.0, color="#999", lw=0.8, ls=":")
    ax.text(ax.get_xlim()[1] * 0.98, 1.02, "one compressed copy", fontsize=7, color="#666", ha="right")
    style(ax, "receiver p99 completion (s, log)", f"{COPIES_LABEL} (log)",
          "Figure 10 — the A family at 500 nodes on the tuned rules (medians, bars = ±1 s.d. across seeds).\n"
          "solid: push depth r=0..4 · dashed: request policy · dotted grey: coding on/off, 32 vs 16 KiB, stop-pull · red: coded arms")
    untangle(fig, ax, texts, txs, tys)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "fu_a_map500.png"), dpi=140)
    plt.close(fig)
    print("  a-map arms present:", len(pts), "seeds:", sorted(nseeds))


# ---------------------------------------------------------------- Figure B: transports + subscription-margin dial
def figure_b():
    fig, ax1 = plt.subplots(1, 1, figsize=(7.6, 5))
    texts, txs, tys = [], [], []
    # n=500 anchors (all arms), plus the n=1000 partial-subscription block (ccust0 / ccust / c32rs) when it exists
    big = {a: pick(a, nsize=1000) for a in ("ccust0", "ccust", "c32rs")}
    have_big = any(big.values())
    for arm in SIX + ["wholend"]:  # the plain-400 ms control left the figure on 2026-09-12; it stays on the A map
        rs = pick(arm)
        if not rs:
            continue
        x, xlo, xhi = agg(rs, "rx_node_bytes"); y, ylo, yhi = agg(rs, "p99_s")
        l, c, m = ARMS[arm]
        W = WIRE["p1m"]
        errpt(ax1, y, x / W, xerr=[[y - ylo], [yhi - y]], yerr=[[(x - xlo) / W], [(xhi - x) / W]], color=c, marker=m, label=l)
        texts.append(ax1.text(y, x / W, l.split(":")[0].split("(")[0].strip(), fontsize=7)); txs.append(y); tys.append(x / W)
    for arm, rs in big.items():
        if not rs:
            continue
        x, xlo, xhi = agg(rs, "rx_node_bytes"); y, ylo, yhi = agg(rs, "p99_s")
        l, c, m = ARMS.get(arm, ("C + RS, partial subscription R = 0", "#ff7f0e", "P"))
        W = WIRE["p1m"]
        errpt(ax1, y, x / W, xerr=[[y - ylo], [yhi - y]], yerr=[[(x - xlo) / W], [(xhi - x) / W]], color=c, marker=m, hollow=True, ms=9)
        texts.append(ax1.text(y, x / W, f"1000 nodes: {l.split(':')[0].split('(')[0].strip()}", fontsize=7, color="#555"))
        txs.append(y); tys.append(x / W)
    ax1.set_xscale("log"); ax1.set_yscale("log")
    ax1.axhline(1.0, color="#999", lw=0.8, ls=":")
    sub = "hollow = the 1000-node partial-subscription block" if have_big else "PLACEHOLDER until the 1000-node block runs"
    style(ax1, "receiver p99 completion (s, log)",
          f"{COPIES_LABEL} (log)",
          f"Figure 1 — the reference configurations, 500-node headline base.\n{sub}")
    untangle(fig, ax1, texts, txs, tys)
    # The subscription-margin dial (merged-harness cells at three seeds, Q75 addendum 2) left the
    # figure on 2026-09-11: one sentence in the post carries it, on the ten-seed R = 0 / 8 / full cells.
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "fu_transports.png"), dpi=140)
    plt.close(fig)


# ---------------------------------------------------------------- Figure C: withholding dose-response
def figure_c():
    phis = [0, 10, 20, 30, 50]
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.8))
    for arm in SIX:
        l, c, m = ARMS[arm]
        keys = [(phi, dict(fault="clean" if phi == 0 else f"wh{phi}")) for phi in phis]
        xs, r3m, r3l, r3h = [], [], [], []
        for phi, kw in keys:
            rs = pick(arm, **kw)
            if rs:
                v = [rate3(r) * 100 for r in rs]
                m_, l_, h_ = spread(v)
                xs.append(phi); r3m.append(m_); r3l.append(l_); r3h.append(h_)
        if not xs:
            continue
        curve(ax1, xs, r3m, r3l, r3h, c, m, l)
        xs2, p99m, p99l, p99h = series(arm, keys, "p99_s")
        curve(ax2, xs2, p99m, p99l, p99h, c, m, l)
        for x, kw in keys:
            rs = pick(arm, **kw)
            if rs and any(censored(r) for r in rs):
                ax2.plot(x, st.median([r["p99_s"] for r in rs]), marker=m, ms=9, mfc="none", mec=c, ls="none")
    ax1.axhline(100, color="#999", lw=0.8, ls=":")
    style(ax1, "withholding share φ (%) — F1 withholders keep relaying pushes", "receivers complete by 3 s (%)",
          f"Figure 3(1) — fraction complete by 3 s, 500-node headline base")
    ax2.axhline(3.0, color="#999", lw=0.8, ls=":")
    style(ax2, "withholding share φ (%)", "receiver p99 completion (s; hollow = a receiver never completed)",
          f"Figure 3(2) — p99 under withholding")
    ax1.legend(fontsize=7, loc="lower left")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "fu_withholding.png"), dpi=140)
    plt.close(fig)


# ---------------------------------------------------------------- Figure D: all-home vs 20% datacenter
def figure_d():
    fig, ax = plt.subplots(figsize=(9.5, 4.8))
    # whole message on stock gossipsub is the leftmost group, not a reference line; coded B joins beside B tuned
    # in its one-part-per-frame form (a-vs-b-same-rules.md; bundled coded B is slower than B tuned in every scenario).
    # Custody (partial subscription) left the figure on 2026-09-16 (author): A, B, C and their best coded forms.
    arms = ["wholend", "atuned", "acoded", "bfixed", "bcodedfc1", "c32", "c32rs"]
    scen = [("home", "home builder, all home", None, 0.9, ":"), ("dc20", "home builder, 20% datacenter", "//", 0.6, "-."),
            ("dcb20", "datacenter builder, 20% datacenter", "xx", 0.4, "--")]  # the first post's Table 2 columns
    w = 0.26
    for i, arm in enumerate(arms):
        l, c, m = ARMS[arm]
        for j, (net, lab, hatch, alpha, _) in enumerate(scen):
            rs = pick(arm, net=net)
            if not rs:
                continue
            p99, lo, hi = agg(rs, "p99_s"); p50, _, _ = agg(rs, "p50_s")
            x = i + (j - 1) * w
            ax.bar(x, p99, width=w, color=c, alpha=alpha, hatch=hatch, edgecolor="white", label=lab if i == 0 else None)
            if hi > lo:
                ax.errorbar(x, p99, yerr=[[p99 - lo], [hi - p99]], color="k", ls="none", elinewidth=0.7, capsize=1.5, alpha=BAR_ALPHA)
            ax.plot(x, p50, marker="_", color="k", ms=14, mew=1.5)
    ax.axhline(3.0, color="#999", lw=0.8, ls="--")
    ax.set_xticks(range(len(arms))); ax.set_xticklabels(["whole message, as today" if a == "wholend" else ARMS[a][0].split(":")[0].split("(")[0].strip() for a in arms], fontsize=8, rotation=12)
    style(ax, "", "completion: receiver p99 (bar) and p50 (tick), s",
          f"Figure 1 — whole message and the three mappings with their coded forms, the first post's three scenarios, 500 nodes")
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "fu_datacenter.png"), dpi=140)
    plt.close(fig)


# ---------------------------------------------------------------- Figure E: bandwidth
def figure_e():
    ups = [10, 15, 20, 30, 50, 100, 200]  # 100/200: seed 7 first (2026-09-08), all seeds once the shape is judged worth it
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.8))
    for arm in SIX + ["aplain400"]:
        l, c, m = ARMS[arm]
        keys = [(up, dict(net="home" if up == 50 else f"up{up}")) for up in ups]
        xs, p50, lo, hi = series(arm, keys, "p50_s")
        if xs:
            curve(ax1, xs, p50, lo, hi, c, m, l)
        xs, p99, lo, hi = series(arm, keys, "p99_s")
        if xs:
            curve(ax2, xs, p99, lo, hi, c, m, l)
    # Q66b's three old arms (400 ms discipline, 3 seeds), hollow, for continuity
    try:
        old = load_dict_literal(os.path.join(FIGDIR, "bandwidth_sweep.py"), "DATA")
        for arm, c in [("A pull-only", "#7f4fc9"), ("A + phase r=2", "#1f77b4"), ("A coded 32+32 + phase + stop-pull", "#d62728")]:
            if arm in old:
                xs = sorted(old[arm]); ys = [st.median(old[arm][u]) for u in xs]
                ax1.plot(xs, ys, color=c, marker="o", mfc="none", ms=6, lw=0.8, ls=":", label=f"Q66b {arm} (old discipline)")
    except Exception as e:  # keep rendering if the old file moves
        print("Q66b overlay skipped:", e)
    for ax in (ax1, ax2):
        ax.set_xscale("log"); ax.set_xticks(ups); ax.set_xticklabels([str(u) for u in ups]); ax.set_yscale("log")
        ax.axhline(3.0, color="#999", lw=0.8, ls="--")
    style(ax1, "residual uplink (Mbps), downlink 2×", "receiver p50 (s, log)", f"Figure 2(1) — median completion against uplink, 500 nodes")
    style(ax2, "residual uplink (Mbps), downlink 2×", "receiver p99 (s, log)", f"Figure 2(2) — p99 against uplink")
    ax1.legend(fontsize=6.5, loc="upper right")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "fu_bandwidth.png"), dpi=140)
    plt.close(fig)


# ---------------------------------------------------------------- Figure F: payload size
PAYS = [("p128k", 128), ("p256k", 256), ("p384k", 384), ("p512k", 512), ("p640k", 640), ("p768k", 768), ("p896k", 896),
        ("p1m", 1024), ("p1536k", 1536), ("p2m", 2048)]
PAY_TICKS = ["128 KiB", "256", "384", "512", "640", "768", "896", "1 MiB", "1.5 MiB", "2 MiB"]


def share_series(arm, keys):
    """Per size: median (and ±1 s.d.) share of honest receivers complete by the deadline, in percent."""
    xs, med, lo, hi = [], [], [], []
    for x, kw in keys:
        vals = [rate3(r) for r in pick(arm, **kw) if rate3(r) is not None]
        if not vals:
            continue
        m, l, h = spread(vals)
        xs.append(x); med.append(100 * m); lo.append(100 * l); hi.append(100 * h)
    return xs, med, lo, hi


def pay_axes(ax):
    from matplotlib.ticker import NullFormatter
    from matplotlib.ticker import FixedLocator, FixedFormatter
    ax.xaxis.set_major_locator(FixedLocator([k for _, k in PAYS]))
    ax.xaxis.set_major_formatter(FixedFormatter(PAY_TICKS))
    ax.xaxis.set_minor_locator(FixedLocator([])); ax.xaxis.set_minor_formatter(NullFormatter())
    ax.tick_params(axis="x", labelsize=7.5)
    for lbl in ax.get_xticklabels():
        lbl.set_rotation(35); lbl.set_ha("right")


def figure_f():
    """The post's Figure 7: receiver p50 against payload size, hollow where the median seed leaves
    receivers past the deadline; one harness, one statistic, one grid."""
    fig, ax1 = plt.subplots(1, 1, figsize=(9, 5.8))
    keys = [(kib, dict(pay=pay)) for pay, kib in PAYS]
    for arm in SIX:  # the whole-message and bare lines moved to the ladder (Figure 2) on 2026-09-11
        l, c, m = ARMS[arm]
        ls = "--" if arm == "whole" else (":" if arm == "wholeot" else "-")
        xs, p50, lo, hi = series(arm, keys, "p50_s")
        sx, sh, slo, shi = share_series(arm, keys)
        share_at = dict(zip(sx, sh))
        if xs:
            ax1.plot(xs, p50, color=c, lw=1.2, ls=ls)
            ax1.plot([], [], color=c, lw=1.2, ls=ls, marker=m, ms=6, label=l)
            for x, y, l_, h_ in zip(xs, p50, lo, hi):
                errpt(ax1, x, y, yerr=[[y - l_], [h_ - y]] if h_ > l_ else None, color=c, marker=m, hollow=share_at.get(x, 100) < 99.9, ms=6)
    ax1.axhline(3.0, color="#999", lw=0.8, ls="--")
    ax1.text(135, 3.15, "3 s budget", fontsize=8, color="#666")
    ax1.set_xscale("log"); ax1.set_yscale("log")
    style(ax1, "execution payload size (uncompressed)", "receiver p50 (s, log)",
          "Figure 4 — receiver p50 against payload size, 500 nodes\nhollow marker: the median seed leaves receivers past the 3 s budget")
    pay_axes(ax1)
    handles, labels = ax1.get_legend_handles_labels()
    fig.legend(handles, labels, fontsize=7.5, loc="lower center", ncol=2, frameon=False, bbox_to_anchor=(0.5, 0.0))
    fig.tight_layout(rect=(0, 0.1, 1, 1))
    fig.savefig(os.path.join(OUT, "fu_payload.png"), dpi=140)
    plt.close(fig)


def figure_f_slide():
    """The talk's copy of Figure 7: one panel, short labels, legend below the plot."""
    short = {"wholend": "whole message, stock gossipsub", "whole": "whole message, one-outstanding IWANT",
             "wholeot": "whole message, A tuned's request policy", "bare": "segments on stock gossipsub, no rules",
             "atuned": "A tuned", "acoded": "A coded 32+32", "bfixed": "B tuned", "c64": "C, 64 topics", "c32": "C, 32 topics",
             "c32rs": "C + RS 32+32", "ccust": "C + RS, partial subscription"}
    fig, ax1 = plt.subplots(1, 1, figsize=(7.2, 6.4))
    keys = [(kib, dict(pay=pay)) for pay, kib in PAYS]
    for arm in ["wholend", "whole", "wholeot", "bare"] + SIX:
        _, c, m = ARMS[arm]; l = short[arm]
        ls = "--" if arm == "whole" else (":" if arm == "wholeot" else "-")
        xs, p50, lo, hi = series(arm, keys, "p50_s")
        sx, sh, slo, shi = share_series(arm, keys)
        share_at = dict(zip(sx, sh))
        if xs:
            ax1.plot(xs, p50, color=c, lw=1.2, ls=ls)
            ax1.plot([], [], color=c, lw=1.2, ls=ls, marker=m, ms=6, label=l)
            for x, y, l_, h_ in zip(xs, p50, lo, hi):
                errpt(ax1, x, y, yerr=[[y - l_], [h_ - y]] if h_ > l_ else None, color=c, marker=m, hollow=share_at.get(x, 100) < 99.9, ms=6)
    ax1.axhline(3.0, color="#999", lw=0.8, ls="--")
    ax1.text(135, 3.15, "3 s budget", fontsize=8, color="#666")
    ax1.set_xscale("log"); ax1.set_yscale("log")
    style(ax1, "execution payload size (uncompressed)", "receiver p50 (s, log)",
          "receiver p50 against payload size, 500 nodes — hollow: the median seed misses the 3 s budget")
    pay_axes(ax1)
    handles, labels = ax1.get_legend_handles_labels()
    fig.legend(handles, labels, fontsize=7.5, loc="lower center", ncol=2, frameon=False, bbox_to_anchor=(0.5, 0.0))
    fig.tight_layout(rect=(0, 0.16, 1, 1))
    fig.savefig(os.path.join(OUT, "fu_payload_slide.png"), dpi=150)
    plt.close(fig)


LADDER_STEPS = [("wholend", "whole message, stock gossipsub"), ("bare", "+ segmentation (full-mesh push, sequential publish)"),
                ("lad_batch", "+ batch publishing"), ("lad_split", "+ announce instead of push (push r = 2, announce the rest)"),
                ("lad_phase", "+ the phase (the push budget decays with IDONTWANT)")]
LADDER_COLORS = ["#777777", "#8c564b", "#e377c2", "#17becf", "#bcbd22"]


def ladder_panels(steps, colors, title, fname, nsize=500, dashed=(), ymax=None):
    """Curves against payload size on four panels: p50 and p99 stacked on the left (hollow where
    the median seed misses the budget); on the right, all bytes received per node (data plus
    control, in copies of the payload compressed whole) above the control bytes alone (KB), so the
    control share is visible and nothing is left out (author, 2026-09-15). Shared by the ladder
    (Figure 2), the discipline (3), the branches (5) and the closing (9). `ymax` caps the latency
    panels so a reference line can leave the figure where it leaves the budget (author, 2026-09-16)."""
    fig = plt.figure(figsize=(13, 7.4))
    gs = fig.add_gridspec(2, 2, width_ratios=[3, 2], hspace=0.10, wspace=0.16)
    ax1 = fig.add_subplot(gs[0, 0])
    ax2 = fig.add_subplot(gs[1, 0], sharex=ax1)
    ax3 = fig.add_subplot(gs[0, 1])
    ax4 = fig.add_subplot(gs[1, 1], sharex=ax3)
    keys = [(kib, dict(pay=pay, nsize=nsize)) for pay, kib in PAYS]
    if not any(pick(arm, pay=pay, nsize=nsize) for arm, _ in steps for pay, _ in PAYS):
        plt.close(fig); print(f"ladder at {nsize} nodes: no cells yet"); return
    top = {ax1: 0.0, ax2: 0.0}  # the highest bar end drawn on each latency panel
    for (arm, label), c in zip(steps, colors):
        m = ARMS[arm][2]
        sx, sh, slo, shi = share_series(arm, keys)
        share_at = dict(zip(sx, sh))
        drawn = False
        ls = "--" if arm in dashed else "-"
        for ax, stat in ((ax1, "p50_s"), (ax2, "p99_s")):
            xs, ys, lo, hi = series(arm, keys, stat)
            if not xs:
                continue
            ax.plot(xs, ys, color=c, lw=1.3, ls=ls)
            top[ax] = max(top[ax], max(hi))
            if not drawn:
                ax1.plot([], [], color=c, lw=1.3, ls=ls, marker=m, ms=6, label=label); drawn = True
            for x, y, l_, h_ in zip(xs, ys, lo, hi):
                errpt(ax, x, y, yerr=[[y - l_], [h_ - y]] if h_ > l_ else None, color=c, marker=m, hollow=share_at.get(x, 100) < 99.9, ms=6)
        # all bytes (data + control) in payload copies, and the control bytes alone in KB
        tx, tm, tlo, thi, cx, cm_, clo, chi = [], [], [], [], [], [], [], []
        for kib, kw in keys:
            rs = [r for r in pick(arm, **kw) if r.get("ok") and r.get("rx_node_bytes") is not None]
            if not rs:
                continue
            tot = [r["rx_node_bytes"] + (r.get("ctrl_rx_node_bytes") or 0) for r in rs]
            ctl = [r.get("ctrl_rx_node_bytes") or 0 for r in rs]
            m_, l_, h_ = spread(tot); tx.append(kib); tm.append(m_); tlo.append(l_); thi.append(h_)
            m_, l_, h_ = spread(ctl); cx.append(kib); cm_.append(m_); clo.append(l_); chi.append(h_)
        if tx:
            w = [WIRE[dict((k, p_) for p_, k in PAYS)[x]] for x in tx]
            curve(ax3, tx, [v / wi for v, wi in zip(tm, w)], [v / wi for v, wi in zip(tlo, w)], [v / wi for v, wi in zip(thi, w)], c, m, None, ls=ls)
        if cx and max(cm_) > 0:
            curve(ax4, cx, [v / 1e3 for v in cm_], [v / 1e3 for v in clo], [v / 1e3 for v in chi], c, m, None, ls=ls)
    for ax in (ax1, ax2):  # the budget line only where a curve comes near it; elsewhere it only stretches the y range
        if top[ax] > 2.5:
            ax.axhline(3.0, color="#999", lw=0.8, ls="--")
            ax.text(135, 3.15, "3 s budget", fontsize=8, color="#666")
    for ax in (ax1, ax2, ax3, ax4):
        ax.set_xscale("log"); ax.set_yscale("log")
    if ymax is not None:
        for ax in (ax1, ax2):
            ax.set_ylim(top=ymax)
    style(ax1, "", "receiver p50 (s, log)", f"{title}\nhollow: the median seed misses the 3 s budget")
    style(ax2, "execution payload size (uncompressed)", "receiver p99 (s, log)", "")
    ax3.axhline(1.0, color="#999", lw=0.8, ls=":")
    style(ax3, "", f"{COPIES_LABEL} (log)", "all bytes received per node, data + control")
    style(ax4, "execution payload size (uncompressed)", "control bytes received per node (KB, log)", "of which control")
    for ax in (ax1, ax2, ax3, ax4):
        pay_axes(ax)
    plt.setp(ax1.get_xticklabels(), visible=False); plt.setp(ax3.get_xticklabels(), visible=False)
    handles, labels = ax1.get_legend_handles_labels()
    fig.legend(handles, labels, fontsize=8, loc="lower center", ncol=2, frameon=False, bbox_to_anchor=(0.5, 0.005))
    fig.subplots_adjust(left=0.065, right=0.985, top=0.93, bottom=0.215, hspace=0.10, wspace=0.16)
    fig.savefig(os.path.join(OUT, fname), dpi=140)
    plt.close(fig)


def figure_ladder(nsize=500):
    """Figure 2: the six rungs, whole message to A tuned, against payload size."""
    tag = "Figure 2 — " if nsize == 500 else ""
    ladder_panels(LADDER_STEPS, LADDER_COLORS, f"{tag}the ladder against payload size, {nsize} nodes",
                  "fu_ladder.png" if nsize == 500 else f"fu_ladder_n{nsize}.png", nsize)


def figure_ladder_n1000():
    figure_ladder(1000)


def figure_discipline():
    """Part 1's Figure 3 (section 2.2, reorganised 2026-09-13): the phase rung and the disciplined pull that makes A tuned."""
    ladder_panels([("lad_phase", "the phase (Figure 2's last rung)"),
                   ("atuned", "+ disciplined pulls (one request per id, offer table, park) = A tuned")],
                  ["#bcbd22", "#1f77b4"], "Figure 3 — disciplined pulls against payload size, 500 nodes", "fu_discipline.png")


def figure_branches():
    """Part 1's Figure 5 (section 4): the discipline (A tuned) and the code built on it."""
    ladder_panels([("lad_phase", "the phase (Figure 2's last rung)"),
                   ("atuned", "+ disciplined pulls (one request per id, offer table, park) = A tuned"),
                   ("acoded", "A tuned + erasure code 32+32 with stop-pull"),
                   ("acodedcf", "the same code over the compressed payload (compress first)")],
                  ["#bcbd22", "#1f77b4", "#ff7f0e", "#d62728"], "Figure 5 — the code against payload size, 500 nodes", "fu_branches.png", dashed=("acodedcf",))


def figure_closing():
    """Part 1's Figure 9 (the closing, 2026-09-16; replaces the two-line size-rules figure): Figure 2's
    axes with where the ladder ends: whole message, A tuned at 32 and 16 KiB, the rules that follow the
    payload size, and coded A cut first and compress first. The latency panels are capped just above
    the budget, so whole message leaves the figure where it leaves the budget."""
    ladder_panels([LADDER_STEPS[0],
                   ("atuned", "A tuned (32 KiB, r = 2, the discipline)"),
                   ("am_k64", "A tuned at 16 KiB"),
                   ("aadapt", "rules that follow the size: 16 KiB; push r = 4 to 256 KiB, 3 to 1 MiB, 2 above; discipline from 512 KiB"),
                   ("acoded", "A coded 32+32 + stop-pull (cut first)"),
                   ("acodedcf", "the same code over the compressed payload (compress first)")],
                  [LADDER_COLORS[0], "#1f77b4", "#0b3d91", "#2ca02c", "#d62728", "#ff7f0e"],
                  "Figure 9 — where the ladder ends, 500 nodes", "fu_closing.png", dashed=("acodedcf",), ymax=3.6)


def figure_units(lines=None, title="Figure 5 — the unit size, 500-node headline base (1 MiB)", fname="fu_units.png", ctrl_note="control (B's harness does not separate it)", dashed=(), units=(8, 16, 32, 64), pay="p1m"):
    """The post's Figure 8: the unit size swept at the base (500 nodes, 1 MiB, clean, home) for five
    configurations. Four panels: p50 and p99 on the left, data bytes and control bytes on the right."""
    lines = lines or [("A tuned", ["am_k8k", "am_k64", "atuned", "am_k16"], "#1f77b4", "P"),
             ("A tuned + tail hedge (k = 3, h = 4)", ["ath8", "ath16", "atail_k3h4", "ath64"], "#7b0000", "X"),
             ("A coded 32+32 + stop-pull", ["acoded128", "acoded64", "acoded", "acoded16"], "#d62728", "^"),
             ("B tuned", ["bfixed8", "bfixed16", "bfixed", "bfixed64"], "#9467bd", "D"),
             ("C + RS 32+32", ["c128rs", "c64rs", "c32rs", "c16rs"], "#17becf", "s")]
    units = list(units)
    fig, axes = plt.subplots(2, 2, figsize=(12, 7.6))
    (ax1, ax3), (ax2, ax4) = axes
    for label, arms, c, m in lines:
        for ax, key, scale in ((ax1, "p50_s", 1.0), (ax2, "p99_s", 1.0), (ax3, "rx_node_bytes", 1 / WIRE[pay]), (ax4, "ctrl_rx_node_bytes", 1e-3)):
            xs, med, lo, hi = [], [], [], []
            for u, arm in zip(units, arms):
                rs = pick(arm, pay=pay)
                vals = [r[key] * scale for r in rs if r.get(key) is not None and r.get("ok")]
                if not vals or (key == "ctrl_rx_node_bytes" and max(vals) == 0):
                    continue
                m_, l_, h_ = spread(vals)
                xs.append(u); med.append(m_); lo.append(l_); hi.append(h_)
            if xs:  # the hedge adds no control traffic, so its control line is dashed to keep A tuned's visible beneath it
                curve(ax, xs, med, lo, hi, c, m, label if ax is ax1 else None, ls="--" if (label in dashed or (ax is ax4 and "hedge" in label)) else "-")
    from matplotlib.ticker import NullFormatter, NullLocator
    for ax in (ax1, ax2, ax3, ax4):
        ax.set_xscale("log", base=2)
    ax4.set_yscale("log")
    style(ax1, "", "receiver p50 (s)", title)
    style(ax2, "segment size", "receiver p99 (s)", "")
    ax3.axhline(1.0, color="#999", lw=0.8, ls=":")
    style(ax3, "", "copies of the compressed payload, per node", "bytes, in copies of the payload compressed whole")
    style(ax4, "segment size", "control bytes received per node (KB)", ctrl_note)
    for ax in (ax1, ax2, ax3, ax4):  # after style(): the unit ticks replace the log locator's
        ax.xaxis.set_minor_locator(NullLocator()); ax.xaxis.set_minor_formatter(NullFormatter())
        ax.set_xticks(units); ax.set_xticklabels([f"{u} KiB" for u in units])
    handles, labels = ax1.get_legend_handles_labels()
    fig.legend(handles, labels, fontsize=8, loc="lower center", ncol=3, frameon=False, bbox_to_anchor=(0.5, 0.0))
    fig.subplots_adjust(left=0.07, right=0.985, top=0.94, bottom=0.17, hspace=0.22, wspace=0.2)
    fig.savefig(os.path.join(OUT, fname), dpi=140)
    plt.close(fig)


def figure_units_a():
    """Part 1's Figure 7: the unit swept at the base for the A family only (A tuned, coded A)."""
    figure_units(lines=[("A tuned", ["am_k8k", "am_k64", "atuned", "am_k16"], "#1f77b4", "P"),
                        ("A coded 32+32 + stop-pull", ["acoded128", "acoded64", "acoded", "acoded16"], "#d62728", "^"),
                        ("the same code over the compressed payload (compress first)", ["acodedcf128", "acodedcf64", "acodedcf", "acodedcf16"], "#ff7f0e", "v")],
                 title="Figure 7 — the unit size for the A family, 500-node headline base (1 MiB)", fname="fu_units_a.png",
                 ctrl_note="control bytes", dashed=("the same code over the compressed payload (compress first)",))



def figure_units_a128():
    """Background only since 2026-09-16 (was Part 1's Figure 8): the segment size swept at 128 KiB, from 64 KiB
    (two segments) down to 2 KiB (sixty-four), for the same three lines as Figure 7. Arms are named by their shard count at 1 MiB; at 128 KiB the count is
    an eighth of the name."""
    figure_units(lines=[("A tuned", ["am_k2k", "am_k4k", "am_k8k", "am_k64", "atuned", "am_k16"], "#1f77b4", "P"),
                        ("A coded + stop-pull (K = the segment count, K parity)", ["acoded512", "acoded256", "acoded128", "acoded64", "acoded", "acoded16"], "#d62728", "^"),
                        ("the same code over the compressed payload (compress first)", ["acodedcf512", "acodedcf256", "acodedcf128", "acodedcf64", "acodedcf", "acodedcf16"], "#ff7f0e", "v")],
                 title="Figure 8 — the unit size for the A family at 128 KiB, 500 nodes", fname="fu_units_a128.png",
                 ctrl_note="control bytes", dashed=("the same code over the compressed payload (compress first)",),
                 units=(2, 4, 8, 16, 32, 64), pay="p128k")

NODE_SEEDS = (7, 8, 9)  # the node-count sweep above 500 nodes ran these three seeds (author's decision, 2026-09-15)


def figure_nodes():
    """Part 1's Figure 10 (section 6, the first skeptic's question): the four arms against network size at 1 MiB,
    250 to 4000 nodes, every seed a point has (ten to 500 nodes, three above; 125 nodes was measured and left out: the
    70-peer neighbourhood covers most of such a network, author 2026-09-15). Two panels,
    p50 and p99; hollow where the median seed misses the 3 s budget; whole message's by-3 s share written beside it."""
    arms = [("wholend", "whole message"), ("lad_phase", "the phase shift"), ("atuned", "A tuned"), ("acoded", "coded A")]
    ns = (250, 500, 1000, 2000, 4000)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 3.9), sharex=True)
    drew = False
    for arm, label in arms:
        _, c, m = ARMS[arm]
        for ax, stat in ((ax1, "p50_s"), (ax2, "p99_s")):
            xs, med, lo, hi, hol, shares = [], [], [], [], [], []
            for n in ns:
                rs = [r for r in pick(arm, nsize=n) if r.get("ok")]  # every seed the point has: ten to 500 nodes, three above
                vals = [r[stat] for r in rs if r.get(stat) is not None]
                if len(vals) < 3:
                    continue
                mm, l, h = spread(vals)
                sh = st.median([rate3(r) for r in rs if rate3(r) is not None])
                xs.append(n); med.append(mm); lo.append(l); hi.append(h); hol.append(sh < 0.999); shares.append(sh)
            if not xs:
                continue
            drew = True
            ax.plot(xs, med, color=c, lw=1.3)
            for x, y, l, h, ho, sh in zip(xs, med, lo, hi, hol, shares):
                errpt(ax, x, y, yerr=[[y - l], [h - y]] if h > l else None, color=c, marker=m, hollow=ho, ms=6)
                if arm == "wholend" and ax is ax1:
                    ax.annotate(f"{100 * sh:.0f}% by 3 s" if sh >= 0.01 else f"{100 * sh:.1f}% by 3 s", (x, y), xytext=(0, 7),
                                textcoords="offset points", ha="center", fontsize=7, color=c)
            if ax is ax1:
                ax1.plot([], [], color=c, lw=1.3, marker=m, ms=6, label=label)
    if not drew:
        plt.close(fig); print("nodes: no cells yet"); return
    for ax, name in ((ax1, "receiver p50 (s, log)"), (ax2, "receiver p99 (s, log)")):
        ax.set_xscale("log"); ax.set_yscale("log")
        ax.set_xticks(ns); ax.set_xticklabels([str(n) for n in ns]); ax.minorticks_off()
        ax.set_xlim(200, 5200)
        ax.set_yticks([0.5, 0.7, 1, 1.5, 2, 3, 5, 7]); ax.set_yticklabels(["0.5", "0.7", "1", "1.5", "2", "3", "5", "7"])
        ax.axhline(3.0, color="grey", ls="--", lw=0.8)
        ax.set_ylabel(name); ax.set_xlabel("nodes"); ax.grid(True, alpha=0.3)
    ax1.legend(fontsize=8, loc="center left")
    fig.suptitle("Figure 10 — the four arms against network size, 1 MiB (ten seeds per point to 500 nodes, three above)", fontsize=10)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "fu_nodes.png"), dpi=140); plt.close(fig)


CENS_Y = 6.0  # where a strand count is written when the p99 itself is not observable (more than 1% never completed)


MEM_LEFT = [("atuned", "A tuned (32 KiB)"), ("aplain400", "plain 400 ms discipline, no memory (control)"),
            ("am_k64", "A tuned, 16 KiB"), ("atail_k3h4", "A tuned + tail hedge (k = 3, h = 4)"),
            ("acoded", "A coded 32+32 + stop-pull")]
MEM_RIGHT = [("aphaseot", "offer table only, 400 ms window: memory without move-on or park (control)"),
             ("atuned", "A tuned (32 KiB)"), ("am_k64", "A tuned, 16 KiB"),
             ("atail_k3h4", "A tuned + tail hedge (k = 3, h = 4)"), ("acoded", "A coded 32+32 + stop-pull")]


def figure_memory_base():
    """The ablation controls (was Part 1's Figure 4 on 2026-09-13; replaced by the ladder version on 2026-09-14) —
    A tuned against the plain discipline without memory (left) and against the offer table without move-on or park (right)."""
    figure_memory(left=[a for a in MEM_LEFT if a[0] in ("atuned", "aplain400")],
                  right=[a for a in MEM_RIGHT if a[0] in ("aphaseot", "atuned")], num="ablation", fname="fu_memory_base.png")


LADDER_STRESS = [("wholend", "whole message, stock gossipsub"), ("bare", "+ segmentation (full-mesh push)"),
                 ("lad_batch", "+ batch publishing"), ("lad_split", "+ announce instead of push (r = 2)"),
                 ("lad_phase", "+ the phase shift"), ("atuned", "+ disciplined pulls = A tuned")]


def figure_memory_branches():
    """Part 1's Figure 6 (section 4): Figure 4's two panels with the coded line added, so the code is read
    against the rungs that explain it (author, 2026-09-16; the two-line version isolated A tuned and the code)."""
    lines = LADDER_STRESS + [("acoded", "+ erasure code 32+32 with stop-pull"),
                             ("acodedcf", "the same code over the compressed payload (compress first)", ARMS["acoded"][1])]
    figure_memory(left=lines, right=lines, num="6", fname="fu_memory_branches.png", dashed=("acodedcf",))


def figure_memory_ladder():
    """Part 1's Figure 4 (2026-09-14): the ladder's rungs under the two stresses, A tuned as the top rung."""
    figure_memory(left=LADDER_STRESS, right=LADDER_STRESS, num="4", fname="fu_memory_ladder.png")


def figure_memory(left=MEM_LEFT, right=MEM_RIGHT, num="8", fname="fu_memory.png", dashed=()):
    """The request memory's two sides. Left: p50 against uplink (the control is the plain discipline, no memory).
    Right: p99 against withholding dose (the control keeps the offer table but has no move-on or park); the strands
    on the worst seed are written beside each point that has any. The five-arm version (fu_memory.png) stays for the
    background doc; Part 1 uses the two subsets above."""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12.5, 4.8))
    ups = [10, 15, 20, 30, 50, 100, 200]
    def look(entry):
        """(arm, label[, colour]) -> arm, label, colour, marker; arms in `dashed` draw dashed with hollow markers."""
        arm, lab = entry[0], entry[1]
        _, c, m = ARMS[arm]
        return arm, lab, (entry[2] if len(entry) > 2 else c), m

    for entry in left:
        arm, lab, c, m = look(entry)
        keys = [(up, dict(net="home" if up == 50 else f"up{up}")) for up in ups]
        xs, p50, lo, hi = series(arm, keys, "p50_s")
        if xs:
            curve(ax1, xs, p50, lo, hi, c, m, lab, ls="--" if arm in dashed else "-", hollow=arm in dashed)
    ax1.set_xscale("log"); ax1.set_yscale("log"); ax1.axhline(3.0, color="#999", lw=0.8, ls="--")
    style(ax1, "residual uplink (Mbps), downlink 2×", "receiver p50 (s, log)", f"Figure {num}(1) — median completion against uplink, 500 nodes, 1 MiB")
    from matplotlib.ticker import NullFormatter, NullLocator
    ax1.xaxis.set_minor_locator(NullLocator()); ax1.xaxis.set_minor_formatter(NullFormatter())
    ax1.set_xticks(ups); ax1.set_xticklabels([str(u) for u in ups])
    phis = [0, 10, 20, 30, 50, 70]
    for entry in right:
        arm, lab, c, m = look(entry)
        keys = [(phi, dict(fault="clean" if phi == 0 else f"wh{phi}")) for phi in phis]
        xs, p99, lo, hi = series(arm, keys, "p99_s")
        if xs:
            curve(ax2, xs, p99, lo, hi, c, m, lab, ls="--" if arm in dashed else "-", hollow=arm in dashed)
        for x, kw in keys:
            rs = pick(arm, **kw)
            if not rs:
                continue
            v = victims(rs)
            obs = [r["p99_s"] for r in rs if r.get("p99_s") is not None]
            y = st.median(obs) if obs else CENS_Y
            if any(censored(r) for r in rs) or not obs:
                ax2.plot(x, y, marker=m, ms=9, mfc="none", mec=c, ls="none")
            if v:
                ax2.annotate(str(v), (x, y), textcoords="offset points", xytext=(5, 4), fontsize=7, color=c)
    ax2.axhline(3.0, color="#999", lw=0.8, ls="--")
    style(ax2, "share of nodes withholding (%)", "receiver p99 (s); a number = strands on the worst seed",
          f"Figure {num}(2) — p99 against the share of nodes withholding, 500 nodes, 1 MiB")  # the withholder is defined in the caption
    if [e[0] for e in left] == [e[0] for e in right]:
        # one legend for both panels, below them (eight lines no longer fit inside the withholding panel, 2026-09-16)
        h, l = ax1.get_legend_handles_labels()
        fig.legend(h, l, fontsize=7, loc="lower center", ncol=4, frameon=False, bbox_to_anchor=(0.5, -0.01))
        fig.tight_layout(rect=(0, 0.09, 1, 1))
    else:
        ax1.legend(fontsize=6.5, loc="upper right"); ax2.legend(fontsize=6.5, loc="upper left")
        fig.tight_layout()
    fig.savefig(os.path.join(OUT, fname), dpi=140)
    plt.close(fig)


def figure_f_full():
    """The background's copy: the fixed-count rule, the first post's curves (three seeds, means) and the
    publisher's offered load, on the same grid."""
    pays = PAYS
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    for arm in SIX + ["whole", "wholend", "wholeot", "bare"]:
        l, c, m = ARMS[arm]
        keys = [(kib, dict(pay=pay)) for pay, kib in pays]
        xs, p50, lo, hi = series(arm, keys, "p50_s")
        if xs:
            curve(ax1, xs, p50, lo, hi, c, m, l)
        xs, pub, lo, hi = series(arm, keys, "publisher_bytes")
        if xs:
            curve(ax2, xs, [v / MB for v in pub], [v / MB for v in lo], [v / MB for v in hi], c, m, l)
        # fixed-count rule, hollow
        keys = [(kib, dict(pay=pay + "-fc")) for pay, kib in pays]
        xs, p50, lo, hi = series(arm, keys, "p50_s")
        if xs:
            curve(ax1, xs, p50, lo, hi, c, m, f"{l.split(':')[0]} — fixed count", ms=8, lw=0.8, ls="--", hollow=True)
        xs, pub, lo, hi = series(arm, keys, "publisher_bytes")
        if xs:
            curve(ax2, xs, [v / MB for v in pub], [v / MB for v in lo], [v / MB for v in hi], c, m, None, ms=8, lw=0.8, ls="--", hollow=True)
    # the first post's Figure 1 curves (Q50 re-sweep: means of per-seed p50), hollow black/grey
    try:
        kib = load_list_literal(os.path.join(FIGDIR, "payload_sweep.py"), "kib")
        whole = load_list_literal(os.path.join(FIGDIR, "payload_sweep.py"), "whole")
        seg = load_list_literal(os.path.join(FIGDIR, "payload_sweep.py"), "seg")
        ax1.plot(kib, whole, color="#111", marker="o", mfc="none", ms=6, lw=0.8, ls=":", label="whole message, first post (3 seeds, means of p50)")
        ax1.plot(kib, seg, color="#1f77b4", marker="o", mfc="none", ms=6, lw=0.8, ls=":", label="segmented, first post (plain 1000 ms discipline; 3 seeds, means)")
    except Exception as e:
        print("Q50 overlay skipped:", e)
    ax1.axhline(3.0, color="#999", lw=0.8, ls="--")
    for ax in (ax1, ax2):
        ax.set_xscale("log"); ax.set_yscale("log")
    style(ax1, "execution payload size (uncompressed)", "receiver p50 (s, log)",
          "Figure 7 (background) — receiver p50 against payload size, 500 nodes\nhollow dashed = fixed segment count; dotted = the first post's curves")
    style(ax2, "execution payload size (uncompressed)", "publisher offered load (MB, log)", "the publisher's offered load against size (enqueue side, not wire bytes)")
    for ax in (ax1, ax2):
        pay_axes(ax)
    ax1.legend(fontsize=6, loc="upper left")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "fu_payload_full.png"), dpi=140)
    plt.close(fig)


# ---------------------------------------------------------------- Tables B-E (markdown)
def fmt_ms(v):
    return "—" if v is None else f"{v * 1000:.0f}"


def tables():
    lines = [f"<!-- generated by fu_figures.py from {os.path.basename(RES)}; {SEEDTAG}; values are medians across the seeds present -->", ""]
    lines += [f"**Table 2 — A's request configurations under the adversary battery.** Cells: complete by 3 s % / p99 ms / honest receivers never completing, worst seed ('cens' = an honest receiver never completed, so the p99 is censored; 'n/a' = the harness reports no p99 because the attackers, who never complete by construction, exceed 1% of nodes); clean column: MB received per node · p50 ms. 'not run' = cell not scheduled.", ""]
    afaults = [("wh30", "F1 withholding 30%"), ("wh50", "50%"), ("wh70", "70%"), ("sil30", "silent relay 30%"), ("sil50", "50%"), ("whsp30", "spoof + withhold 30%"), ("whsp50", "50%")]
    lines += ["| request configuration | clean | " + " | ".join(f for _, f in afaults) + " |", "|---|---|" + "---|" * len(afaults)]
    for arm in ["aphaseot", "ahedge", "atuned", "acoded"]:
        row = [ARMS[arm][0]]
        rs = pick(arm)
        if rs:
            rx, _, _ = agg(rs, "rx_node_bytes"); p50, _, _ = agg(rs, "p50_s")
            row.append(f"{rx / MB:.2f} · {fmt_ms(p50)}")
        else:
            row.append("not run")
        for f, _ in afaults:
            rs = pick(arm, fault=f)
            if not rs:
                row.append("not run"); continue
            r3 = st.median([rate3(r) for r in rs]); p99, _, _ = agg(rs, "p99_s")
            stranded = victims(rs)
            p99s = "cens" if any(censored(r) for r in rs) else (fmt_ms(p99) if p99 is not None else "n/a")
            row.append(f"{r3 * 100:.0f}% / {p99s} / {stranded}")
        lines.append("| " + " | ".join(row) + " |")
    lines += ["", f"**Table 1 — the reference configurations at the headline base.** p50 / p99 / last receiver in ms; MB received per node (encoded application data; the pre-fix B row is on its raw wire, the reference B compressed); publisher MB; control KB per node (B's partial-message control bytes are not in this counter); messages per node — duplicate data messages received and IDONTWANTs sent for the gossip arms, segment copies received and partial-message RPCs received for B — because bytes hide message frequency and the harness charges no CPU per message; complete by 3 s.", ""]
    lines += ["| configuration | p50 | p99 | last | MB rx/node | publisher MB | ctrl KB/node | msgs/node | by 3 s |", "|---|---|---|---|---|---|---|---|---|"]
    for arm in SIX + ["bcodedf", "btuned", "aplain400", "whole"]:
        rs = pick(arm)
        if not rs:
            lines.append(f"| {ARMS[arm][0]} | not run | | | | | | | |"); continue
        p50, _, _ = agg(rs, "p50_s"); p99, _, _ = agg(rs, "p99_s"); last, _, _ = agg(rs, "last_s")
        rx, _, _ = agg(rs, "rx_node_bytes"); pub, _, _ = agg(rs, "publisher_bytes"); ctrl, _, _ = agg(rs, "ctrl_rx_node_bytes")
        r3 = st.median([rate3(r) for r in rs])
        nodes = (rs[0].get("total") or 499) + 1
        if rs[0].get("b_copies") is not None:
            cp, _, _ = agg(rs, "b_copies"); rp, _, _ = agg(rs, "partial_rx_rpcs")
            msgs = f"{cp:.2f} copies · {rp / nodes:.0f} RPCs" if rp is not None else f"{cp:.2f} copies"
        else:
            du, _, _ = agg(rs, "dups"); idw, _, _ = agg(rs, "idw_tx")
            msgs = f"{du / nodes:.0f} dup · {idw / nodes:.0f} IDW" if du is not None and idw is not None else "—"
        lines.append(f"| {ARMS[arm][0]} | {fmt_ms(p50)} | {fmt_ms(p99)} | {fmt_ms(last)} | {rx / MB:.2f} | {pub / MB:.1f} | {'—' if ctrl is None else f'{ctrl / 1000:.0f}'} | {msgs} | {r3 * 100:.1f}% |")
    lines += ["", "<!-- KEY 1S -->", "**Table 1 — the six segmented configurations and whole message at the headline base.** Receiver p50 and p99 in ms; encoded data received per node in MB and as copies of the payload compressed whole (746 KB); share of receivers complete by 3 s.", ""]
    lines += ["| configuration | p50 | p99 | MB rx/node | copies | by 3 s |", "|---|---|---|---|---|---|"]
    for arm in SIX + ["wholend"]:
        rs = pick(arm)
        if not rs:
            continue
        p50, _, _ = agg(rs, "p50_s"); p99, _, _ = agg(rs, "p99_s"); rx, _, _ = agg(rs, "rx_node_bytes")
        r3 = st.median([rate3(r) for r in rs])
        lines.append(f"| {ARMS[arm][0]} | {fmt_ms(p50)} | {fmt_ms(p99)} | {rx / MB:.2f} | {rx / WIRE['p1m']:.2f} | {r3 * 100:.1f}% |")
    lines += ["", f"**Table 3 — the φ=30 battery and omission.** Cells: complete by 3 s % / p99 ms / honest receivers never completing, worst seed (attackers that never complete by construction are not counted). 'not run' = injector does not reach the arm or cell not scheduled.", ""]
    faults = [("wh30", "F1 withholding 30%"), ("sil30", "silent relay 30%"), ("whsp30", "spoof + withhold 30%"), ("omit4", "omit 4 of K"), ("omit8", "omit 8 (=R)"), ("omit9", "omit 9 (=R+1)")]
    lines += ["| configuration | " + " | ".join(f for _, f in faults) + " |", "|---|" + "---|" * len(faults)]
    for arm in SIX:
        row = [ARMS[arm][0]]
        for f, _ in faults:
            rs = pick(arm, fault=f)
            if not rs:
                row.append("see Q73" if arm in ("atuned", "acoded") and f in ("wh30", "sil30", "whsp30") else "not run"); continue
            r3 = st.median([rate3(r) for r in rs]); p99, _, _ = agg(rs, "p99_s")
            stranded = victims(rs)
            p99s = "cens" if any(censored(r) for r in rs) else (fmt_ms(p99) if p99 is not None else "n/a")
            row.append(f"{r3 * 100:.0f}% / {p99s} / {stranded}")
        lines.append("| " + " | ".join(row) + " |")
    lines += ["", f"**Table 4 — whole message and the six configurations in the first post's three scenarios.** p50 / p99 / last ms · MB rx/node · publisher MB.", ""]
    lines += ["| configuration | home builder, all home | home builder, 20% datacenter | datacenter builder, 20% datacenter |", "|---|---|---|---|"]
    for arm in ["whole"] + SIX:
        cells = []
        for net in ("home", "dc20", "dcb20"):
            rs = pick(arm, net=net)
            if not rs:
                cells.append("not run"); continue
            p50, _, _ = agg(rs, "p50_s"); p99, _, _ = agg(rs, "p99_s"); last, _, _ = agg(rs, "last_s"); rx, _, _ = agg(rs, "rx_node_bytes"); pub, _, _ = agg(rs, "publisher_bytes")
            flag = " †" if any(r.get("mesh_unsettled") for r in rs) else ""
            cells.append(f"{fmt_ms(p50)} / {fmt_ms(p99)} / {fmt_ms(last)} · {rx / MB:.2f} · {pub / MB:.1f}{flag}")
        lines.append(f"| {ARMS[arm][0]} | " + " | ".join(cells) + " |")
    if any(r.get("mesh_unsettled") for r in recs):
        lines += ["", "† one seed's meshes had not reached the settle band when the payload was published (about 56 of 500 nodes under Dlo across their 64 topics on that draw); the cell is the diffusion on the mesh that formed."]
    lines += ["", f"**Table 5 — payload size.** Per size: p50 ms · MB rx/node · publisher MB; 'cliff' = largest size with every receiver complete by 3 s (fixed 32 KiB / 16 KiB rule); fc = fixed-count rule where run.", ""]
    pays = [p for p, _ in PAYS]
    lines += ["| configuration | " + " | ".join(pays) + " | cliff |", "|---|" + "---|" * (len(pays) + 1)]
    for arm in SIX + ["bare", "whole", "wholend", "wholeot"]:
        row = [ARMS[arm][0]]; cliff = "—"
        for pay in pays:
            rs = pick(arm, pay=pay)
            fc = pick(arm, pay=pay + "-fc")
            if not rs:
                row.append("not run" if not fc else f"fc: {fmt_ms(agg(fc, 'p50_s')[0])}"); continue
            p50, _, _ = agg(rs, "p50_s"); rx, _, _ = agg(rs, "rx_node_bytes"); pub, _, _ = agg(rs, "publisher_bytes")
            r3 = st.median([rate3(r) for r in rs])
            cell = f"{fmt_ms(p50)} · {rx / MB:.2f} · {pub / MB:.1f}"
            if fc:
                cell += f" (fc {fmt_ms(agg(fc, 'p50_s')[0])})"
            row.append(cell)
            if r3 >= 1.0:
                cliff = pay
        row.append(cliff)
        lines.append("| " + " | ".join(row) + " |")
    with open(os.path.join(OUT, "fu_tables.md"), "w") as f:
        f.write("\n".join(lines) + "\n")
    print("tables ->", os.path.join(OUT, "fu_tables.md"))


# Fixed-count rule (K = 32 at every size, units of P/32) for every ladder rung, seed 7 first (Q97,
# 2026-09-16). Those cells live in their own log set (logs_fc/ -> fu_results_fc.json) so the seed-7
# records of the older ten-seed A tuned fixed-count cells (128/256 KiB, 2 MiB) are not overwritten.
RES_FC = os.path.join(os.path.dirname(RES), "fu_results_fc.json") if os.path.exists(os.path.join(os.path.dirname(RES), "fu_results_fc.json")) else os.path.join(HERE, "results_fc.json")
recs_fc = json.load(open(RES_FC)) if os.path.exists(RES_FC) else []


def figure_fixedcount():
    """Per rung: the ladder at fixed 32 KiB units (solid) against the fixed-count rule, K = 32 at
    every size (dashed, hollow); both ten-seed medians with ±1 s.d. bars (seeds 7-16, lanes fu-fc
    and fu-fc10). Rows: p50, p99, all bytes received per node in copies of the compressed payload.
    A tuned also carries the older ten-seed fixed-count points (grey hollow squares) as the
    same-binary check."""
    steps = [("bare", "segmentation alone\n(full-mesh push, sequential publish)"), ("lad_batch", "+ batch publishing"),
             ("lad_split", "+ announce instead of push\n(push r = 2, announce the rest)"), ("lad_phase", "+ the phase\n(push budget decays with IDONTWANT)"),
             ("atuned", "+ disciplined pulls\n= A tuned")]
    colors = LADDER_COLORS[1:] + ["#1f77b4"]
    fig, axes = plt.subplots(3, len(steps), figsize=(3.1 * len(steps), 9.0), sharex=True)  # y per panel: the shape within a rung is the point
    keys = [(kib, pay) for pay, kib in PAYS]

    def fc_series(arm, key):
        """Per size: (median, lo, hi) of key over the fixed-count cells; at 1 MiB the two rules coincide,
        so the 32 KiB cells stand in and the dashed curve is continuous."""
        xs, ys, lo, hi = [], [], [], []
        for kib, pay in keys:
            rs = [r for r in recs_fc if r["arm"] == arm and r["pay"] == pay + "-fc" and r["fault"] == "clean" and r["net"] == "home" and r.get("nsize", 500) == 500 and r.get(key) is not None]
            if pay == "p1m":
                rs = [r for r in pick(arm) if r.get(key) is not None]
            if rs:
                m, l, h = spread([r[key] for r in rs])
                xs.append(kib); ys.append(m); lo.append(l); hi.append(h)
        return xs, ys, lo, hi

    for col, ((arm, label), c) in enumerate(zip(steps, colors)):
        m = ARMS[arm][2]
        for row, (key, scale) in enumerate((("p50_s", None), ("p99_s", None), ("rx_node_bytes", "copies"))):
            ax = axes[row][col]
            # fixed 32 KiB units, ten seeds
            xs, ys, lo, hi = series(arm, [(kib, dict(pay=pay)) for kib, pay in keys], key)
            conv = lambda x, v: v / WIRE[[pay for kib, pay in keys if kib == x][0]]  # bytes -> copies of the compressed payload
            if scale == "copies":
                ys = [conv(x, v) for x, v in zip(xs, ys)]; lo = [conv(x, v) for x, v in zip(xs, lo)]; hi = [conv(x, v) for x, v in zip(xs, hi)]
            if xs:
                curve(ax, xs, ys, lo, hi, c, m, "32 KiB units (count follows size)" if row == 0 else None, ms=5)
            # fixed count, seed 7 (one binary)
            fx, fy, flo, fhi = fc_series(arm, key)
            if scale == "copies":
                fy = [conv(x, v) for x, v in zip(fx, fy)]; flo = [conv(x, v) for x, v in zip(fx, flo)]; fhi = [conv(x, v) for x, v in zip(fx, fhi)]
            if fx:
                curve(ax, fx, fy, flo, fhi, c, m, "K = 32 at every size (unit follows size)" if row == 0 else None, ms=6, lw=1.0, ls="--", hollow=True)
            # the older ten-seed fixed-count cells (A tuned only)
            oxs, oys, olo, ohi = series(arm, [(kib, dict(pay=pay + "-fc")) for kib, pay in keys], key)
            if oxs:
                if scale == "copies":
                    oys = [conv(x, v) for x, v in zip(oxs, oys)]; olo = [conv(x, v) for x, v in zip(oxs, olo)]; ohi = [conv(x, v) for x, v in zip(oxs, ohi)]
                for x, y, l_, h_ in zip(oxs, oys, olo, ohi):
                    errpt(ax, x, y, yerr=[[y - l_], [h_ - y]] if h_ > l_ else None, color="#555555", marker="s", hollow=True, ms=7,
                          label="K = 32, the older ten-seed cells" if (row == 0 and x == oxs[0]) else None)
            if row == 0:
                ax.set_title(label, fontsize=8.5)
                ax.legend(fontsize=6.5, loc="upper left", frameon=False)
            ax.grid(True, alpha=0.25)
            ax.set_ylim(bottom=0)
            if row == 2:
                pay_axes(ax)
        axes[2][col].set_xlabel("execution payload size (uncompressed)", fontsize=8)
    axes[0][0].set_ylabel("receiver p50 (s)"); axes[1][0].set_ylabel("receiver p99 (s)"); axes[2][0].set_ylabel(COPIES_LABEL, fontsize=8)
    for ax in axes[0]: ax.axhline(3.0, color="grey", ls=":", lw=0.8)
    for ax in axes[1]: ax.axhline(3.0, color="grey", ls=":", lw=0.8)
    fig.suptitle("The ladder under two rules for the unit: 32 KiB units (solid) against K = 32 segments at every size (dashed), ten seeds each, 500 nodes, home network", fontsize=10)
    fig.tight_layout(rect=(0, 0, 1, 0.965))
    fig.savefig(os.path.join(OUT, "fu_fixedcount.png"), dpi=130); plt.close(fig)
    print("fixedcount ->", os.path.join(OUT, "fu_fixedcount.png"), f"({len(recs_fc)} fixed-count cells)")



PART1 = (figure_d, figure_ladder, figure_discipline, figure_memory_ladder, figure_branches, figure_memory_branches,
         figure_units_a, figure_nodes, figure_closing)  # Figures 1-9 of the first post, in order
EVERYTHING = (figure_a, figure_amap, figure_b, figure_c, figure_d, figure_e, figure_f, figure_f_slide, figure_f_full, figure_ladder, figure_ladder_n1000, figure_branches, figure_closing, figure_units, figure_units_a, figure_memory, figure_memory_base, figure_memory_branches, figure_memory_ladder, figure_discipline, figure_nodes, figure_fixedcount, tables)

if __name__ == "__main__":
    os.makedirs(OUT, exist_ok=True)
    for fn in (EVERYTHING if "--all" in sys.argv else PART1):
        fn()
        print("ok", fn.__name__)
