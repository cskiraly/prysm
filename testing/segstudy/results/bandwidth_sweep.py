#!/usr/bin/env python3
# Q66b: completion vs residual uplink (n=500, degree 70, geo, downlink = 2x uplink,
# discipline 400 ms, seeds 7-9; markers = per-arm median of per-seed p50s, bars = seed range).
# The knee for a 1 MiB payload sits between 15 and 10 Mbps: at 20 every arm holds the 3 s
# reveal budget, at 10 every arm completes but none is timely, and the coded stack - fastest
# at 50 Mbps - misses hardest, its +0.8 copies priced at scarcity. Byte-lean pull-only gains
# as capacity shrinks. 50 Mbps sits off-axis: the headline base (aphase p50 756 ms).
# Data literals for fu_figures.py --all (the first post's uplink sweep); renders on its own with python3.
import statistics as st

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FixedLocator, NullFormatter, FuncFormatter

DATA = {  # arm -> {uplink: [p50 s, seeds 7,8,9]}
    "A pull-only": {10: [5.183, 3.843, 3.272], 15: [2.195, 1.746, 2.424],
                    20: [1.668, 1.511, 1.725], 30: [1.318, 1.115, 0.994]},
    "A + phase r=2": {10: [4.389, 3.335, 3.405], 15: [2.886, 2.028, 2.277],
                      20: [1.464, 1.449, 1.414], 30: [1.054, 1.004, 0.943]},
    "A coded 32+32 + phase + stop-pull": {10: [4.977, 4.283, 4.030], 15: [2.548, 2.291, 2.153],
                                          20: [1.838, 1.675, 1.488], 30: [1.184, 1.015, 1.030]},
}
STYLE = {"A pull-only": ("#7f4fc9", "P"), "A + phase r=2": ("#1f77b4", "P"),
         "A coded 32+32 + phase + stop-pull": ("#d62728", "^")}
fig, ax = plt.subplots(figsize=(8.6, 5.4))
for arm, pts in DATA.items():
    xs = sorted(pts)
    med = [st.median(pts[x]) for x in xs]
    lo = [st.median(pts[x]) - min(pts[x]) for x in xs]
    hi = [max(pts[x]) - st.median(pts[x]) for x in xs]
    c, m = STYLE[arm]
    ax.errorbar(xs, med, yerr=[lo, hi], marker=m, color=c, ms=7, lw=1.4,
                elinewidth=0.9, capsize=2, label=arm)
ax.axhline(3.0, color="#777777", lw=0.9, ls="--", alpha=0.7)
ax.annotate("3 s reveal-relative budget", (21, 3.08), fontsize=8.5, color="#777777")
ax.annotate("all complete, none timely", (10.1, 5.35), fontsize=8.5, color="#333333")
ax.set_xlabel("uplink (Mbps), downlink = 2×  —  headline base sits at 50/100")
ax.set_ylabel("median completion p50 (s)")
ax.set_xscale("log")
fmt = FuncFormatter(lambda v, _: f"{v:g}")
ax.xaxis.set_major_locator(FixedLocator([10, 15, 20, 30]))
ax.xaxis.set_major_formatter(fmt)
ax.xaxis.set_minor_formatter(NullFormatter())
ax.set_ylim(0.7, 5.7)
ax.legend(fontsize=8.5, frameon=False, loc="upper right")
ax.set_title("Q66b — residual-bandwidth sweep: the capacity knee is between 15 and 10 Mbps\n"
             "n=500, degree 70, geo, seeds 7–9 (bars = seed range)", fontsize=10)
ax.grid(True, alpha=0.25, lw=0.5)
fig.tight_layout()
fig.savefig("notes/figures/bandwidth_sweep.png", dpi=130)
print("written")
