#!/usr/bin/env python3
# Q65: the production-shape frontier — n=1000, degree 70, geo latency (one-way mean 56 ms),
# 50/100 Mbps, IWANT discipline 400 ms except the stock star, SEGMENT_DET_RAND=1.
# Markers sit at per-arm MEDIANS; bars span the seed range. Base arms carry 8+ seeds,
# extension arms 8+ (seed-major sweep, growing); the coded pair and A+phase carry 20 (7-26, claim batch); k64d16m2 carries 12 (7-18, Hetzner batch).
# X is completion (time until every node holds the payload), Y is payload equivalents
# received per node (rx/node over the ~746 KB compressed payload; B arms report copies).
# Data: the fleet box's out/{fleet,tier1,tier2,tier3,sweep} and the local q65_*.log,
# aggregated by the parser recorded in experiments.md Q65.
#
# Data literals for fu_figures.py --all (the first post's frontier); renders on its own with python3.

import statistics as st

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

DATA = {
  "aphase": {"seeds": [7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26], "x": [1.124, 1.038, 1.238, 1.182, 1.055, 1.072, 1.171, 1.262, 1.172, 1.109, 1.394, 1.239, 1.12, 1.11, 1.269, 1.192, 1.208, 1.081, 1.212, 1.124], "y": [1.275, 1.275, 1.206, 1.296, 1.278, 1.275, 1.286, 1.28, 1.271, 1.282, 1.275, 1.28, 1.269, 1.278, 1.269, 1.267, 1.281, 1.267, 1.274, 1.271]},
  "aphasedelay": {"seeds": [7, 8, 9, 10, 11, 12, 13, 14, 15], "x": [1.194, 1.206, 1.186, 1.208, 1.265, 1.089, 1.119, 1.224, 1.336], "y": [1.283, 1.263, 1.209, 1.291, 1.278, 1.267, 1.28, 1.279, 1.268]},
  "aplain": {"seeds": [7, 8, 9, 10, 11, 12, 13, 14, 15], "x": [1.209, 1.247, 1.139, 1.264, 1.203, 1.314, 1.193, 1.27, 1.273], "y": [5.543, 5.727, 5.825, 5.765, 5.531, 5.985, 5.459, 5.713, 5.785]},
  "apull": {"seeds": [7, 8, 9, 10, 11, 12, 13, 14, 15], "x": [1.339, 1.355, 1.338, 1.283, 1.312, 1.215, 1.327, 1.316, 1.339], "y": [1.05, 1.039, 1.09, 1.047, 1.042, 1.018, 1.05, 1.024, 1.047]},
  "bphase": {"seeds": [7, 8, 9, 10, 11, 12, 13, 14, 15], "x": [2.295, 2.805, 2.0, 2.509, 2.392, 2.467, 2.26, 2.61, 2.616], "y": [1.17, 1.19, 1.17, 1.22, 1.2, 1.18, 1.19, 1.18, 1.21]},
  # bpull is measured but not plotted: pure request/response with legacy-derived pacing.
  # Q74 attributed the 13-16 s to first-holder mobbing + reissue storm and fixed it with the
  # per-peer claim cap: n=500 2.4-3.1 s / 1.03-1.25 copies; n=1000 (cap4+adt, s7) 3.57 s / 1.09.
  # timers costs ~14-16 s on geo (the "RTT price, geo-doubled") and stretched the x-axis by a
  # third for a never-a-candidate baseline. Data kept for the record.
  "bpull": {"seeds": [7, 8, 9, 10, 12, 13, 14], "x": [13.786, 13.394, 16.415, 15.805, 15.598, 13.57, 15.223], "y": [2.55, 2.35, 2.96, 3.09, 2.78, 2.69, 2.51]},
  "bsplit": {"seeds": [7, 8, 9, 10, 11, 12, 13, 14, 15], "x": [2.053, 2.37, 1.741, 2.442, 2.149, 2.047, 2.151, 2.354, 2.488], "y": [2.71, 2.85, 2.84, 2.84, 2.87, 2.67, 2.88, 2.76, 2.86]},
  "bsplit1": {"seeds": [7, 8, 9, 10, 11, 12, 13, 14, 15], "x": [2.449, 2.181, 1.822, 2.364, 1.905, 1.983, 2.143, 2.275, 2.448], "y": [1.92, 1.86, 1.99, 1.94, 1.92, 1.9, 1.89, 1.9, 1.93]},
  "bsplit3": {"seeds": [7, 8, 9, 10, 11, 12, 13, 14, 15], "x": [2.348, 2.209, 2.315, 2.826, 2.299, 2.35, 2.224, 2.474, 2.186], "y": [3.64, 3.53, 3.36, 3.26, 3.61, 3.26, 3.65, 3.28, 3.68]},
  "bsplit4": {"seeds": [7, 8, 9, 10, 11, 12, 13, 14, 15], "x": [2.37, 2.564, 2.524, 2.396, 2.345, 2.398, 2.317, 2.817, 2.408], "y": [3.82, 4.03, 6.13, 3.91, 4.05, 5.88, 4.44, 5.04, 3.87]},
  "codedphase": {"seeds": [7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26], "x": [0.983, 0.941, 0.892, 1.107, 0.972, 0.967, 0.997, 1.053, 0.958, 1.02, 1.058, 1.025, 0.926, 1.048, 0.999, 0.998, 1.039, 0.949, 0.943, 0.983], "y": [2.097, 2.135, 1.787, 2.141, 2.019, 1.98, 2.069, 2.008, 2.045, 2.274, 2.151, 2.164, 1.986, 2.152, 1.917, 2.158, 2.254, 2.074, 2.107, 2.035]},
  "codedpull": {"seeds": [7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26], "x": [1.275, 1.229, 1.163, 1.264, 1.165, 1.327, 1.208, 1.287, 1.402, 1.226, 1.122, 1.233, 1.191, 1.321, 1.098, 1.244, 1.355, 1.171, 1.072, 1.184], "y": [2.285, 2.126, 2.282, 2.069, 2.153, 2.238, 2.046, 2.214, 2.431, 2.077, 2.14, 2.009, 2.047, 2.339, 2.102, 2.113, 2.188, 2.26, 2.033, 2.042]},
  "cphase": {"seeds": [7, 8, 9, 10, 11, 12, 13, 14, 15], "x": [1.353, 1.309, 1.266, 1.143, 1.16, 1.258, 1.236, 1.353, 1.026], "y": [1.294, 1.25, 1.313, 1.268, 1.255, 1.252, 1.271, 1.257, 1.251]},
  "cphase32": {"seeds": [7, 8, 9, 10, 11, 12, 13, 14, 15], "x": [1.405, 1.415, 1.382, 1.458, 1.32, 1.557, 1.518, 1.444, 1.27], "y": [1.299, 1.322, 1.277, 1.315, 1.246, 1.311, 1.27, 1.349, 1.27]},
  "dcustR0": {"seeds": [7, 8, 9, 10, 11, 12, 13, 14], "x": [2.14, 2.157, 2.095, 2.228, 2.124, 2.171, 2.122, 2.089], "y": [7.762, 7.777, 8.774, 7.675, 8.055, 7.605, 7.419, 7.588]},
  "dcustR16": {"seeds": [7, 8, 9, 10, 11, 12, 13, 14], "x": [1.927, 1.876, 1.692, 1.898, 2.019, 1.934, 1.848, 1.93], "y": [6.231, 6.356, 7.737, 6.885, 6.839, 6.982, 6.847, 6.832]},
  "dcustR8": {"seeds": [7, 8, 9, 10, 11, 12, 13, 14], "x": [2.013, 1.979, 1.858, 2.005, 2.019, 2.011, 2.006, 1.94], "y": [7.497, 6.735, 8.416, 7.494, 7.229, 6.899, 6.976, 6.327]},
  "k64d16m2": {"seeds": [7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18], "x": [1.466, 0.941, 1.211, 0.915, 1.185, 1.196, 0.92, 1.094, 1.131, 0.927, 1.194, 1.118], "y": [1.337, 1.318, 1.232, 1.329, 1.371, 1.354, 1.314, 1.33, 1.33, 1.32, 1.314, 1.331]},
  "meshless": {"seeds": [7, 8, 9, 10, 11, 12, 13, 14], "x": [1.725, 1.674, 1.663, 1.671, 1.511, 1.577, 1.587, 1.729], "y": [2.957, 2.935, 2.934, 2.984, 2.921, 2.953, 2.946, 2.977]},
  "whole": {"seeds": [7, 8, 9, 10, 11, 12, 13, 14, 15], "x": [5.532, 6.096, 5.581, 6.44, 6.366, 5.714, 5.749, 5.734, 6.553], "y": [3.655, 4.185, 4.742, 4.12, 4.182, 3.917, 3.712, 3.919, 4.611]},
  "whole_stock": {"seeds": [7, 8, 9, 10, 11, 12, 13, 14, 15], "x": [6.364, 6.762, 7.095, 6.468, 6.485, 6.597, 6.208, 6.925, 6.632], "y": [4.178, 4.814, 5.647, 4.485, 4.057, 4.638, 3.879, 4.833, 5.554]},
  # Q72 frontier-gap fill (2026-09-03), 3 seeds each
  "aphase_nd": {"seeds": [7, 8, 9], "x": [1.616, 1.16, 1.193], "y": [3.492, 3.057, 2.956]},
  "aphase_r1": {"seeds": [7, 8, 9], "x": [1.306, 1.344, 1.267], "y": [1.057, 1.041, 1.082]},
  "aphase_r3": {"seeds": [7, 8, 9], "x": [1.137, 1.082, 1.107], "y": [1.794, 1.756, 1.608]},
  "aphase_r4": {"seeds": [7, 8, 9], "x": [1.32, 1.121, 1.104], "y": [2.461, 2.432, 2.22]},
  "aphase_sp": {"seeds": [7, 8, 9], "x": [1.228, 1.195, 1.07], "y": [1.285, 1.273, 1.226]},
  "apull_nd": {"seeds": [7, 8, 9], "x": [1.412, 1.563, 1.185], "y": [3.849, 4.205, 3.487]},
  "bphase1": {"seeds": [7, 8, 9], "x": [2.452, 3.409, 3.881], "y": [1.43, 1.6, 1.5]},
  "bphase3": {"seeds": [7, 8, 9], "x": [2.35, 2.328, 2.643], "y": [1.4, 1.39, 1.33]},
  "bphase4": {"seeds": [7, 8, 9], "x": [2.229, 2.912, 2.105], "y": [1.72, 1.68, 2.01]},
  # Q74: B phase r=2 with the per-peer fresh-claim cap (SEGMENT_CLAIM_PER_PEER=4).
  "bphase_c4": {"seeds": [7, 8, 9], "x": [1.919, 2.152, 2.014], "y": [1.23, 1.23, 1.22]},
  "codedphase_nd": {"seeds": [7, 8, 9], "x": [1.243, 1.271, 1.329], "y": [4.044, 4.429, 4.761]},
  "codedphase_nosp": {"seeds": [7, 8, 9], "x": [0.991, 0.924, 1.003], "y": [2.414, 2.379, 2.531]},
  "cplain": {"seeds": [7, 8, 9], "x": [1.398, 1.367, 1.324], "y": [7.536, 7.542, 7.995]},
  "dphase": {"seeds": [7, 8, 9], "x": [1.558, 1.43, 1.37], "y": [2.173, 2.098, 2.073]},
  "fullpush_d4": {"seeds": [7, 8, 9], "x": [1.309, 1.267, 1.172], "y": [2.86, 2.912, 2.85]},
  "fullpush_d6": {"seeds": [7, 8, 9], "x": [1.239, 1.214, 0.938], "y": [4.286, 4.312, 3.94]},
  "k64only": {"seeds": [7, 8, 9], "x": [1.106, 1.081, 0.938], "y": [1.32, 1.31, 1.238]},
  # Q73 offer-table arms (3 seeds) — the corrected discipline
  "aphase_ot": {"seeds": [7, 8, 9], "x": [1.209, 1.115, 1.231], "y": [1.294, 1.273, 1.224]},
  "codedphase_ot": {"seeds": [7, 8, 9], "x": [1.049, 0.921, 1.038], "y": [2.131, 2.072, 1.871]},
  "moveban_ot": {"seeds": [7, 8, 9], "x": [1.15, 1.09, 1.075], "y": [1.468, 1.448, 1.309]},
  # measured, not plotted (decomposition detail — see experiments.md Q72):
  # "selrr": x med 1.157 y med 1.28; "sidsonly": x 1.185 y 1.28; "d16only": x 1.429 y 1.15;
  # "m2only": x 1.373 y 1.50
}
# aphase: x med 1.124 [1.038,1.238] y med 1.27 [1.21,1.30] n=5
# aphasedelay: x med 1.194 [1.186,1.206] y med 1.26 [1.21,1.28] n=3

STYLE = {  # label, color, marker, (dx, dy, ha)
    "whole_stock": ("whole message, stock (today)", "#777777", "*", (-0.06, 0.0, "right")),
    "whole": ("whole + discipline", "#333333", "*", (-0.06, -0.15, "right")),
    "aplain": ("segmented (A), no discipline", "#1f77b4", "o", (0.08, 0.0, "left")),
    "apull": ("A pull-only", "#1f77b4", "P", (0.08, -0.14, "left")),
    "aphase": ("A + phase r=2", "#1f77b4", "P", (-0.14, 0.10, "right")),
    "aphasedelay": ("A + phase, source-delay", "#5a9bd4", "P", (0.10, 0.30, "left")),
    "codedpull": ("A coded 32+32, pull-only", "#d62728", "v", (0.04, -0.32, "left")),
    "codedphase": ("A coded 32+32 + phase + stop-pull", "#d62728", "^", (-0.08, -0.24, "right")),
    "k64d16m2": ("K=64, D=16, link m=2", "#1f77b4", "s", (-0.28, -0.42, "right")),
    "meshless": ("A meshless", "#1f77b4", "X", (0.08, 0.0, "left")),
    "cphase": ("C + phase, 64 topics", "#2ca02c", "D", (-0.06, 0.26, "right")),
    "cphase32": ("C + phase, 32 topics", "#2ca02c", "d", (0.22, -0.34, "left")),
    "bsplit": ("B split r=2", "#9467bd", "o", (0.10, 0.24, "left")),
    "bsplit1": ("B split r=1", "#9467bd", "o", (0.08, -0.14, "left")),
    "bsplit3": ("r=3", "#9467bd", "o", (0.08, 0.0, "left")),
    "bsplit4": ("r=4", "#9467bd", "o", (0.08, 0.0, "left")),
    "bphase": ("B phase r=2", "#9467bd", "D", (0.10, -0.24, "left")),
    "dcustR0": ("D custody R=0", "#ff7f0e", "P", (0.08, 0.0, "left")),
    "dcustR8": ("R=8", "#ff7f0e", "P", (0.02, 0.28, "left")),
    "dcustR16": ("R=16", "#ff7f0e", "P", (-0.10, 0.10, "right")),
    # Q72/Q73 additions
    "aphase_ot": ("A+phase + offer table", "#008b8b", "P", (0.10, -0.10, "left")),
    "codedphase_ot": ("coded + offer table", "#008b8b", "^", (0.10, 0.10, "left")),
    "moveban_ot": ("move-on/ban + offer table", "#008b8b", "s", (0.10, 0.0, "left")),

    "aphase_r1": ("r=1", "#1f77b4", "P", (-0.06, -0.14, "right")),
    "aphase_r3": ("r=3", "#1f77b4", "P", (-0.08, 0.0, "right")),
    "aphase_r4": ("r=4", "#1f77b4", "P", (0.08, 0.0, "left")),
    "aphase_sp": ("A+phase+stop-pull", "#5a9bd4", "s", (0.55, 0.02, "left")),
    "aphase_nd": ("A+phase, no discipline", "#b8860b", "P", (0.10, 0.10, "left")),
    "apull_nd": ("pull-only, no discipline", "#b8860b", "v", (0.10, 0.0, "left")),
    "codedphase_nd": ("coded, no discipline", "#b8860b", "^", (0.08, 0.14, "left")),
    "codedphase_nosp": ("coded+phase, no stop-pull", "#d62728", "s", (-0.06, 0.24, "right")),
    "dphase": ("D+phase R=8", "#ff7f0e", "D", (0.16, 0.10, "left")),
    "fullpush_d4": ("full-push D=4", "#17becf", "o", (0.10, -0.10, "left")),
    "fullpush_d6": ("D=6", "#17becf", "o", (-0.06, 0.12, "right")),
    "cplain": ("C plain", "#2ca02c", "D", (0.08, 0.0, "left")),
    "bphase1": ("B phase r=1", "#9467bd", "D", (0.08, 0.10, "left")),
    "bphase3": ("r=3", "#9467bd", "D", (-0.06, -0.14, "right")),
    "bphase4": ("r=4", "#9467bd", "D", (-0.06, -0.18, "right")),
    "k64only": ("K=64 alone", "#1f77b4", "s", (-0.06, 0.34, "right")),
    "bphase_c4": ("B phase r=2 + claim cap", "#008b8b", "D", (0.10, 0.12, "left")),
}


def med(arm):
    d = DATA[arm]
    return st.median(d["x"]), st.median(d["y"])



def render(outfile, xmax, ymax, xticks, yticks, mech_pos=None, mech_fontsize=7.5, dpi=130):
    fig, ax = plt.subplots(figsize=(10.4, 7.0))
    shown = [a for a in STYLE if med(a)[0] <= xmax and med(a)[1] <= ymax]
    texts, pts_x, pts_y = [], [], []
    for arm in shown:
        label, color, marker, (dx, dy, ha) = STYLE[arm]
        d = DATA[arm]
        x, y = med(arm)
        xerr = [[x - min(d["x"])], [max(d["x"]) - x]]
        yerr = [[y - min(d["y"])], [max(d["y"]) - y]]
        # Bars are the seed RANGE (min-max), dimmed well below the median markers so the
        # frontier reads first and the spread second.
        ax.errorbar([x], [y], xerr=xerr, yerr=yerr, marker=None, color=color,
                    ls="none", elinewidth=0.7, capsize=1.5, alpha=0.22)
        ax.plot([x], [y], marker=marker, color=color, ms=7, ls="none", alpha=0.95)
        t = ax.text(x, y, label, fontsize=8.5, color=color, ha="center", va="center")
        texts.append(t)
        pts_x.append(x)
        pts_y.append(y)

    # Connectors, Figure-1 grammar: solid = parameter sweep within a family, dotted =
    # mechanism progression inside a variant. Drawn between medians of shown arms only.
    def line(arms, color, **kw):
        arms = [a for a in arms if a in shown]
        if len(arms) < 2:
            return
        xs, ys = zip(*[med(a) for a in arms])
        ax.plot(xs, ys, color=color, **kw)

    line(["aplain", "aphase", "apull"], "#1f77b4", lw=1.0, ls=":", alpha=0.5)
    line(["codedpull", "codedphase_nosp", "codedphase"], "#d62728", lw=1.0, ls=":", alpha=0.6)
    line(["cphase32", "cphase"], "#2ca02c", lw=1.2)
    line(["bsplit1", "bsplit", "bsplit3", "bsplit4"], "#9467bd", lw=1.2)
    line(["dcustR0", "dcustR8", "dcustR16"], "#ff7f0e", lw=1.2)
    line(["aphase_r1", "aphase", "aphase_r3", "aphase_r4"], "#1f77b4", lw=1.2)
    line(["bphase1", "bphase", "bphase3", "bphase4"], "#9467bd", lw=1.2, alpha=0.7)
    line(["fullpush_d4", "fullpush_d6", "aplain"], "#17becf", lw=1.2)
    line(["cplain", "cphase"], "#2ca02c", lw=1.0, ls=":", alpha=0.6)
    line(["dcustR8", "dphase"], "#ff7f0e", lw=1.0, ls=":", alpha=0.6)
    line(["k64only", "k64d16m2"], "#1f77b4", lw=1.0, ls=":", alpha=0.5)
    for a, b in (("aphase", "aphase_nd"), ("codedphase", "codedphase_nd"), ("apull", "apull_nd")):
        line([a, b], "#b8860b", lw=1.0, ls=":", alpha=0.6)
    for a, b in (("aphase", "aphase_ot"), ("codedphase", "codedphase_ot"), ("bphase", "bphase_c4")):
        line([a, b], "#008b8b", lw=1.0, ls=":", alpha=0.6)


    ax.set_xlabel("completion: time until every node holds the full payload (s)")
    ax.set_ylabel("payload equivalents received per node")
    ax.set_title("1000 nodes, 1 MiB, degree 70, geo latency, 50/100 Mbps — medians, bars = seed range;\n"
                 "gold dotted = discipline removed; teal = request-path memory (A: offer table, B: claim cap);\n"
                 "B bytes assume per-segment compression (measured equivalent)",
                 fontsize=9)
    ax.set_xscale("log")
    ax.set_yscale("log")
    from matplotlib.ticker import FixedLocator, NullFormatter, FuncFormatter
    fmt = FuncFormatter(lambda v, _: f"{v:g}")
    ax.xaxis.set_major_locator(FixedLocator(xticks))
    ax.xaxis.set_major_formatter(fmt)
    ax.xaxis.set_minor_formatter(NullFormatter())
    ax.yaxis.set_major_locator(FixedLocator(yticks))
    ax.yaxis.set_major_formatter(fmt)
    ax.yaxis.set_minor_formatter(NullFormatter())
    ax.grid(True, alpha=0.25, lw=0.5)
    ax.set_xlim(0.68, xmax)
    ax.set_ylim(0.85, ymax)
    t_better = ax.annotate("better", xy=(0.012, 0.012), xytext=(0.10, 0.030),
                xycoords="axes fraction", textcoords="axes fraction", fontsize=9.5,
                color="#333333", ha="left", va="bottom",
                arrowprops=dict(arrowstyle="->", color="#333333", lw=1.0))
    t_worse = ax.annotate("worse", xy=(0.985, 0.985), xytext=(0.915, 0.915),
                xycoords="axes fraction", textcoords="axes fraction", fontsize=9.5,
                color="#aaaaaa", ha="right", va="top",
                arrowprops=dict(arrowstyle="->", color="#aaaaaa", lw=1.0))
    t_legend = ax.text(0.985, 0.03,
                "transport variants\n"
                "A — segments as gossip messages, one topic\n"
                "B — gossipsub partial messages: parts requested; push per policy (split/phase)\n"
                "C — one gossip topic per segment\n"
                "D — per-segment topics, custody S of N (coded)",
                transform=ax.transAxes, fontsize=mech_fontsize, color="#555555",
                ha="right", va="bottom", linespacing=1.35,
                bbox=dict(boxstyle="round,pad=0.4", fc="white", ec="#cccccc", lw=0.6, alpha=0.85))
    t_mech = None
    if mech_pos:
        mx, my, mha, mva = mech_pos
        t_mech = ax.text(mx, my,
                    "mechanisms\n"
                    "phase r — push each segment to r mesh peers, IHAVE the rest\n"
                    "pull-only — announce everything; receivers IWANT what they lack\n"
                    "discipline — one outstanding IWANT per id (400 ms window)\n"
                    "coded 32+32 — RS parity: any 32 of 64 reconstruct\n"
                    "stop-pull — decline further pulls once a coded group is complete\n"
                    "custody R — D nodes subscribe 32+R of the 64 coded topics\n"
                    "split r (B) — push each part to r peers, request the rest\n"
                    "claim cap (B) — at most 4 fresh asks per peer per pass",
                    transform=ax.transAxes, fontsize=mech_fontsize, color="#555555",
                    ha=mha, va=mva, linespacing=1.35,
                    bbox=dict(boxstyle="round,pad=0.4", fc="white", ec="#cccccc", lw=0.6, alpha=0.85))

    # Label layout: adjustText untangles overlaps, thin gray leaders keep moved labels attached.
    from adjustText import adjust_text
    fig.canvas.draw()
    adjust_text(texts, x=pts_x, y=pts_y, ax=ax, objects=[o for o in (t_better, t_worse, t_legend, t_mech) if o],
                expand_axes=False, ensure_inside_axes=True, time_lim=20,
                arrowprops=dict(arrowstyle="-", color="#999999", lw=0.5, alpha=0.6,
                                shrinkA=2, shrinkB=4))
    fig.tight_layout()
    fig.savefig(outfile, dpi=dpi)
    print("written", outfile)


render("notes/figures/tradeoff_realistic.png", 9.5, 10.5,
       [0.8, 1, 1.5, 2, 3, 4, 5, 6, 8], [1, 1.5, 2, 3, 4, 5, 6, 8],
       mech_pos=(0.985, 0.225, "right", "bottom"), mech_fontsize=7.0)
render("notes/figures/tradeoff_realistic_zoom.png", 3.0, 3.2,
       [0.8, 1, 1.5, 2, 2.5, 3], [1, 1.2, 1.5, 2, 2.5, 3],
       mech_pos=(0.015, 0.075, "left", "bottom"))
