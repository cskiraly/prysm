#!/usr/bin/env python3
"""The one figure for the short writeup: completion time against payload size, both arms,
against the 3 s reveal-relative budget. Carries the whole argument in one picture: the
whole-message cliff between 512 and 768 KiB, the segmented arm flat through 2 MiB, and
today's mainnet (~200 KB, our extrapolation) sitting left of the plotted problem — so the
claim reads as prospective without an adjective. Source: the 2026-09-04 re-sweep (fig1_out on the fleet box): whole message STOCK vs the recommended stack
(n=500, degree 70, home builder, 0% datacenter relays, geo latency, 6 seeds per point,
p50 = mean of per-seed p50s, no competing traffic)."""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FixedLocator, NullFormatter, FuncFormatter

fig, ax = plt.subplots(figsize=(7.6, 4.6), dpi=160)

kib = [256, 384, 512, 768, 1024, 2048]
whole = [0.84, 1.23, 1.72, 3.09, 4.85, 15.0]
seg = [0.46, 0.52, 0.56, 0.64, 0.74, 1.11]
# Per-seed timely counts (of 499 receivers) for the whole-message arm; segmented is
# 499/499 in every seed at every size.
whole_all_timely = [True, True, True, False, False, False]
timely_note = {768: ("88–429 of 499 timely", (10, -14), "left"),
               1024: ("10–54 timely", (-8, 4), "right"),
               2048: ("0–1 of 499", (-4, -14), "right")}

grey, red = "#555555", "#c44e52"

ax.axhline(3.0, color="#333333", lw=1.0, ls="--", alpha=0.7)
ax.annotate("3 s reveal-relative budget", (2048, 3.0), xytext=(0, 5),
            textcoords="offset points", fontsize=9, color="#333333", ha="right")

ax.plot(kib, whole, "-", color=grey, lw=1.4, zorder=2)
ax.plot(kib, seg, "-", color=red, lw=1.4, zorder=2)
for x, y, full in zip(kib, whole, whole_all_timely):
    ax.plot([x], [y], "o", color=grey, ms=7,
            mfc=grey if full else "white", mew=1.4, zorder=3)
ax.plot(kib, seg, "o", color=red, ms=7, zorder=3)

for x, (note, off, ha) in timely_note.items():
    y = whole[kib.index(x)]
    ax.annotate(note, (x, y), xytext=off, textcoords="offset points",
                fontsize=8, color=grey, ha=ha)

ax.annotate("whole message", (kib[1], whole[1]), xytext=(-8, 8),
            textcoords="offset points", fontsize=10, color=grey, ha="right")
ax.annotate("segmented, recommended stack", (kib[3], seg[3]), xytext=(8, -14),
            textcoords="offset points", fontsize=10, color=red, ha="left")

# Today's mainnet, ~200 KB typical block at the 60M gas limit — our extrapolation
# from a 560-block sample at 30M, not a measured figure (Q50, "Against mainnet").
ax.axvline(200, color="#4c72b0", lw=1.0, ls=":", alpha=0.8)
ax.annotate("mainnet today\n(~200 KB, est.)", (200, 9.5), xytext=(5, 0),
            textcoords="offset points", fontsize=9, color="#4c72b0", ha="left")

ax.set_xlabel("execution payload size (uncompressed)")
ax.set_ylabel("completion p50: every node holds the payload (s)")
ax.set_xscale("log")
ax.set_yscale("log")
ax.set_xlim(170, 2400)
ax.set_ylim(0.3, 20)
fmt_x = FuncFormatter(lambda v, _: "1 MiB" if v == 1024 else ("2 MiB" if v == 2048 else f"{v:g} KiB"))
ax.xaxis.set_major_locator(FixedLocator([256, 384, 512, 768, 1024, 2048]))
ax.xaxis.set_major_formatter(fmt_x)
ax.xaxis.set_minor_formatter(NullFormatter())
ax.yaxis.set_major_locator(FixedLocator([0.3, 0.5, 1, 2, 3, 5, 10, 15]))
ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
ax.yaxis.set_minor_formatter(NullFormatter())
ax.tick_params(axis="x", labelsize=8.5)
ax.grid(True, alpha=0.25, lw=0.5)
ax.set_title("n=500, degree 70, home builder, no datacenter relays, geo latency; mean of per-seed p50s",
             fontsize=8.5)
ax.text(0.98, 0.03, "open markers: not all receivers timely (min–max timely across seeds)",
        transform=ax.transAxes, fontsize=8, color=grey, ha="right", va="bottom")
fig.tight_layout()
fig.savefig("notes/figures/payload_sweep.png")
fig.savefig("notes/figures/payload_sweep.svg")
print("written")
