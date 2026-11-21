"""Export slide-ready resource, response-time, SLO, and pod-placement figures.

Run from the repository root with:
    python -m utils.plot_slides RESULTS_DIR --slo-ms 150

RESULTS_DIR is the archived run containing baseline/ and proposed/.
Figures go to slides/ by default.
"""
import argparse
from pathlib import Path
from tempfile import TemporaryDirectory

import matplotlib.pyplot as plt

from utils import plot_autoscaler
from utils.defaults import SLO_MS


SLIDE_LABELS = {
    "resource-usage": ("Resource usage", ("CPU (15s window)", "RAM (15s window)")),
    "response-time": ("Response time", ("Response time p90", "Response time p50")),
    "slo-violations": ("SLO violations", (None,)),
}


def export_slide(fig, output_dir, name, *, legend_columns=None):
    if name == "pod-placement":
        plot_autoscaler._export_png(fig, output_dir, name)
        return

    title, panel_titles = SLIDE_LABELS[name]
    fig.suptitle(title, fontsize=15)
    for ax, panel_title in zip(fig.axes, panel_titles):
        ax.set_title(panel_title or ax.get_title(), fontsize=12)
        for line in ax.lines:
            if line.get_gid() == "window-series":
                line.set_marker("None")
        # Update the legend to use the line-only series.
        if ax.get_legend() is not None:
            ax.legend(frameon=False, ncols=legend_columns or (3 if name == "response-time" else 2))

    bottom = fig.axes[-1]
    xlabel = bottom.get_xlabel()
    bottom.set_xlabel(f"{xlabel}\n(0,0) is a display origin")
    fig.savefig(output_dir / f"{name}.png", dpi=300)

    plot_autoscaler._zoom_axes(fig)
    fig.suptitle(title, fontsize=15)
    bottom.set_xlabel(f"{xlabel}\nZoomed y-axis")
    fig.savefig(output_dir / f"{name}_zoom.png", dpi=300)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results_dir", type=Path, help="archived run with baseline/ and proposed/ data")
    parser.add_argument("--output-dir", type=Path, default=Path("slides"), help="image directory (default: slides)")
    parser.add_argument("--slo-ms", type=float, default=SLO_MS,
                        help="recorded request SLO in milliseconds (default: %(default)g)")
    args = parser.parse_args()

    for method in plot_autoscaler.METHODS:
        if not (args.results_dir / method / "requests.jsonl").is_file():
            parser.error(f"missing {method}/requests.jsonl in {args.results_dir}")

    # The review plotter still writes its diagnostics to the temporary directory.
    # Stage slide images there too, so a failed render leaves prior slides intact.
    with TemporaryDirectory() as workspace:
        workspace = Path(workspace)
        slide_dir = workspace / "slides"
        slide_dir.mkdir()

        def save_slide(fig, _output_dir, name):
            export_slide(fig, slide_dir, name)

        plot_autoscaler.main(
            [str(args.results_dir), str(workspace / "plots"), "--slo-ms", str(args.slo_ms)],
            export_png=save_slide,
            slo_coverage=False,
        )
        args.output_dir.mkdir(parents=True, exist_ok=True)
        for image in sorted(slide_dir.glob("*.png")):
            (args.output_dir / image.name).write_bytes(image.read_bytes())
        print(f"Slide figures written to {args.output_dir}")


if __name__ == "__main__":
    main()
