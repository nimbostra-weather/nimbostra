"""Command line entry point: convert the latest staged AROME run into Zarr.

Usage (from ``src``):
  python -m ingest.arome_convert inventory        # list GRIB keys of the latest staged run
  python -m ingest.arome_convert convert          # convert it if not done yet
  python -m ingest.arome_convert convert --keep 2 # then keep the 2 latest stores

Paths: ``$RAW_DIR/arome`` (staged GRIB) and ``$DATA_DIR/arome`` (Zarr stores).
Exit status: 0 done or nothing new, 1 conversion error.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from ingest.arome_zarr import (
    AROME_VARIABLES,
    _match,
    convert_run,
    next_run_to_convert,
    prune_stores,
    read_messages_eccodes,
    write_latest,
)
from ingest.meteofrance_paquets import RunStage, format_run, staged_runs

logger = logging.getLogger("ingest.arome_convert")


def inventory(raw_root: Path) -> int:
    runs = staged_runs(raw_root)
    if not runs:
        print("no complete staged run")
        return 0
    stage = RunStage(raw_root, runs[-1])
    manifest = stage.read_manifest()
    print(f"run {manifest['run']}")
    for package, labels in manifest["packages"].items():
        # The first file of a package lacks accumulations and maxima (lead time 0),
        # so look at the second one when there is one.
        label = labels[1] if len(labels) > 1 else labels[0]
        print(f"{package}/{label}.grib2")
        for meta, _ in read_messages_eccodes(
            stage.path / package / f"{label}.grib2", lambda m: False
        ):
            spec = _match(meta, AROME_VARIABLES)
            target = spec.name if spec else "-"
            print(f"  {meta.key:<40} {meta.units:<12} -> {target:<9} {meta.name}")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("command", choices=["inventory", "convert"])
    ap.add_argument("--raw", type=Path, default=Path(os.environ.get("RAW_DIR", "data/raw")))
    ap.add_argument("--out", type=Path, default=Path(os.environ.get("DATA_DIR", "data/zarr")))
    ap.add_argument("--keep", type=int, default=None, help="Zarr stores to keep after convert")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    raw_root = args.raw / "arome"
    out_root = args.out / "arome"
    if args.command == "inventory":
        return inventory(raw_root)

    run = next_run_to_convert(raw_root, out_root)
    if run is None:
        logger.info("nothing new to convert")
        return 0
    try:
        report = convert_run(raw_root, run, out_root)
    except (ValueError, OSError) as exc:
        logger.error("%s: %s", format_run(run), exc)
        return 1
    write_latest(out_root, run)
    logger.info(
        "%s: %d fields, %d variables, %d lead times -> %s",
        format_run(run),
        report.fields,
        len(report.variables),
        len(report.steps),
        report.path,
    )
    for name, hours in report.missing.items():
        logger.warning("%s: no data at +%s h", name, ",".join(map(str, hours)))
    if report.unknown:
        logger.info("ignored GRIB keys: %s", ", ".join(sorted(report.unknown)))
    if args.keep:
        for old in prune_stores(out_root, args.keep):
            logger.info("pruned %s", format_run(old))
    return 0


if __name__ == "__main__":
    sys.exit(main())
