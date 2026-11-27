"""Command line entry point: convert the latest staged API Paquets run into Zarr.

Usage (from ``src``):
  python -m ingest.arome_convert inventory        # list GRIB keys of the latest staged run
  python -m ingest.arome_convert convert          # convert it if not done yet
  python -m ingest.arome_convert convert --keep 2 # then keep the 2 latest stores
  python -m ingest.arome_convert convert --model arpege

Paths: ``$RAW_DIR/<model>`` (staged GRIB) and ``$DATA_DIR/<model>`` (Zarr stores).
``--constants`` (or ``<MODEL>_CONSTANTS``, e.g. ``ARPEGE_CONSTANTS``) names an
optional GRIB file read for the model orography only.
Exit status: 0 done or nothing new, 1 conversion error.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from ingest.arome_zarr import (
    PROFILES,
    MessageMeta,
    VarSpec,
    _match,
    convert_run,
    next_run_to_convert,
    prune_stores,
    read_messages_eccodes,
    write_latest,
)
from ingest.dotenv import load_dotenv
from ingest.meteofrance_paquets import RunStage, format_run, staged_runs

logger = logging.getLogger("ingest.arome_convert")


def inventory(raw_root: Path, variables: tuple[VarSpec, ...]) -> int:
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
        # ARPEGE files hold 12 lead times: one line per GRIB key, with its hours.
        seen: dict[str, tuple[MessageMeta, list[int]]] = {}
        for meta, _ in read_messages_eccodes(
            stage.path / package / f"{label}.grib2", lambda m: False
        ):
            seen.setdefault(meta.key, (meta, []))[1].append(meta.end_step)
        for key, (meta, hours) in seen.items():
            spec = _match(meta, variables)
            target = spec.name if spec else "-"
            print(
                f"  {key:<40} {meta.units:<12} -> {target:<12} "
                f"+{','.join(map(str, sorted(set(hours))))}h {meta.name}"
            )
    return 0


def main(argv: list[str] | None = None) -> int:
    load_dotenv()  # before argparse: defaults read RAW_DIR / DATA_DIR
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("command", choices=["inventory", "convert"])
    ap.add_argument("--model", default="arome", choices=sorted(PROFILES))
    ap.add_argument("--raw", type=Path, default=Path(os.environ.get("RAW_DIR", "data/raw")))
    ap.add_argument("--out", type=Path, default=Path(os.environ.get("DATA_DIR", "data/zarr")))
    ap.add_argument("--keep", type=int, default=None, help="Zarr stores to keep after convert")
    ap.add_argument(
        "--constants",
        type=Path,
        default=None,
        help="GRIB file with the model orography (default: $<MODEL>_CONSTANTS if set)",
    )
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    profile = PROFILES[args.model]
    raw_root = args.raw / args.model
    out_root = args.out / args.model
    if args.command == "inventory":
        return inventory(raw_root, profile.variables)
    constants = args.constants
    if constants is None and os.environ.get(f"{args.model.upper()}_CONSTANTS"):
        constants = Path(os.environ[f"{args.model.upper()}_CONSTANTS"])
    if constants is not None and not constants.is_file():
        logger.error("constants file %s not found", constants)
        return 1

    run = next_run_to_convert(raw_root, out_root)
    if run is None:
        logger.info("nothing new to convert")
        return 0
    try:
        report = convert_run(
            raw_root,
            run,
            out_root,
            variables=profile.variables,
            static_packages=profile.static_packages,
            required=profile.required,
            constants=constants,
        )
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
