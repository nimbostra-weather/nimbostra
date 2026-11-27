"""Command line entry point: stage the latest published ECMWF open data run (IFS).

Usage (from ``src``):
  python -m ingest.ecmwf_fetch plan             # read-only: latest published run and its cost
  python -m ingest.ecmwf_fetch fetch            # download it (no-op if already staged)
  python -m ingest.ecmwf_fetch fetch --keep 2   # then keep only the 2 latest staged runs

Then convert with ``python -m ingest.mf_paquets_convert convert --model ifs``:
the staging layout is the API Paquets one.

No credentials. ``ECMWF_SOURCES`` (or ``--sources``) orders the mirrors,
default ``aws,ecmwf``. Exit status: 0 complete or nothing new, 1 incomplete
or failed download, 2 configuration.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import replace
from pathlib import Path

import httpx

from ingest.dotenv import load_dotenv
from ingest.ecmwf_opendata import (
    MODELS,
    SOURCES,
    OpenDataClient,
    OpenDataError,
    env_sources,
    estimate,
)
from ingest.meteofrance_paquets import format_run, prune_runs
from ingest.meteofrance_wcs import RateLimiter

logger = logging.getLogger("ingest.ecmwf_fetch")


def main(argv: list[str] | None = None) -> int:
    load_dotenv()  # before argparse: defaults read RAW_DIR / ECMWF_SOURCES
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("command", choices=["plan", "fetch"])
    ap.add_argument("--model", default="ifs", choices=sorted(MODELS))
    ap.add_argument(
        "--root",
        type=Path,
        default=Path(os.environ.get("RAW_DIR", "data/raw")),
        help="staging root; the model name is appended (default: $RAW_DIR or data/raw)",
    )
    ap.add_argument("--keep", type=int, default=None, help="staged runs to keep after fetch")
    ap.add_argument("--candidates", type=int, default=4, help="recent runs to consider")
    ap.add_argument(
        "--sources",
        default=",".join(env_sources()),
        help=f"mirrors in order, names among {sorted(SOURCES)} or URLs (default: aws,ecmwf)",
    )
    ap.add_argument("--max-lead", type=int, default=None, help="last lead time in hours")
    ap.add_argument("--rate", type=float, default=None, help="requests per second, all workers")
    ap.add_argument("--workers", type=int, default=None, help="parallel range requests")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)

    model = MODELS[args.model]
    if args.max_lead is not None:
        model = replace(model, max_lead_hours=args.max_lead)
    sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    root = args.root / model.name
    try:
        with httpx.Client(follow_redirects=True) as http:
            client = OpenDataClient(
                model,
                http,
                sources=sources,
                limiter=RateLimiter(args.rate) if args.rate else None,
                workers=args.workers,
            )
            if args.command == "plan":
                run = client.latest_published_run(max_candidates=args.candidates)
                if run is None:
                    print("no published run")
                    return 0
                print(f"{model.name} {format_run(run)}: {estimate(model, run)}")
                print(f"sources: {', '.join(client.sources)}")
                return 0
            report = client.fetch_latest(root, max_candidates=args.candidates)
    except ValueError as exc:
        logger.error("%s", exc)
        return 2
    except OpenDataError as exc:
        logger.error("%s", exc)
        return 1

    if report.run is None:
        return 0
    logger.info(
        "%s %s: %d lead times fetched (%d messages, %.2f GB), %d skipped, %d failed, "
        "%d calls, %d throttled",
        model.name,
        format_run(report.run),
        report.fetched,
        report.messages,
        report.bytes_fetched / 1e9,
        report.skipped,
        len(report.failed),
        report.http_calls,
        report.throttled,
    )
    if args.keep and report.ok:
        for run in prune_runs(root, args.keep):
            logger.info("pruned %s", format_run(run))
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
