"""Command line entry point: stage the latest fully published API Paquets run.

Usage (from the repository root, with ``src`` on PYTHONPATH):
  python -m ingest.mf_paquets_fetch plan            # read-only: show the latest complete run
  python -m ingest.mf_paquets_fetch fetch           # download it (no-op if already staged)
  python -m ingest.mf_paquets_fetch fetch --keep 2  # then keep only the 2 latest staged runs
  python -m ingest.mf_paquets_fetch plan --model arpege   # same for ARPEGE 0.1°

Credentials come from the environment or from a ``.env`` file in the current
directory or a parent (see ``.env.example``), never from the repository:
``METEOFRANCE_APPLICATION_ID`` (OAuth token, renewed automatically) or else
``METEOFRANCE_API_KEY`` (sent in the ``apikey`` header, as in the spike).
Exit status: 0 complete or nothing new, 1 incomplete or failed download, 2 configuration.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

import httpx

from ingest.dotenv import load_dotenv
from ingest.meteofrance_paquets import (
    MODELS,
    PaquetsAuthError,
    PaquetsClient,
    PaquetsError,
    format_run,
    prune_runs,
)
from ingest.meteofrance_wcs import ApiKeyAuth, ApplicationTokenAuth, Auth

logger = logging.getLogger("ingest.mf_paquets_fetch")


def make_auth(http: httpx.Client, env: dict[str, str]) -> Auth:
    if env.get("METEOFRANCE_APPLICATION_ID"):
        return ApplicationTokenAuth(env["METEOFRANCE_APPLICATION_ID"], http)
    if env.get("METEOFRANCE_API_KEY"):
        return ApiKeyAuth(env["METEOFRANCE_API_KEY"])
    raise SystemExit("set METEOFRANCE_APPLICATION_ID or METEOFRANCE_API_KEY")


def main(argv: list[str] | None = None) -> int:
    load_dotenv()  # before argparse: defaults read RAW_DIR / DATA_DIR
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("command", choices=["plan", "fetch"])
    ap.add_argument("--model", default="arome", choices=sorted(MODELS))
    ap.add_argument(
        "--root",
        type=Path,
        default=Path(os.environ.get("RAW_DIR", "data/raw")),
        help="staging root; the model name is appended (default: $RAW_DIR or data/raw)",
    )
    ap.add_argument("--keep", type=int, default=None, help="staged runs to keep after fetch")
    ap.add_argument("--candidates", type=int, default=4, help="recent runs to consider")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    model = MODELS[args.model]
    root = args.root / model.name
    with httpx.Client(follow_redirects=True) as http:
        client = PaquetsClient(model, make_auth(http, dict(os.environ)), http)
        try:
            if args.command == "plan":
                found = client.latest_complete_run(args.candidates)
                if found is None:
                    print("no fully published run")
                    return 0
                run, plan = found
                print(f"{model.name} {format_run(run)}: {len(plan)} files")
                for f in plan:
                    print(f"  {f.key}")
                return 0

            report = client.fetch_latest(root, args.candidates)
        except PaquetsAuthError as exc:
            logger.error("authentication failed: %s", exc)
            return 2
        except PaquetsError as exc:
            logger.error("%s", exc)
            return 1

    if report.run is None:
        return 0
    logger.info(
        "%s %s: %d fetched (%.2f GB, %d resumed), %d skipped, %d failed, %d calls, %d throttled",
        model.name,
        format_run(report.run),
        report.fetched,
        report.bytes_fetched / 1e9,
        report.resumed,
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
