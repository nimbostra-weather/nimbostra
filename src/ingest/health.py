"""Freshness check of the published runs, and alerts.

``python -m ingest.health`` exits 1 when the latest published AROME run is older
than ``ALERT_MAX_AGE_HOURS`` (default 12 h: runs every 3 to 6 h, plus the
publication delay) and, with ``--notify``, sends an alert.
``python -m ingest.health alert "<text>"`` only sends an alert (used by the
systemd ``OnFailure=`` unit).

Alerts are a plain-text POST to ``ALERT_WEBHOOK_URL`` (for example an ntfy
topic, or any endpoint that accepts a text body). Without that variable the
message only goes to the log, hence to the systemd journal.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import socket
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import httpx

from ingest.dotenv import load_dotenv

logger = logging.getLogger("ingest.health")

DEFAULT_MAX_AGE_HOURS = 12.0


@dataclass(frozen=True)
class Freshness:
    model: str
    run: datetime | None
    age_hours: float | None
    max_age_hours: float

    @property
    def ok(self) -> bool:
        return self.age_hours is not None and self.age_hours <= self.max_age_hours

    def message(self) -> str:
        if self.run is None:
            return f"{self.model} : aucun run publié"
        state = "à jour" if self.ok else "en retard"
        return (
            f"{self.model} : dernier run {self.run:%Y-%m-%d %H} UTC, "
            f"âge {self.age_hours:.1f} h ({state}, seuil {self.max_age_hours:g} h)"
        )


def check_freshness(
    model_root: Path,
    model: str = "arome",
    *,
    max_age_hours: float = DEFAULT_MAX_AGE_HOURS,
    now: datetime | None = None,
) -> Freshness:
    """Age of the run named in ``<model_root>/latest.json``."""
    now = now or datetime.now(UTC)
    p = Path(model_root) / "latest.json"
    try:
        run_text = json.loads(p.read_text())["run"]
    except (FileNotFoundError, KeyError, ValueError):
        return Freshness(model, None, None, max_age_hours)
    run = datetime.strptime(run_text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    age = (now - run).total_seconds() / 3600
    return Freshness(model, run, age, max_age_hours)


def send_alert(text: str, url: str | None, http: httpx.Client | None = None) -> bool:
    """POST ``text`` to ``url``; always log it. Returns True if the POST succeeded."""
    logger.warning("ALERT: %s", text)
    if not url:
        return False
    client = http or httpx.Client()
    try:
        resp = client.post(
            url,
            content=text.encode(),
            headers={"Title": "Nimbostra", "Content-Type": "text/plain; charset=utf-8"},
            timeout=15,
        )
        resp.raise_for_status()
        return True
    except httpx.HTTPError as exc:
        logger.error("alert not delivered: %s", exc)
        return False
    finally:
        if http is None:
            client.close()


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    ap = argparse.ArgumentParser(description="Run freshness check and alerts")
    ap.add_argument("command", nargs="?", default="check", choices=["check", "alert"])
    ap.add_argument("text", nargs="?", default="", help="alert text (command alert)")
    ap.add_argument("--notify", action="store_true", help="send an alert when stale")
    ap.add_argument("--model", default="arome")
    ap.add_argument("--data", type=Path, default=Path(os.environ.get("DATA_DIR", "data/zarr")))
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    url = os.environ.get("ALERT_WEBHOOK_URL") or None
    host = socket.gethostname()

    if args.command == "alert":
        send_alert(f"[{host}] {args.text or 'échec sans détail'}", url)
        return 0

    max_age = float(os.environ.get("ALERT_MAX_AGE_HOURS", DEFAULT_MAX_AGE_HOURS))
    f = check_freshness(args.data / args.model, args.model, max_age_hours=max_age)
    print(f.message())
    if f.ok:
        return 0
    if args.notify:
        send_alert(f"[{host}] {f.message()}", url)
    return 1


if __name__ == "__main__":
    sys.exit(main())
