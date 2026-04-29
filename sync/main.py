"""Entry point. Starts Worker R and Worker B as independent threads."""
from __future__ import annotations

import argparse
import logging
import signal
import sys
import threading

from . import config as config_mod
from .backfill_worker import BackfillWorker
from .oracle_pool import OraclePool
from .rate_limiter import IntervalLimiter
from .realtime_worker import RealtimeWorker
from .token_manager import TokenManager
from .zoho_client import ZohoClient


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="sync",
        description="Oracle -> Zoho Creator sync (realtime + backfill).",
    )
    p.add_argument(
        "--interval", type=float, required=True,
        help="Seconds between outbound Zoho calls (global throttle, e.g. 1.0).",
    )
    p.add_argument(
        "--mode", choices=("both", "realtime", "backfill"), default="both",
        help="Which workers to run.",
    )
    p.add_argument("--log-level", default="INFO")
    args = p.parse_args(argv)
    if args.interval < 0.1:
        p.error("--interval must be >= 0.1")
    return args


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging(args.log_level)
    log = logging.getLogger("sync.main")
    cfg = config_mod.load()

    pool = OraclePool(cfg.oracle_user, cfg.oracle_pass, cfg.oracle_dsn)
    tokens = TokenManager(cfg.zoho_client_id, cfg.zoho_client_secret,
                          cfg.zoho_refresh_token, cfg.zoho_token_url)
    limiter = IntervalLimiter(args.interval)
    zoho = ZohoClient(cfg.zoho_account_owner, cfg.zoho_app, cfg.zoho_api_base,
                      tokens, limiter, max_attempts=cfg.max_attempts)

    stop = threading.Event()

    def shutdown(*_: object) -> None:
        log.info("shutdown requested")
        stop.set()
    signal.signal(signal.SIGINT, shutdown)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, shutdown)

    threads: list[threading.Thread] = []
    if args.mode in ("both", "realtime"):
        rt = RealtimeWorker(pool, zoho, cfg.zoho_form_items, cfg.zoho_form_branches,
                            max_attempts=cfg.max_attempts,
                            batch_size=cfg.realtime_batch, stop_event=stop)
        threads.append(threading.Thread(target=rt.run, name="realtime",
                                        daemon=False))
    if args.mode in ("both", "backfill"):
        bf = BackfillWorker(pool, zoho, cfg.zoho_form_items, cfg.zoho_form_branches,
                            stop_event=stop)
        threads.append(threading.Thread(target=bf.run, name="backfill",
                                        daemon=False))

    for t in threads:
        t.start()
    try:
        for t in threads:
            while t.is_alive():
                t.join(timeout=0.5)
                if stop.is_set():
                    break
    finally:
        stop.set()
        for t in threads:
            t.join()
        pool.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
