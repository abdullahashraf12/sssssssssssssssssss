"""Entry point for the Oracle → Zoho Creator sync service.

Starts up to three worker groups:
  - Worker R × N   (realtime event processor – default 6 threads)
  - Worker B × 1   (backfill / historical data loader)
  - Worker D × 1   (always-on DEAD-event retry)

On startup, any events stuck in INFLIGHT (from a previous crash) are
automatically reset to NEW so they will be reprocessed.
"""
from __future__ import annotations

import argparse
import logging
import signal
import sys
import threading

from . import config as config_mod
from .backfill_worker import BackfillWorker
from .dead_worker import DeadRetryWorker
from .oracle_pool import OraclePool
from .rate_limiter import ZohoTrafficGate
from .realtime_worker import RealtimeWorker
from .token_manager import TokenManager
from .zoho_client import ZohoClient


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Oracle → Zoho Creator sync service")
    p.add_argument("--mode", default="both", choices=("realtime", "backfill", "both"))
    p.add_argument("--realtime-interval", type=float, default=None)
    p.add_argument("--backfill-interval", type=float, default=None)
    p.add_argument("--zoho-rate-per-minute", type=int, default=50)
    p.add_argument("--zoho-max-concurrency", type=int, default=6)
    p.add_argument("--realtime-workers", type=int, default=6)
    p.add_argument("--realtime-idle-sleep", type=float, default=0.05)
    p.add_argument("--dead-poll-interval", type=float, default=30.0,
                    help="Seconds between DEAD-event poll cycles (default: 30)")
    p.add_argument("--log-level", default="INFO")

    args = p.parse_args(argv)
    if getattr(args, "interval", None) is not None:
        p.error("--interval removed; use --realtime-interval and --backfill-interval")
    if args.realtime_interval is not None and args.realtime_interval < 0:
        p.error("--realtime-interval must be >= 0")
    if args.backfill_interval is not None and args.backfill_interval < 0:
        p.error("--backfill-interval must be >= 0")
    if args.zoho_rate_per_minute < 0:
        p.error("--zoho-rate-per-minute must be >= 0")
    if args.zoho_max_concurrency < 0:
        p.error("--zoho-max-concurrency must be >= 0")
    if args.realtime_workers < 1:
        p.error("--realtime-workers must be >= 1")
    if args.realtime_idle_sleep < 0:
        p.error("--realtime-idle-sleep must be >= 0")
    if args.realtime_interval is None:
        args.realtime_interval = 0.0
    if args.backfill_interval is None:
        args.backfill_interval = 0.0
    return args


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def _recover_inflight(pool, log) -> None:
    """Reset INFLIGHT events left over from a previous crash back to NEW."""
    try:
        with pool.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE SYNC_EVENTS "
                "SET status = 'NEW', picked_at = NULL, next_attempt_at = NULL "
                "WHERE status = 'INFLIGHT'"
            )
            count = cursor.rowcount
            conn.commit()
            if count:
                log.info("recovered %d orphaned INFLIGHT event(s) → NEW", count)
    except Exception:
        log.exception("INFLIGHT recovery failed (non-fatal)")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging(args.log_level)
    log = logging.getLogger("sync.main")
    cfg = config_mod.load()

    stop = threading.Event()

    pool = OraclePool(cfg.oracle_user, cfg.oracle_pass, cfg.oracle_dsn,
                      stop_event=stop)

    # FIX 2: recover orphaned INFLIGHT events from a previous crash
    _recover_inflight(pool, log)

    tokens = TokenManager(env_path=cfg.env_path, stop_event=stop)
    limiter = ZohoTrafficGate(
        realtime_interval_seconds=args.realtime_interval,
        backfill_interval_seconds=args.backfill_interval,
        max_concurrency=args.zoho_max_concurrency,
        rate_per_minute=args.zoho_rate_per_minute,
    )
    zoho = ZohoClient(cfg.zoho_account_owner, cfg.zoho_app, cfg.zoho_api_base,
                      tokens, limiter, max_attempts=cfg.max_attempts)

    def shutdown(*_: object) -> None:
        log.info("shutdown requested")
        stop.set()
    signal.signal(signal.SIGINT, shutdown)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, shutdown)

    threads: list[threading.Thread] = []

    # --- Worker R (realtime)
    if args.mode in ("both", "realtime"):
        for idx in range(args.realtime_workers):
            rt = RealtimeWorker(
                pool, zoho, cfg.zoho_form_items, cfg.zoho_form_branches,
                max_attempts=cfg.max_attempts,
                batch_size=cfg.realtime_batch,
                idle_sleep=args.realtime_idle_sleep,
                stop_event=stop,
            )
            threads.append(threading.Thread(target=rt.run, name=f"realtime-{idx + 1}",
                                            daemon=False))

    # --- Worker B (backfill)
    if args.mode in ("both", "backfill"):
        bf = BackfillWorker(pool, zoho, cfg.zoho_form_items, cfg.zoho_form_branches,
                            stop_event=stop)
        threads.append(threading.Thread(target=bf.run, name="backfill",
                                        daemon=False))

    # --- Worker D (always-on DEAD retry)
    dead = DeadRetryWorker(pool, poll_interval=args.dead_poll_interval,
                           stop_event=stop)
    threads.append(threading.Thread(target=dead.run, name="dead-retry",
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
