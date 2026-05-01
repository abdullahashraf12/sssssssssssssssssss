import pytest

from sync.main import parse_args


def test_default_args_no_flags():
    """No flags at all should work — intervals default to 0.0."""
    ns = parse_args([])
    assert ns.mode == "both"
    assert ns.realtime_interval == 0.0
    assert ns.backfill_interval == 0.0
    assert ns.realtime_workers == 6
    assert ns.realtime_idle_sleep == 0.05
    assert ns.dead_poll_interval == 30.0


def test_old_interval_argument_is_rejected():
    with pytest.raises(SystemExit):
        parse_args(["--interval", "0"])


def test_realtime_interval_zero_is_accepted_for_realtime_mode():
    ns = parse_args(["--mode", "realtime", "--realtime-interval", "0"])
    assert ns.realtime_interval == 0
    assert ns.backfill_interval == 0


def test_backfill_interval_zero_is_accepted_for_backfill_mode():
    ns = parse_args(["--mode", "backfill", "--backfill-interval", "0"])
    assert ns.realtime_interval == 0
    assert ns.backfill_interval == 0


def test_negative_intervals_rejected():
    with pytest.raises(SystemExit):
        parse_args(["--mode", "realtime", "--realtime-interval", "-0.01"])
    with pytest.raises(SystemExit):
        parse_args(["--mode", "backfill", "--backfill-interval", "-0.01"])


def test_default_mode_is_both():
    ns = parse_args(["--realtime-interval", "0", "--backfill-interval", "2"])
    assert ns.realtime_interval == 0
    assert ns.backfill_interval == 2
    assert ns.mode == "both"
    assert ns.zoho_rate_per_minute == 50
    assert ns.zoho_max_concurrency == 6
    assert ns.realtime_workers == 6
    assert ns.realtime_idle_sleep == 0.05


def test_mode_realtime_only():
    ns = parse_args(["--mode", "realtime", "--realtime-interval", "0.5"])
    assert ns.mode == "realtime"


def test_realtime_controls_are_configurable():
    ns = parse_args([
        "--mode", "both",
        "--realtime-interval", "0",
        "--backfill-interval", "2",
        "--zoho-rate-per-minute", "0",
        "--zoho-max-concurrency", "0",
        "--realtime-workers", "3",
        "--realtime-idle-sleep", "0",
        "--dead-poll-interval", "60",
    ])
    assert ns.realtime_interval == 0
    assert ns.backfill_interval == 2
    assert ns.zoho_rate_per_minute == 0
    assert ns.zoho_max_concurrency == 0
    assert ns.realtime_workers == 3
    assert ns.realtime_idle_sleep == 0
    assert ns.dead_poll_interval == 60.0
