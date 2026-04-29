import pytest

from sync.main import parse_args


def test_interval_is_required():
    with pytest.raises(SystemExit):
        parse_args([])


def test_interval_below_minimum_rejected():
    with pytest.raises(SystemExit):
        parse_args(["--interval", "0.05"])


def test_default_mode_is_both():
    ns = parse_args(["--interval", "1.0"])
    assert ns.interval == 1.0
    assert ns.mode == "both"


def test_mode_realtime_only():
    ns = parse_args(["--interval", "0.5", "--mode", "realtime"])
    assert ns.mode == "realtime"
